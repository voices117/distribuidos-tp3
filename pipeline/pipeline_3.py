"""
This file contains the tasks that solve the third item required:
Top 10 of tags by total score for each year.
"""

import json
from typing import Dict

from middleware import END_OF_STREAM, USE_HASH, as_worker, consume_from, send_to_client, send_data
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def join_callback(channel:BlockingChannel, worker_id:str):
    """This is a sharded stage that for each answer that it receives, finds the
    matching question (by ParentID) and yields a new record also containing the
    tags from the question. The rows are sharded by the CSV parser using the
    'ParentID' for the answers an the 'Id' for the questions, so we make sure
    that every answer is matched to the correspnding question."""

    batch_id:Dict[str, int] = defaultdict(int)
    questions, answers = defaultdict(dict), defaultdict(dict)
    for correlation_id, body in consume_from(channel, 'join', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            questions.pop(correlation_id, None)
            answers.pop(correlation_id, None)
            batch_id.pop(correlation_id, None)
            continue
            
        batch = []
        
        data = json.loads(body)
        if 'questions' in data:
            # we yield each new question received to the next stage
            for question in data['questions']:
                questions[correlation_id][question['Id']] = question
                batch.append(question)
        elif 'answers' in data:
            # register the new answer in case we didn't receive the corresponding
            # question yet
            for answer in data['answers']:
                answers[correlation_id][answer['ParentId']] = answer
        else:
            assert False, f'unexpected message "{data}"'[:120]  # truncate the message in case is too large

        # find the answers of which we already have the matching question
        # and make the join
        for parent_id, answer in answers[correlation_id].items():
            if question := questions[correlation_id].get(parent_id):
                answer['Tags'] = question['Tags']
                batch.append(answer)

        # remove the joined answers from the cache because we don't need them
        # anymore
        for joined in batch:
            if parent_id := joined.get('ParentId'):
                del answers[correlation_id][parent_id]

        if batch:
            # send the new processed batch
            send_data(
                json.dumps(batch),
                channel=channel,
                worker='score_by_tag_and_year',
                correlation_id=correlation_id,
                shard_key=batch_id[correlation_id]
            )
            batch_id[correlation_id] += 1


@as_worker
def score_by_tag_and_year_callback(channel:BlockingChannel, worker_id:str):
    """Collect the resulting joined answers and questions from the previous
    stage and aggregate the score by year and tag. Then proceed to send the
    data by batches to the next step."""

    from collections import Counter

    batch_id:Dict[str, int] = defaultdict(int)
    received, tags_per_year = defaultdict(int), defaultdict(lambda: defaultdict(Counter))
    for correlation_id, body in consume_from(channel, 'score_by_tag_and_year', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            # flush remaining data
            data = {
                # include batch ID so the next stage can detect duplicates
                'id': f'{worker_id}_{batch_id[correlation_id]}',
                'data': tags_per_year[correlation_id],
            }
            send_data(json.dumps(data), channel=channel, worker='top_10_tags', correlation_id=correlation_id, shard_key=USE_HASH)

            tags_per_year.pop(correlation_id, None)
            received.pop(correlation_id, None)
            batch_id.pop(correlation_id, None)
            continue

        rows = json.loads(body)
        for row in rows:
            year = row['CreationDate'][:4]

            tags = {tag: int(row['Score']) for tag in row['Tags'].split(' ')}
            tags_per_year[correlation_id][year].update(tags)
            
        received[correlation_id] += len(rows)
        if received[correlation_id] > 500:
            # once we accumulated a sufficiently large batch, we pass it to the
            # next stage
            data = {
                # include batch ID so the next stage can detect duplicates
                'id': f'{worker_id}_{batch_id[correlation_id]}',
                'data': tags_per_year[correlation_id],
            }
            send_data(json.dumps(data), channel=channel, worker='top_10_tags', correlation_id=correlation_id, shard_key=USE_HASH)
            received[correlation_id] = 0
            tags_per_year[correlation_id] = defaultdict(Counter)
            batch_id[correlation_id] += 1


@as_worker
def top_10_tags_callback(channel:BlockingChannel, worker_id:str):
    """Collects the aggregated data batches and calculates the final values.
    Finally, sorts the tags by score for each year to get the resulting
    top 10."""

    from collections import Counter

    tags_per_year = defaultdict(lambda: defaultdict(Counter))
    for correlation_id, body in consume_from(channel, 'top_10_tags', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            # build final result
            response  = '=====================\n'
            response += '= Top tags per year =\n'
            response += '=====================\n'
            for year in sorted(tags_per_year[correlation_id]):
                response += f'{year}\n'
                top_10 = tags_per_year[correlation_id][year].most_common()
                i = 0
                for tag, score in sorted(top_10, key=lambda x: (x[1],x[0]), reverse=True):
                    i+=1
                    response += f'    {tag} ({score})\n'
                    if i == 10:
                        break

            send_to_client(channel=channel, correlation_id=correlation_id, body=response.encode('utf-8'))

            tags_per_year.pop(correlation_id)
            continue

        data = json.loads(body)['data']
        for year, tag_scores in data.items():
            tags_per_year[correlation_id][year].update(tag_scores)
