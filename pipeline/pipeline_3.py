"""
This file contains the tasks that solve the third item required:
Top 10 of tags by total score for each year.
"""

import json

from middleware import as_worker, consume_from, send_data
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def join_callback(channel:BlockingChannel):
    """This is a sharded stage that for each answer that it receives, finds the
    matching question (by ParentID) and yields a new record also containing the
    tags from the question. The rows are sharded by the CSV parser using the
    'ParentID' for the answers an the 'Id' for the questions, so we make sure
    that every answer is matched to the correspnding question."""

    questions, answers = {}, {}
    for _, properties, body in consume_from(channel, 'join'):
        batch = []
        
        data = json.loads(body)
        if properties.correlation_id == 'questions':
            # we yield each new question received to the next stage
            for question in data:
                questions[question['Id']] = question
                batch.append(question)
        elif properties.correlation_id == 'answers':
            # register the new answer in case we didn't receive the corresponding
            # question yet
            for answer in data:
                answers[answer['ParentId']] = answer
        else:
            assert False, f'unexpected message "{properties.correlation_id}"'

        # find the answers of which we already have the matching question
        # and make the join
        for parent_id, answer in answers.items():
            if question := questions.get(parent_id):
                answer['Tags'] = question['Tags']
                batch.append(answer)

        # remove the joined answers from the cache because we don't need them
        # anymore
        for joined in batch:
            if parent_id := joined.get('ParentId'):
                del answers[parent_id]

        # send the new processed batch
        send_data(json.dumps(batch), channel=channel, worker='score_by_tag_and_year')


@as_worker
def score_by_tag_and_year_callback(channel:BlockingChannel):
    """Collect the resulting joined answers and questions from the previous
    stage and aggregate the score by year and tag. Then proceed to send the
    data by batches to the next step."""

    from collections import Counter

    received, tags_per_year = 0, defaultdict(Counter)
    for _, _, body in consume_from(channel, 'score_by_tag_and_year'):
        rows = json.loads(body)
        for row in rows:
            year = row['CreationDate'][:4]

            tags = {tag: int(row['Score']) for tag in row['Tags'].split(' ')}
            tags_per_year[year].update(tags)
            
        received += len(rows)
        if received > 1000:
            # once we accumulated a sufficiently large batch, we pass it to the
            # next stage
            send_data(json.dumps(tags_per_year), channel=channel, worker='top_10_tags')
            received, tags_per_year = 0, defaultdict(Counter)

    # flush remaining data
    send_data(json.dumps(tags_per_year), channel=channel, worker='top_10_tags')


@as_worker
def top_10_tags_callback(channel:BlockingChannel):
    """Collects the aggregated data batches and calculates the final values.
    Finally, sorts the tags by score for each year to get the resulting
    top 10."""

    from collections import Counter

    tags_per_year = defaultdict(Counter)
    for _, _, body in consume_from(channel, 'top_10_tags'):
        data = json.loads(body)
        for year, tag_scores in data.items():
            tags_per_year[year].update(tag_scores)

    # print final result
    for year in sorted(tags_per_year):
        print(year)
        top_10 = tags_per_year[year].most_common(10)
        for tag, score in top_10:
            print('    ', tag, f'({score})')
