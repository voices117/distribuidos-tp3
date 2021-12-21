"""
This file contains the tasks that solve the second item required:
Top 10 of users by total score after filtering by the global average question
and answer scores.
"""

import json
from typing import Dict

from middleware import as_worker, consume_from, send_to_client, send_data, END_OF_STREAM
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def score_by_user_callback(channel:BlockingChannel, worker_id:str):
    """Accumulates the quesion and answer scores for each user.
    The output of this stage is a dict with a key for each user mapped to
    the score and count of questions and answers."""

    from collections import Counter

    def _default():
        return {
            'questions': Counter(score=0, count=0),
            'answers': Counter(score=0, count=0),
        }

    batch_id:Dict[str, int] = defaultdict(int)
    score_by_user = defaultdict(lambda: defaultdict(_default))
    for correlation_id, body in consume_from(channel, 'score_by_user', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            data = json.dumps(score_by_user[correlation_id])
            send_data(
                data,
                channel=channel,
                worker='filter_top_10_by_score',
                correlation_id=correlation_id,
                shard_key=batch_id[correlation_id]
            )

            score_by_user.pop(correlation_id, None)
            batch_id.pop(correlation_id, None)
            continue

        data = json.loads(body)
        if 'answers' in data:
            type = 'answers'
        elif 'questions' in data:
            type = 'questions'
        else:
            assert False, f'Unexpected message {data}'[:120]

        for record in data[type]:
            user_id = record['OwnerUserId']
            score_by_user[correlation_id][user_id][type]['score'] += int(record['Score'])
            score_by_user[correlation_id][user_id][type]['count'] += 1


@as_worker
def filter_top_10_by_score_callback(channel:BlockingChannel, worker_id:str):
    """This stage accumulates the aggregated user data into a global average
    that is then used to filter the individual user data to get the final
    result."""

    from collections import Counter

    def _default():
        return {
            'questions': Counter(score=0, count=0),
            'answers': Counter(score=0, count=0),
        }

    questions_stats = defaultdict(lambda: Counter(score=0, count=0))
    answers_stats = defaultdict(lambda: Counter(score=0, count=0))
    score_by_user = defaultdict(lambda: defaultdict(_default))

    for correlation_id, body in consume_from(channel, 'filter_top_10_by_score', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            _calculate_final_score(
                channel=channel,
                correlation_id=correlation_id,
                questions_stats=questions_stats,
                answers_stats=answers_stats,
                score_by_user=score_by_user
            )
            questions_stats.pop(correlation_id, None)
            answers_stats.pop(correlation_id, None)
            score_by_user.pop(correlation_id, None)
            continue
    
        for user_id, stats in json.loads(body).items():
            # per user accumulator
            score_by_user[correlation_id][user_id]['questions'].update(stats['questions'])
            score_by_user[correlation_id][user_id]['answers'].update(stats['answers'])

            # global accumulator
            questions_stats[correlation_id].update(stats['questions'])
            answers_stats[correlation_id].update(stats['answers'])


def _calculate_final_score(channel, correlation_id:str, questions_stats, answers_stats, score_by_user):
    # global averages
    avg_question_score = questions_stats[correlation_id]['score'] / questions_stats[correlation_id]['count']
    avg_answer_score = answers_stats[correlation_id]['score'] / answers_stats[correlation_id]['count']

    # top ten
    import heapq
    top_ten = []
    for user_id, stats in score_by_user[correlation_id].items():
        if stats['questions']['count'] == 0 or stats['answers']['count'] == 0:
            continue

        q_score = stats['questions']['score'] / stats['questions']['count']
        a_score = stats['answers']['score']   / stats['answers']['count']

        if q_score > avg_question_score and a_score > avg_answer_score:
            score = stats['answers']['score'] + stats['questions']['score']
            heapq.heappush(top_ten, (score, user_id))

            if len(top_ten) > 10:
                heapq.heappop(top_ten)

    response  = '==========\n'
    response += '= Top 10 =\n'
    response += '==========\n'
    for i, (score, user) in enumerate(sorted(top_ten, reverse=True)):
        response += f' {i+1: 2} User: {user}  Score: {score}\n'

    send_to_client(channel=channel, correlation_id=correlation_id, body=response.encode('utf-8'))
