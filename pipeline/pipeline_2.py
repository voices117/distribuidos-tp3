"""
This file contains the tasks that solve the second item required:
Top 10 of users by total score after filtering by the global average question
and answer scores.
"""

import json

from middleware import as_worker, consume_from, send_data
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def score_by_user_callback(channel:BlockingChannel):
    """Accumulates the quesion and answer scores for each user.
    The output of this stage is a dict with a key for each user mapped to
    the score and count of questions and answers."""

    from collections import Counter

    def _default():
        return {
            'questions': Counter(score=0, count=0),
            'answers': Counter(score=0, count=0),
        }

    score_by_user = defaultdict(_default)
    for _, properties, body in consume_from(channel, 'score_by_user'):
        data = json.loads(body)
        for record in data:
            type = properties.correlation_id

            user_id = record['OwnerUserId']
            score_by_user[user_id][type]['score'] += int(record['Score'])
            score_by_user[user_id][type]['count'] += 1

    data = json.dumps(score_by_user)
    send_data(data, channel=channel, worker='filter_top_10_by_score')


@as_worker
def filter_top_10_by_score_callback(channel:BlockingChannel):
    """This stage accumulates the aggregated user data into a global average
    that is then used to filter the individual user data to get the final
    result."""

    from collections import Counter

    def _default():
        return {
            'questions': Counter(score=0, count=0),
            'answers': Counter(score=0, count=0),
        }

    questions_stats = Counter(score=0, count=0)
    answers_stats = Counter(score=0, count=0)
    score_by_user = defaultdict(_default)

    for _, _, body in consume_from(channel, 'filter_top_10_by_score'):
        for user_id, stats in json.loads(body).items():
            # per user accumulator
            score_by_user[user_id]['questions'].update(stats['questions'])
            score_by_user[user_id]['answers'].update(stats['answers'])

            # global accumulator
            questions_stats.update(stats['questions'])
            answers_stats.update(stats['answers'])

    # global averages
    avg_question_score = questions_stats['score'] / questions_stats['count']
    avg_answer_score = answers_stats['score'] / answers_stats['count']

    # top ten
    import heapq
    top_ten = []
    for user_id, stats in score_by_user.items():
        if stats['questions']['count'] == 0 or stats['answers']['count'] == 0:
            continue

        q_score = stats['questions']['score'] / stats['questions']['count']
        a_score = stats['answers']['score']   / stats['answers']['count']

        if q_score > avg_question_score and a_score > avg_answer_score:
            score = stats['answers']['score'] + stats['questions']['score']
            heapq.heappush(top_ten, (score, user_id))

            if len(top_ten) > 10:
                heapq.heappop(top_ten)

    print('==========')
    print('= Top 10 =')
    print('==========')
    for i, (score, user) in enumerate(sorted(top_ten)):
        print(f' {i+1: 2} ', user, score)
