import os
import csv
import json
import pika
import time
import service_config

from io import StringIO
from pika import connection
from typing import Generator, List
from collections import defaultdict
from service_config import WORKERS, NEXT_TASK
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


WORKERS_TO_WAIT = defaultdict(int)
for task, next_tasks in NEXT_TASK.items():
    for next in next_tasks:
        WORKERS_TO_WAIT[next] += 1 * WORKERS[task]

DONE_MESSAGES_RECEIVED = defaultdict(int)


def send_data(data:bytes, channel:BlockingChannel, worker:str, id='', shard_key=None):
    exchange, routing_key = '', worker

    if worker in service_config.SHARDED:
        exchange, routing_key = worker, shard_key
    
    assert routing_key, routing_key
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=data,
        mandatory=1,
        properties=pika.BasicProperties(
            delivery_mode=1,  # transient delivery mode
            correlation_id=id,
        )
    )

def send_done(channel:BlockingChannel, worker:str):
    num_messages = WORKERS[worker]
    for i in range(num_messages):
        send_data(data=b'', channel=channel, worker=worker, shard_key=str(i))

def is_task_done(worker_name:str):
    return DONE_MESSAGES_RECEIVED[worker_name] == WORKERS_TO_WAIT[worker_name]

def register_done(channel:BlockingChannel, worker_name:str):
    DONE_MESSAGES_RECEIVED[worker_name] += 1
    if is_task_done(worker_name=worker_name):
        print(worker_name, 'received', DONE_MESSAGES_RECEIVED[worker_name], 'done')
        for next_task in NEXT_TASK[worker_name]:
            send_done(channel=channel, worker=next_task)
        raise ExitWorkerException()


class ExitWorkerException(Exception):
    pass


from functools import wraps

def as_worker(task_callback):
    @wraps(task_callback)
    def _wrapper(channel:BlockingChannel):
        task_callback(channel)
        worker_name = task_callback.__name__.replace('_callback', '')

        for next_task in NEXT_TASK[worker_name]:
            send_done(channel=channel, worker=next_task)

        print(worker_name, 'finished')

    return _wrapper


#################################################################################
#################################################################################
#################################################################################

def csv_parse(body:bytes, columns_to_keep:List[str]):
    def _drop_fields(record:dict) -> dict:
        return {
            field: record[field]
            for field in columns_to_keep
        }

    data = StringIO(body.decode('utf-8'))
    output = [
        _drop_fields(record=record)
        for record in csv.DictReader(data)
    ]
    print(len(output))
    return output


def select_cols(records:List[dict], cols:List[str]) -> List[dict]:
    def _select_fields(record:dict) -> dict:
        return {
            field: record[field]
            for field in cols
        }

    return [_select_fields(record) for record in records]


@as_worker
def answers_csv_parser_callback(channel:BlockingChannel):
    sharded_data = [list() for _ in range(WORKERS['join'])]

    for _, _, body in consume_from(channel, 'answers_csv_parser'):
        rows = csv_parse(body, columns_to_keep=['ParentId', 'CreationDate', 'Score', 'Body', 'OwnerUserId'])

        data = json.dumps(select_cols(rows, ['OwnerUserId', 'Score']))
        send_data(data, channel=channel, worker='score_by_user', id='answers')

        data = json.dumps(select_cols(rows, ['Body', 'Score']))
        send_data(data, channel=channel, worker='filter_by_score', id='answers')

        # collect batches of rows to join to avoid sending too many messages
        for row in select_cols(rows, ['ParentId', 'CreationDate', 'Score']):
            shard = hash(row['ParentId']) % WORKERS['join']
            sharded_data[shard].append(row)

        for shard, data in enumerate(sharded_data):
            if len(data) > len(rows) / 2:
                send_data(json.dumps(data), channel=channel, worker='join', id='answers', shard_key=str(shard))

    # flush data
    for shard, data in enumerate(sharded_data):
        if data:
            send_data(json.dumps(data), channel=channel, worker='join', id='answers', shard_key=str(shard))



#################################################################################
#################################################################################
#################################################################################

@as_worker
def filter_by_score_callback(channel:BlockingChannel):
    for _, _, body in consume_from(channel, 'filter_by_score'):
        records = json.loads(body)
        filtered = [
            record for record in records
            if record['Score'] is not None and int(record['Score']) > 10
        ]
        if filtered:
            send_data(json.dumps(filtered), channel=channel, worker='filter_by_sentiment_analysis')
            send_data(str(len(filtered)), channel=channel, worker='calculate_percentage', id='denominator')


#################################################################################
#################################################################################
#################################################################################

def _get_sentiment_analysis_score(body:str) -> int:
    # TODO: use NLTK
    return (hash(body) % 3) - 1


@as_worker
def filter_by_sentiment_analysis_callback(channel:BlockingChannel):
    for _, _, body in consume_from(channel, 'filter_by_sentiment_analysis'):
        records = json.loads(body)
        filtered = [
            record for record in records
            if _get_sentiment_analysis_score(record['Body']) < 0
        ]
        if filtered:
            send_data(str(len(filtered)), channel=channel, worker='calculate_percentage', id='numerator')


#################################################################################
#################################################################################
#################################################################################

@as_worker
def calculate_percentage_callback(channel:BlockingChannel):
    final_count_total, final_count_negative = 0, 0

    for _, properties, body in consume_from(channel, 'calculate_percentage'):
        count = int(body)
        if properties.correlation_id == 'numerator':
            final_count_negative += count
        elif properties.correlation_id == 'denominator':
            final_count_total += count
        else:
            assert False, properties.correlation_id

    print('>>> percentage of answers with score > 10 and negative S.A. is', final_count_negative / final_count_total)


#################################################################################
######                              PIPELINE 2                             ######
#################################################################################

@as_worker
def questions_csv_parser_callback(channel:BlockingChannel):
    sharded_data = [list() for _ in range(WORKERS['join'])]

    for _, _, body in consume_from(channel, 'questions_csv_parser'):
        rows = csv_parse(body=body, columns_to_keep=['Id', 'CreationDate', 'OwnerUserId', 'Score', 'Tags'])

        data = select_cols(rows, ['OwnerUserId', 'Score'])
        send_data(json.dumps(data), channel=channel, worker='score_by_user', id='questions')

        # collect batches of rows to join to avoid sending too many messages
        for row in select_cols(rows, ['Id', 'Tags', 'Score', 'CreationDate']):
            shard = hash(row['Id']) % WORKERS['join']
            sharded_data[shard].append(row)

        for shard, data in enumerate(sharded_data):
            if len(data) > len(rows) / 2:
                send_data(json.dumps(data), channel=channel, worker='join', id='questions', shard_key=str(shard))

    # flush data
    for shard, data in enumerate(sharded_data):
        if data:
            send_data(json.dumps(data), channel=channel, worker='join', id='questions', shard_key=str(shard))



@as_worker
def score_by_user_callback(channel:BlockingChannel):
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

    print(top_ten)


#################################################################################
#################################################################################
#################################################################################

@as_worker
def join_callback(channel:BlockingChannel):
    questions, answers = {}, {}
    for _, properties, body in consume_from(channel, 'join'):
        batch = []
        
        data = json.loads(body)
        if properties.correlation_id == 'questions':
            for question in data:
                questions[question['Id']] = question
                batch.append(question)
        elif properties.correlation_id == 'answers':
            for answer in data:
                answers[answer['ParentId']] = answer
        else:
            assert False, f'unexpected message "{properties.correlation_id}"'

        # flush
        for parent_id, answer in answers.items():
            if question := questions.get(parent_id):
                answer['Tags'] = question['Tags']
                batch.append(answer)

        for joined in batch:
            if parent_id := joined.get('ParentId'):
                del answers[parent_id]

        send_data(json.dumps(batch), channel=channel, worker='score_by_tag_and_year')

    #assert len(answers) == 0, answers


@as_worker
def score_by_tag_and_year_callback(channel:BlockingChannel):
    from collections import Counter

    tags_per_year = defaultdict(Counter)
    received = 0
    for _, _, body in consume_from(channel, 'score_by_tag_and_year'):
        rows = json.loads(body)
        for row in rows:
            year = row['CreationDate'][:4]

            tags = {tag: int(row['Score']) for tag in row['Tags'].split(' ')}
            tags_per_year[year].update(tags)
            
        received += len(rows)
        if received > 1000:
            send_data(json.dumps(tags_per_year), channel=channel, worker='top_10_tags')
            received = 0
            tags_per_year = defaultdict(Counter)
    
    send_data(json.dumps(tags_per_year), channel=channel, worker='top_10_tags')


@as_worker
def top_10_tags_callback(channel:BlockingChannel):
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


#################################################################################
#################################################################################
#################################################################################

def connect(address:str) -> BlockingConnection:
    success = False
    for _ in range(15):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_ADDRESS))
            success = True
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    
    if not success:
        raise Exception('could not connect to rabbit MQ')

    return connection

def consume_from(channel:BlockingChannel, worker_name:str) -> Generator:
    if worker_name in service_config.SHARDED:
        result = channel.queue_declare(queue='', exclusive=True, durable=False)
        queue_name = result.method.queue

        channel.queue_bind(
            exchange=worker_name,
            queue=queue_name,
            routing_key=os.environ['WORKER_ID']
        )
    else:
        queue_name = worker_name

    for method_frame, properties, body in channel.consume(queue_name, auto_ack=False):
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        if body == b'':
            #channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            DONE_MESSAGES_RECEIVED[worker_name] += 1

            if is_task_done(worker_name=worker_name):
                print(worker_name, 'exiting, received', DONE_MESSAGES_RECEIVED[worker_name], 'done')
                return

            continue

        yield method_frame, properties, body

        #channel.basic_ack(delivery_tag=method_frame.delivery_tag)



if __name__ == '__main__':
    RABBITMQ_ADDRESS = os.environ['RABBITMQ_ADDRESS']
    WORKER_TASK = os.environ['WORKER_TASK']
    WORKER_ID = os.environ['WORKER_ID']

    connection = connect(RABBITMQ_ADDRESS)
    channel = connection.channel()

    try:
        for worker in WORKERS:
            if worker.startswith('client'):
                continue

            if worker in service_config.SHARDED:
                channel.exchange_declare(exchange=worker, exchange_type='direct')
            else:
                channel.queue_declare(queue=worker, durable=False)

        print(WORKER_TASK, 'starting')
        callback = globals()[f'{WORKER_TASK}_callback']
        callback(channel)
    except ExitWorkerException:
        print(WORKER_TASK, 'graceful exit')
    finally:
        channel.close()
        connection.close()
