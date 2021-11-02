"""
This module contains the worker tasks that receive the CSV input from the
client. This is the entrypoint of the pipeline.
"""

import csv
import json
import service_config

from io import StringIO
from typing import List
from middleware import consume_from, send_data, as_worker
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def answers_csv_parser_callback(channel:BlockingChannel):
    """Parses CSV chunks of answers and sends the relevant columns of each row
    to the next stages of the pipeline."""

    # questions and answers related to them must be sent to the same "join"
    # worker. That is why we use the Id as the sharding key
    sharded_data = [list() for _ in range(service_config.WORKERS['join'])]

    for _, _, body in consume_from(channel, 'answers_csv_parser'):
        rows = _csv_parse(body)

        data = json.dumps(_select_cols(rows, ['OwnerUserId', 'Score']))
        send_data(data, channel=channel, worker='score_by_user', id='answers')

        data = json.dumps(_select_cols(rows, ['Body', 'Score']))
        send_data(data, channel=channel, worker='filter_by_score', id='answers')

        # collect batches of rows to join to avoid sending too many messages
        for row in _select_cols(rows, ['ParentId', 'CreationDate', 'Score']):
            shard = hash(row['ParentId']) % service_config.WORKERS['join']
            sharded_data[shard].append(row)

        # regularly flushes data to avoid chunks from getting too big
        for shard, data in enumerate(sharded_data):
            if len(data) > len(rows) / 2:
                send_data(json.dumps(data), channel=channel, worker='join', id='answers', shard_key=str(shard))

    # flush remaining data
    for shard, data in enumerate(sharded_data):
        if data:
            send_data(json.dumps(data), channel=channel, worker='join', id='answers', shard_key=str(shard))


@as_worker
def questions_csv_parser_callback(channel:BlockingChannel):
    """Parses CSV chunks of questions and sends the relevant columns of each
    row to the next stages of the pipeline."""

    sharded_data = [list() for _ in range(service_config.WORKERS['join'])]

    for _, _, body in consume_from(channel, 'questions_csv_parser'):
        rows = _csv_parse(body=body)

        data = _select_cols(rows, ['OwnerUserId', 'Score'])
        send_data(json.dumps(data), channel=channel, worker='score_by_user', id='questions')

        # collect batches of rows to join to avoid sending too many messages
        for row in _select_cols(rows, ['Id', 'Tags', 'Score', 'CreationDate']):
            shard = hash(row['Id']) % service_config.WORKERS['join']
            sharded_data[shard].append(row)

        for shard, data in enumerate(sharded_data):
            if len(data) > len(rows) / 2:
                send_data(json.dumps(data), channel=channel, worker='join', id='questions', shard_key=str(shard))

    # flush remaining data
    for shard, data in enumerate(sharded_data):
        if data:
            send_data(json.dumps(data), channel=channel, worker='join', id='questions', shard_key=str(shard))


def _csv_parse(body:bytes):
    """Parses a CSV from `body` and returns a list of dictionaries for each row."""

    data = StringIO(body.decode('utf-8'))
    return list(csv.DictReader(data))


def _select_cols(records:List[dict], cols:List[str]) -> List[dict]:
    """Return a new list containing dictionaries with only the subset of listed
    fields in `cols`."""

    def _select_fields(record:dict) -> dict:
        return {
            field: record[field]
            for field in cols
        }

    return [_select_fields(record) for record in records]
