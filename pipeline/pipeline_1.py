"""
This file contains the tasks that solve the first item required:
Get the percentage of answers with a score higher than 10 and a
negative sentiment analysis.
"""

import json
import service_config

from typing import Dict
from collections import defaultdict
from middleware import as_worker, consume_from, send_data, END_OF_STREAM
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def filter_by_score_callback(channel:BlockingChannel, worker_id:str):
    """Receives batches of answers, filters them and then redirects the
    remaining ones to the next stages of the pipeline."""

    batch_id:Dict[str, int] = defaultdict(int)
    for correlation_id, body in consume_from(channel, 'filter_by_score'):
        if isinstance(body, END_OF_STREAM):
            batch_id.pop(correlation_id, None)
            continue

        records = json.loads(body)['answers']
        filtered = [
            record for record in records
            if record['Score'] is not None and int(record['Score']) > 10
        ]

        if filtered:
            send_data(json.dumps(filtered), channel=channel, worker='filter_by_sentiment_analysis', correlation_id=correlation_id)
            send_data(
                # include batch ID so the next stage can detect duplicates
                json.dumps({'id': f'{worker_id}_{batch_id[correlation_id]}', 'denominator': len(filtered)}),
                channel=channel,
                worker='calculate_percentage',
                correlation_id=correlation_id,
                shard_key=batch_id[correlation_id]
            )
            batch_id[correlation_id] += 1


@as_worker
def filter_by_sentiment_analysis_callback(channel:BlockingChannel, worker_id:str):
    """Applies sentiment analysis to the batch of answers received and filters
    the ones with negative value. The count of the remaining answers is sent to
    the last stage that calculates the percentage."""

    batch_id:Dict[str, int] = defaultdict(int)
    analyzer = SentimentIntensityAnalyzer()
    for correlation_id, body in consume_from(channel, 'filter_by_sentiment_analysis'):
        if isinstance(body, END_OF_STREAM):
            batch_id.pop(correlation_id, None)
            continue

        records = json.loads(body)
        filtered = [
            record for record in records
            if analyzer.polarity_scores(record['Body'])['compound'] < 0
        ]
        if filtered:
            send_data(
                # include batch ID so the next stage can detect duplicates
                json.dumps({'id': f'{worker_id}_{batch_id[correlation_id]}', 'numerator': len(filtered)}),
                channel=channel,
                worker='calculate_percentage',
                correlation_id=correlation_id,
                shard_key=batch_id[correlation_id]
            )
            batch_id[correlation_id] += 1


@as_worker
def calculate_percentage_callback(channel:BlockingChannel, worker_id:str):
    """Expects 2 types of packages: the output after filtering by score
    and the output after filtering by sentiment. All packages are integers."""

    final_count_total:Dict[str, int] = defaultdict(int)
    final_count_negative:Dict[str, int] = defaultdict(int)

    # consumes all input packages until done
    for correlation_id, body in consume_from(channel, 'calculate_percentage', remove_duplicates=True):
        if isinstance(body, END_OF_STREAM):
            p = final_count_negative[correlation_id] / final_count_total[correlation_id]
            print('>>> percentage of answers with score > 10 and negative S.A. is', p)

            final_count_total.pop(correlation_id, None)
            final_count_negative.pop(correlation_id, None)
            continue

        data = json.loads(body)
        if 'numerator' in data:
            final_count_negative[correlation_id] += data['numerator']
        elif 'denominator' in data:
            final_count_total[correlation_id] += data['denominator']
        else:
            assert False, data

