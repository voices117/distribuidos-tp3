"""
This file contains the tasks that solve the first item required:
Get the percentage of answers with a score higher than 10 and a
negative sentiment analysis.
"""

import json

from middleware import as_worker, consume_from, send_data
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pika.adapters.blocking_connection import BlockingChannel


@as_worker
def filter_by_score_callback(channel:BlockingChannel):
    """Receives batches of answers, filters them and then redirects the
    remaining ones to the next stages of the pipeline."""
    
    for _, _, body in consume_from(channel, 'filter_by_score'):
        records = json.loads(body)
        filtered = [
            record for record in records
            if record['Score'] is not None and int(record['Score']) > 10
        ]

        if filtered:
            send_data(json.dumps(filtered), channel=channel, worker='filter_by_sentiment_analysis')
            send_data(str(len(filtered)), channel=channel, worker='calculate_percentage', id='denominator')


@as_worker
def filter_by_sentiment_analysis_callback(channel:BlockingChannel):
    """Applies sentiment analysis to the batch of answers received and filters
    the ones with negative value. The count of the remaining answers is sent to
    the last stage that calculates the percentage."""

    analyzer = SentimentIntensityAnalyzer()
    for _, _, body in consume_from(channel, 'filter_by_sentiment_analysis'):
        records = json.loads(body)
        filtered = [
            record for record in records
            if analyzer.polarity_scores(record['Body'])['compound'] < 0
        ]
        if filtered:
            send_data(
                str(len(filtered)),
                channel=channel,
                worker='calculate_percentage',
                id='numerator'
            )


@as_worker
def calculate_percentage_callback(channel:BlockingChannel):
    """Expects 2 types of packages: the output after filtering by score
    and the output after filtering by sentiment. All packages are integers."""

    final_count_total, final_count_negative = 0, 0

    # consumes all input packages until done
    for _, properties, body in consume_from(channel, 'calculate_percentage'):
        count = int(body)
        if properties.correlation_id == 'numerator':
            final_count_negative += count
        elif properties.correlation_id == 'denominator':
            final_count_total += count
        else:
            assert False, properties.correlation_id

    p = final_count_negative / final_count_total
    print('>>> percentage of answers with score > 10 and negative S.A. is', p)
