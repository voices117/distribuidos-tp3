import os
import csv
import pika
import threading
import middleware

from io import StringIO
from typing import List
import sys

def perror(msg, end='\n'):
    sys.stderr.write(msg + end)

def _serialize_chunk(header:List[str], chunk:List[str]):
    """Serializes the given CSV chunk including the header."""

    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerow(header)
    writer.writerows(chunk)

    sio.seek(0)
    return sio.read()


def _read_lines(reader:csv.reader, n:int) -> List[str]:
    """Reads `n` lines from the CSV reader."""

    lines:List[str] = []
    for i, line in enumerate(reader):
        lines.append(line)
        if i == n - 1:
            break

    return lines


def read_file_by_chunks(file_name:str, lines:int, chunks:int = -1):
    """Yields chunks of `lines` from a file. The last chunk may contain
    less lines than indicated.
    `chunks` is the number of chunks to yield. If negative then the whole
    file is processed."""

    with open(file_name, 'r') as fp:
        reader = csv.reader(fp)
        header = next(reader)

        read_chunks = 0
        while chunks < 0 or chunks > read_chunks:
            chunk = _read_lines(reader, n=lines)

            yield _serialize_chunk(header=header, chunk=chunk)
            if len(chunk) < lines:
                # finished processing the file
                return

            read_chunks += 1


def upload_csv(routing_key:str, file_name:str, lines:int, chunks:int, correlation_id:str):
    """Sends a CSV file as chunks through a rabbit MQ exchange. The CSV header
    is repeated on each chunk."""

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_ADDRESS))
    channel = connection.channel()

    try:
        for i, chunk in enumerate(read_file_by_chunks(file_name=file_name, lines=lines, chunks=chunks)):
            perror(f'sending chunk {i}   ', end='\r')

            middleware.send_data(chunk.encode('utf-8'), channel=channel, worker=routing_key, correlation_id=correlation_id)

        middleware.send_done(channel=channel, worker=routing_key, correlation_id=correlation_id)
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    RABBITMQ_ADDRESS = os.environ['RABBITMQ_ADDRESS']

    LINES_PER_CHUNK = int(os.environ.get('LINES_PER_CHUNK', 1000))
    NUM_CHUNKS = int(os.environ.get('NUM_CHUNKS', -1))
    CORRELATION_ID = os.environ.get('CORRELATION_ID')

    connection, channel, response_queue, correlation_id = middleware.build_response_queue(
        rbmq_address=RABBITMQ_ADDRESS,
        correlation_id=CORRELATION_ID
    )

    perror(f'start client using request ID {correlation_id}')

    def upload_answers():
        upload_csv(
            routing_key='answers_csv_parser',
            file_name='data/answers.csv',
            lines=LINES_PER_CHUNK,
            chunks=NUM_CHUNKS,
            correlation_id=correlation_id
        )

    def upload_questions():
        upload_csv(
            routing_key='questions_csv_parser',
            file_name='data/questions.csv',
            lines=LINES_PER_CHUNK,
            chunks=NUM_CHUNKS,
            correlation_id=correlation_id
        )


    try:
        t1 = threading.Thread(target=upload_questions)
        t2 = threading.Thread(target=upload_answers)

        perror('starting threads')
        t1.start(); t2.start()

        t1.join(); t2.join()
        perror('joined threads')

        perror('waiting for response...')
        # expects 3 messages (one por each result in the output of the pipeline)
        for msg in middleware.consume_response(channel=channel, queue_name=response_queue):
            # prints the pipeline response
            print(msg)
    finally:
        channel.close()
        connection.close()
