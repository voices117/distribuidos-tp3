import csv
import os
import pika
import mmap
import logging
import service_config

from typing import Generator, Tuple
from pika.adapters.blocking_connection import BlockingChannel


Chunk = Generator[mmap.mmap, int, int]


def _find_1_record(mm:mmap.mmap, offset:int) -> Tuple[int, int]:
    """Returns the index of the ending of the next CSV record starting from 
    `offset` and appropriately escaping quotes."""

    while True:
        eol = mm.find(b'\n', offset)
        quote = mm.find(b'"', offset)

        if eol == -1:
            return mm.size() - 1

        if eol < quote:
            return eol + 1

        escaping = True
        while escaping:
            next_quote = quote = mm.find(b'"', quote + 1)
            next_next_quote = quote = mm.find(b'"', next_quote + 1)

            if next_quote + 1 == next_next_quote:
                # escaped quote, ignore
                quote = next_next_quote + 1
            else:
                escaping = False
                offset = next_quote + 1


def _find_n_records(mm:mmap.mmap, n:int, offset:int) -> Tuple[int, int]:
    """Starting at position `offset`, returns a tuple with number of lines
    found and the position where the next `n` lines end."""

    lines_count, end = 0, -1
    for _ in range(n):
        pos = _find_1_record(mm, offset=offset)

        lines_count += 1
        end = pos + 1
        offset = end

        if pos == mm.size() - 1:
            # no more data to read
            break
        
    return end, lines_count


def read_file_by_chunks(file_name:str, lines:int, chunks:int = -1) -> Chunk:
    """Yields chunks of `lines` from a file. The last chunk may contain
    less lines than indicated.
    `chunks` is the number of chunks to yield. If negative then the whole
    file is processed."""

    import csv
    with open(file_name, 'r') as fp:
        reader = csv.reader(fp)
        header = next(reader)
        read_chunks = 0
        while chunks < 0 or chunks > read_chunks:
            chunk = []
            for i, line in enumerate(reader):
                chunk.append(line)
                if i == lines - 1:
                    break

            from io import StringIO
            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerow(header)
            writer.writerows(chunk)
            sio.seek(0)
            chunk_data = sio.read()
            yield chunk_data
            if len(chunk) < lines:
                return
            read_chunks += 1

    return

    with open(file_name, 'r+b') as fp:
        read_chunks = 0
        with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            # reads the CSV header
            header_end, lines_found = _find_n_records(mm, n=1, offset=0)
            assert lines_found == 1
            
            chunk_start = header_end
            while chunks < 0 or chunks > read_chunks:
                chunk_end, lines_found = _find_n_records(mm, n=lines, offset=chunk_start)
                
                if lines_found == 0:
                    return

                yield mm, chunk_start, chunk_end
                chunk_start = chunk_end

                read_chunks += 1


def upload_csv(routing_key:str, file_name:str, lines:int, chunks:int):
    """Sends a CSV file as chunks through a rabbit MQ exchange. The CSV header
    is repeated on each chunk."""

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_ADDRESS))
    channel = connection.channel()


    i = 0
    for mm in read_file_by_chunks(file_name=file_name, lines=lines, chunks=chunks):
        i += 1
        print(f'sending chunk {i}', end='\r')
        #chunk_data = mm[start:end]
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=mm,
            mandatory=1,
            properties=pika.BasicProperties(
                delivery_mode=1,  # transient delivery mode
                #correlation_id=id,
            )
        )

    for _ in range(service_config.WORKERS[routing_key]):
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=b'',
            mandatory=1,
            properties=pika.BasicProperties(
                delivery_mode=1,  # transient delivery mode
                #correlation_id=id,
            )
        )

    connection.close()

    # signal end of transmission
    #messaging.emit_done(channel, '1.client.start')


if __name__ == '__main__':
    RABBITMQ_ADDRESS = os.environ['RABBITMQ_ADDRESS']
    #INPUT_QUEUE_NAME = os.environ['INPUT_QUEUE_NAME']
    INPUT_QUEUE_NAME = 'map'

    LINES_PER_CHUNK = int(os.environ['LINES_PER_CHUNK'])
    NUM_CHUNKS = int(os.environ['NUM_CHUNKS'])

    #logging.basicConfig(level=logging.INFO)
    logging.info(f'{RABBITMQ_ADDRESS=}, {INPUT_QUEUE_NAME=}, {LINES_PER_CHUNK=}, {NUM_CHUNKS=}')

    def upload_answers():
        upload_csv(
            routing_key='answers_csv_parser',
            file_name='data/answers.csv',
            lines=LINES_PER_CHUNK,
            chunks=NUM_CHUNKS
        )

    def upload_questions():
        upload_csv(
            routing_key='questions_csv_parser',
            file_name='data/questions.csv',
            lines=LINES_PER_CHUNK,
            chunks=NUM_CHUNKS
        )

    import threading

    t1 = threading.Thread(target=upload_questions)
    t2 = threading.Thread(target=upload_answers)

    print('starting threads')
    t1.start()
    t2.start()

    t1.join()
    t2.join()
    print('joined threads')
