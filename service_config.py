"""
This file defines the pipeline's DAG and the number of workers to process each
stage.
"""

# indicates the number of nodes on each stage
# this is used by `compose_builder.py` to define how many services to create
WORKERS = {
    'client_answers': 1,  # not an actual worker, but helps to avoid special cases
    'client_questions': 1,  # not an actual worker, but helps to avoid special cases

    # input
    'answers_csv_parser': 3,
    'questions_csv_parser': 3,

    # pipeline 1
    'filter_by_score': 2,
    'filter_by_sentiment_analysis': 6,
    'calculate_percentage': 1,

    # pipeline 2
    'score_by_user': 4,
    'filter_top_10_by_score': 1,

    # pipeline 3
    'join': 3,
    'score_by_tag_and_year': 4,
    'top_10_tags': 1,
}


# adjacency list representing the DAG. This is required so each worker knows
# how many workers are on the other stages to receive and send DONE messages.
NEXT_TASK = {
    'client_answers': ['answers_csv_parser'],
    'client_questions': ['questions_csv_parser'],

    # input
    'questions_csv_parser': ['score_by_user', 'join'],
    'answers_csv_parser': ['filter_by_score', 'score_by_user', 'join'],

    # pipeline 1
    'filter_by_score': ['calculate_percentage', 'filter_by_sentiment_analysis'],
    'filter_by_sentiment_analysis': ['calculate_percentage'],
    'calculate_percentage': [],

    # pipeline 2
    'score_by_user': ['filter_top_10_by_score'],
    'filter_top_10_by_score': [],

    # pipeline 3
    'join': ['score_by_tag_and_year'],
    'score_by_tag_and_year': ['top_10_tags'],
    'top_10_tags': [],
}


# list of stages that require the input to be sharded (i.e. to guarantee that
# each node processes all instances that share a common key).
SHARDED = [
    'join',
]

LIBRARIANS=[
    {
        "id": 1,
        "name": "librarian_1",
        "port": 5000
    },
    {
        "id": 2,
        "name": "librarian_2",
        "port": 5001
    },
    {
        "id": 3,
        "name": "librarian_3",
        "port": 5002
    },
    # {
    #     "id": 4,
    #     "name": "librarian_4",
    #     "port": 5003
    # },
    # {
    #     "id": 5,
    #     "name": "librarian_5",
    #     "port": 5004
    # }
]

MAX_QUEUE_SIZE=5
TIMEOUT=1