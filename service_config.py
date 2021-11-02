WORKERS = {
    'client_answers': 1,  # not an actual worker, but helps to avoid special cases
    'client_questions': 1,  # not an actual worker, but helps to avoid special cases

    # input
    'answers_csv_parser': 7,
    'questions_csv_parser': 7,

    # pipeline 1
    'filter_by_score': 2,
    'filter_by_sentiment_analysis': 6,
    'calculate_percentage': 1,

    # pipeline 2
    'score_by_user': 3,
    'filter_top_10_by_score': 1,

    # pipeline 3
    'join': 4,
    'score_by_tag_and_year': 4,
    'top_10_tags': 1,
}


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


SHARDED = [
    'join',
]
