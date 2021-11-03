FROM python:3.9.7-slim

COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
RUN python -c 'import nltk; nltk.download("vader_lexicon")'

COPY ./ /app
WORKDIR /app
ENTRYPOINT ["python3"]
