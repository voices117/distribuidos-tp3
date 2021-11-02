FROM python:3.9.7-slim

COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY ./ /app
WORKDIR /app
ENTRYPOINT ["python3"]