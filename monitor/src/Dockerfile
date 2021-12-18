FROM python:3.7-alpine
RUN apk update
RUN apk add docker
RUN pip install requests
COPY . /src
WORKDIR /src
ENTRYPOINT python3 -u sauronEye.py
