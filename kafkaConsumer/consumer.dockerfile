FROM python:3.10 as builder

RUN mkdir -p /app

RUN mkdir -p /app/support

RUN mkdir -p /app/logs

WORKDIR /app

COPY  requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY  consumer.py consumer.py

EXPOSE 8181

CMD [ "sleep", "30s"]

CMD [ "python3", "consumer.py"]