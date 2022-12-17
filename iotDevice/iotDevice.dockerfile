FROM python:3.10 as builder

RUN mkdir -p /app

RUN mkdir -p /app/support

RUN mkdir -p /app/logs

WORKDIR /app

RUN pip install kafka-python 

COPY  iotDevice.py iotDevice.py

COPY  sensor.py sensor.py

EXPOSE 8080

CMD [ "sleep", "30s"]

CMD [ "python3", "iotDevice.py"]