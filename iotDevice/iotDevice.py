from json import dumps
from time import sleep
from kafka import KafkaProducer
from sensor import Sensor
from support.config import bootstrap_servers, topic
from support.source import informationSensor
from threading import Thread
import logging

log_info = logging.getLogger('info')
log_error = logging.getLogger('error')

file_handler_info = logging.FileHandler(filename='logs/sensor.log')
file_handler_info.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M'))
file_handler_info.setLevel(logging.INFO)
log_info.addHandler(file_handler_info)
log_info.setLevel(logging.INFO)


file_handler_error = logging.FileHandler(filename='logs/sensor.log')
file_handler_error.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M'))
file_handler_error.setLevel(logging.ERROR)
log_error.addHandler(file_handler_error)
log_error.setLevel(logging.ERROR)

def produce_event(name_sensor, data_sensor):
    count_events = 0
    ID_SENSOR = data_sensor["ID_SENSOR"]
    is_conected = False
    while is_conected == False:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, api_version=(0, 10, 1), value_serializer=lambda v: dumps(v).encode('utf-8'))
            log_info.info(msg=f"sensor with id {ID_SENSOR}, name {name_sensor} connected to kafka")
            is_conected = True
        except Exception as exc:
            log_error.error(msg=f"sensor with id {ID_SENSOR}, name {name_sensor} did not connect to kafka: {exc}")
            sleep(5)
    sensor = Sensor(id_sensor = ID_SENSOR, latitude = data_sensor["LATITUDE"], 
                    longitude = data_sensor["LONGITUDE"], name_sensor = name_sensor)
    while True:
        data_event = sensor.get_data_events()
        try:
            producer.send(topic, data_event)
            producer.flush()
        except Exception as exc:
            log_error.error(msg=exc)
            continue
        if count_events % 10 == 0:
            log_info.info(msg=f"sensor with id {ID_SENSOR}, name {name_sensor} sent {count_events} events")
        count_events += 1
        sleep(1)        
    
list_thred = list()

for name_sensor, data_sensor in informationSensor.items():
    list_thred.append(Thread(target=produce_event, args=[name_sensor, data_sensor]))

for thread in list_thred:
    thread.start()
