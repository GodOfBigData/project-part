from kafka import KafkaConsumer
import logging
from support.config import bootstrap_servers, topic, username_db, password_db, addres, port, db, table
from time import sleep
from sqlalchemy import create_engine
from json import loads

log_info = logging.getLogger('info')
log_error = logging.getLogger('error')

file_handler_info = logging.FileHandler(filename='logs/consumer.log')
file_handler_info.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M'))
file_handler_info.setLevel(logging.INFO)
log_info.addHandler(file_handler_info)
log_info.setLevel(logging.INFO)


file_handler_error = logging.FileHandler(filename='logs/consumer.log')
file_handler_error.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M'))
file_handler_error.setLevel(logging.ERROR)
log_error.addHandler(file_handler_error)
log_error.setLevel(logging.ERROR)

count_events = 0
while True:
    try:
        engine = create_engine(f"postgresql://{username_db}:{password_db}@{addres}:7856/{db}")
        with engine.connect() as conn:
            log_info.info(msg=f"consumer connected to greenplum")
            tables = list(conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name='{table}';").fetchall()[0])
            if len(tables) == 0:
                conn.execute(f"create table {table}(time_event character varying," 
                            "timestamp_event integer, id_sensor bigint, coordinates point," 
                            "temp_controller integer, id_controler bigint, name_sensor character varying) distributed by (id_sensor);")
            while True:
                try:
                    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
                    log_info.info(msg=f"consumer connected to kafka")                    
                    for msg in consumer:
                        data = loads(msg.value)
                        conn.execute(f"INSERT INTO device_info_stg VALUES "
                                    f"('{data['time_event']}', {data['timestamp_event']}," 
                                    f"{data['id_sensor']},'({data['latitude']}, {data['longitude']})'," 
                                    f"{data['temp_controller']}, {data['id_controler']}, '{data['name_sensor']}');")
                        sleep(1)
                        count_events += 1
                        if count_events % 10 == 0:
                            log_info.info(msg=f"consumer sent {count_events} events to greenplum")
                except Exception as exc:
                    log_error.error(msg=exc)
                    sleep(5)
    except Exception as exc:
        log_error.error(msg=f"consumer did not connect to greenplum: {exc}")
        sleep(5)
