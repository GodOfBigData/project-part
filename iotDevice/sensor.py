from datetime import datetime
from random import randrange


class Sensor:

    def __init__(self, id_sensor, latitude, longitude, name_sensor):
        self.id_sensor = id_sensor
        self.latitude = latitude
        self.longitude = longitude
        self.name_sensor = name_sensor

    def get_data_events(self):
        time_event = datetime.utcnow()
        date_time_string = time_event.strftime("%d.%m.%Y %H:%M:%S")
        timestamp_event = time_event.timestamp()
        temp_controller = randrange(-20, 20)
        id_controler = hash(self.id_sensor)
        data_event = dict(time_event = date_time_string, timestamp_event = timestamp_event, id_sensor = self.id_sensor, latitude = self.latitude,
                        longitude = self.longitude, temp_controller = temp_controller, id_controler = id_controler, name_sensor = self.name_sensor)
        return data_event