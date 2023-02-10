import random
import sched
import time

from src.things.Things import Things


class SoilSensor(Things):

    # Soil Moisture in percentage
    __moisture_min = 0
    __moisture_max = 100

    # Soil Temperature in Fahrenheit
    __temp_min = -4
    __temp_max = 158

    def __init__(self, id, lat, long, mapped_sprinkler, pub_topic, ss_prov_topic):
        self._soil_moisture = 50
        self._soil_temp = 35
        self._mapped_sprinkler = mapped_sprinkler
        self._publish_topic = pub_topic + mapped_sprinkler.split('_')[-1]
        self._provisioning_topic = ss_prov_topic
        super().__init__(id, 'SS', lat, long)

    def _provision_thing(self, ss_thing_add_msg):
        ss_thing_add_msg = {"device_id": self._id,
                            "type": self._type,
                            "request": 'provision',
                            "mapped_sprinkler_id": self._mapped_sprinkler,
                            "lat": self._lat,
                            "long": self._long,
                            "timestamp": self.get_current_timestamp()}
        super()._provision_thing(ss_thing_add_msg)

    def __publish(self):
        msg = dict()
        msg['device_id'] = self._id
        msg['type'] = self._type
        msg['timestamp'] = self.get_current_timestamp()
        msg['soil_moisture'] = random.randint(SoilSensor.__moisture_min, SoilSensor.__moisture_max)
        msg['soil_temperature'] = random.randint(SoilSensor.__temp_min, SoilSensor.__temp_max)
        # msg['data'] = [{"data_type": "soil_moisture",
        #                "value": random.randint(SoilSensor.__moisture_min, SoilSensor.__moisture_max)},
        #               {"data_type": "soil_temperature",
        #                "value": random.randint(SoilSensor.__temp_min, SoilSensor.__temp_max)}]
        super()._pub_to_topic(self._publish_topic, msg)

    def publish_scheduler_self_loop(self):
        loop_thru = True
        scheduler = sched.scheduler(time.time, time.sleep)
        while loop_thru:
            try:
                now = time.time()
                # Schedules for every 30 sec
                scheduler.enterabs(now + 30, 1, self.__publish)
                scheduler.run()
                time.sleep(28)
            except KeyboardInterrupt:
                print('Keyboard interrupt received for {} sensor, hence process will be halted.'.format(self._id))
                loop_thru = False

    def publish_scheduler(self, schedule_time):
        scheduler = sched.scheduler(time.time, time.sleep)
        now = time.time()
        # Schedules for every 30 sec
        # scheduler.enterabs(now + 15, 1, self.__publish)
        scheduler.enterabs(schedule_time, 1, self.__publish)
        scheduler.run()
