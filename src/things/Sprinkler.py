import json
import sched
import time

from src.things.Things import Things

sp_things_obj_list = list()


def sprinkler_process(sp, lat, long, sub_topic, prov_topic):
    temp_sp = Sprinkler(sp, lat, long, sub_topic, prov_topic)
    sp_things_obj_list.append(temp_sp)


def _on_message(client, userdata, msg):
    # super(Sprinkler, self)._on_message(userdata, msg)
    temp_msg = json.loads(msg.payload.decode())
    topic_no = int(msg.topic.split('_')[-1]) - 1
    sp_things_obj_list[topic_no].process_message(temp_msg)


class Sprinkler(Things):
    __allowed_states = ['ON', 'OFF']
    __command_msgs_list = list()

    def __init__(self, id, lat, long, sub_topic, sp_prov_topic):
        self.state = 'OFF'
        self.req_topic = ''
        self.req_res_topic = ''
        self.sub_topic = sub_topic + id.split('_')[-1]
        self._provisioning_topic = sp_prov_topic
        super().__init__(id, 'SP', lat, long)
        self._iot_mqtt_client.subscribe(self.sub_topic, 1, _on_message)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def req_topic(self):
        return self._req_topic

    @req_topic.setter
    def req_topic(self, topic_name):
        self._req_topic = topic_name

    @property
    def req_res_topic(self):
        return self._req_res_topic

    @req_res_topic.setter
    def req_res_topic(self, topic_name):
        self._req_res_topic = topic_name

    def _provision_thing(self, sp_thing_add_msg):
        sp_thing_add_msg = {"device_id": self._id,
                            "type": self._type,
                            "request": 'provision',
                            "lat": self._lat,
                            "long": self._long,
                            "sprinkler_state": self.state,
                            "timestamp": self.get_current_timestamp()}
        super()._provision_thing(sp_thing_add_msg)

    def _process_state_change(self, value):
        if value in Sprinkler.__allowed_states:
            if value != self.state:
                print("Request to change sprinkler - {} status from {} to {}.".format(self._id, self.state, value))
                self.state = value
            else:
                print("Request state change for sprinkler - {} is same as current status {}.".format(self._id,
                                                                                                     self.state))
        else:
            print("Request to change sprinkler - {} status from {} to {} which invalid.".format(self._id,
                                                                                                self.state,
                                                                                                value))
            print("Valid requests are - {}".format(', '.join(Sprinkler.__allowed_states)))

    def process_message(self, temp_msg):
        if 'device_id' in temp_msg and temp_msg['device_id'] == self._id:
            if 'response' in temp_msg and temp_msg['response'] == 'provision':
                self.req_topic = temp_msg['req_topic']
                self.req_res_topic = temp_msg['req_resp_topic']
            elif 'request' in temp_msg and temp_msg['request'] == 'status_change_command':
                print('Processing the status change command')
                self._process_state_change(temp_msg['sprinkler_state'])
            else:
                print('Received message does not have request to change the state or provision acknowledgement!')
                print('\n')
                print(temp_msg)
            print('Sprinkler responding back to the core with current state detail')
        else:
            print('Incoming message does not have device_id - ', temp_msg)
            # self.publish()

    def subscribe_data_check(self):
        pass

