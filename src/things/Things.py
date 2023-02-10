import datetime
import json

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import conf, get_file_path, check_file_existence


# Parent class for IoT Sensors or sprinklers
# It retains all the common functionalities
# SoilSensor & Sprinkler things will be derived from this class
class Things:

    def __init__(self, id, type, lat, long):
        # Assigning thing level information for each of the things.
        self._id = id
        self._type = type
        self._lat = lat
        self._long = long
        self._rootCAPath = Things._get_conf_data('agri_iot', 'root_cert_path', True, 'Root CA Certificate file')
        self._certificatePath = Things._get_conf_data(id, 'root_ca', True, 'Thing Certificate file')
        self._privateKeyPath = Things._get_conf_data(id, 'pvt_key', True, 'Thing Private Key file')
        self._host = Things._get_conf_data('agri_iot', 'end_point', True, 'End point or host', False)
        self._web_socket = conf.getboolean('agri_iot', 'web_socket', fallback=True)
        self._port = conf.getint('agri_iot', 'port', fallback=443)
        self._iot_mqtt_client = self._create_get_mqtt_conn()
        self._iot_mqtt_client.connect()
        self._provision_thing('')

    @staticmethod
    def _get_conf_data(conf_section, conf_option, hard_stop=False, msg='', file_check=True):
        conf_option_exist, tmp_file_path = get_file_path(conf_section, conf_option)
        if conf_option_exist and file_check:
            check_file_existence(tmp_file_path, hard_stop, msg)
        elif not conf_option_exist and hard_stop:
            print(msg + ' does not exist, hence skipping further process!')
            exit(778)
        return tmp_file_path

    # Instantiating mqtt client and configuring it
    def _create_get_mqtt_conn(self):
        # Port defaults
        if not self._web_socket and not self._port:  # When no port override for non-WebSocket, default to 8883
            port = 8883
        # Init AWSIoTMQTTClient
        iot_mqtt_client = None
        if self._web_socket:
            iot_mqtt_client = AWSIoTMQTTClient(self._id, useWebsocket=True)
            iot_mqtt_client.configureEndpoint(self._host, self._port)
            iot_mqtt_client.configureCredentials(self._rootCAPath)
        else:
            iot_mqtt_client = AWSIoTMQTTClient(self._id)
            iot_mqtt_client.configureEndpoint(self._host, self._port)
            iot_mqtt_client.configureCredentials(self._rootCAPath, self._privateKeyPath, self._certificatePath)

        # AWSIoTMQTTClient connection configuration
        iot_mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
        iot_mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        iot_mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
        iot_mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
        iot_mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec
        # Callback that gets called when the client is online
        # This will be called first for things provisioning
        # iot_mqtt_client.onOnline = self._on_connect
        # Callback that gets called when the client receives a new message
        # iot_mqtt_client.onMessage = self._on_message
        return iot_mqtt_client

    # calling registration method to register the device
    def _provision_thing(self, msg):
        # print('Publishing below provisioning msg to {} topic'.format(self._provisioning_topic))
        # print(msg)
        self._pub_to_topic(self._provisioning_topic, msg)

    # Connect method to subscribe to various topics.
    def _on_connect(self):
        print('{} Thing {} has been connected to AWSIoTMQTTClient'.format(self._type,
                                                                          self._id))
        print('{} Thing {} will send provisioning request to {} topic'.format(self._type,
                                                                              self._id,
                                                                              self._provisioning_topic))
        # Registration message will be created by respective child class, hence passing empty string
        self._provision_thing('')

    # method to process the received messages and publish them on relevant topics
    # provisioning response will be handled by this method other sprinkler request will be handled in derived class.
    # @staticmethod
    # def _on_message(client, userdata, msg):
    #     temp_msg = json.loads(msg.payload.decode())
    #     print('Message received from {} topic.'.format(msg.topic))
    #     print('Message received -  ', temp_msg)
    #     if temp_msg['response'] == 'provision' and temp_msg['status'] == 'success' and temp_msg[id] == self._id:
    #         print('{} of {}-Thing type provisioned!'.format(self._type, self._id))
    #     else:
    #         print('{} of {}-Thing type is not provisioned yet!'.format(self._type, self._id))

    # Publish to the topic
    def _pub_to_topic(self, topic, msg, retain=False):
        print('{} sensor publishing data to {} topic.'.format(self._id, topic))
        self._iot_mqtt_client.publish(topic, json.dumps(msg), 1)

    # Terminating the MQTT broker and stopping the execution
    def terminate(self):
        self._iot_mqtt_client.disconnect()

    # Returns current timestamp
    @staticmethod
    def get_current_timestamp():
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
