import json
import sys

sys.path.append('../../')
# Imports Boto3 for aws services / client creations
import boto3
# Imports ClientError class to check for the exceptions raised in boto3
from botocore.exceptions import ClientError
# Imports time to facilitate wait / sleep until service / client creation is completed

from src import conf, conf_update, write_file, conf_write

# https://docs.aws.amazon.com/iot/latest/developerguide/iot-provision.html?icmpid=docs_iot_console_connect_many_step1
# https://docs.aws.amazon.com/iot/latest/developerguide/jit-provisioning.html
# https://docs.aws.amazon.com/iot/latest/developerguide/auto-register-device-cert.html
# https://docs.aws.amazon.com/iot/latest/developerguide/iot-provision.html?icmpid=docs_iot_console_connect_many_step2
# https://docs.aws.amazon.com/iot/latest/developerguide/provision-template.html?icmpid=docs_iot_console_connect_many_step3
# https://stackoverflow.com/questions/47202668/creating-aws-iot-things-with-policies-and-certificates-on-the-fly


class ThingRegister:
    __boto3_client = boto3.client('iot')

    def __init__(self, thing_id, thing_type_name):
        self._thing_id = thing_id
        self._thing_type_name = thing_type_name
        self._cert_arn = ''
        self._cert_id = ''
        self._cert_id = ''
        self._cert_pem = ''
        self._pub_key = ''
        self._pvt_key = ''
        self._cert_ownership_token = ''
        self._cert_path = '../../conf/' + self._thing_id + '/root_ca.crt'
        self._pvt_key_path = '../../conf/' + self._thing_id + '/pvt_key.pem'
        self._pub_key_path = '../../conf/' + self._thing_id + '/pub_key.pem'
        self._create_thing()
        self._create_cert()
        self._attach_thing_principal()

    def _create_thing(self):
        thing_create_resp = ThingRegister.__boto3_client.create_thing(thingName=self._thing_id,
                                                                      thingTypeName=self._thing_type_name)
        print(thing_create_resp)

    def _attach_thing_principal(self):
        thing_attach_resp = ThingRegister.__boto3_client.attach_thing_principal(thingName=self._thing_id,
                                                                                principal=self._cert_arn)
        print(thing_attach_resp)

    def _create_cert(self):
        try:
            resp = ThingRegister.__boto3_client.create_keys_and_certificate(setAsActive=True)
            self._cert_id = resp['certificateId']
            self._cert_arn = resp['certificateArn']
            self._cert_pem = resp['certificatePem']
            print(self._cert_pem)
            self._pvt_key = resp['keyPair']['PublicKey']
            self._pub_key = resp['keyPair']['PrivateKey']
            write_file(self._cert_path, self._cert_pem)
            write_file(self._pvt_key_path, self._pvt_key)
            write_file(self._pub_key_path, self._pub_key)
            conf_update(self._thing_id, 'root_ca', self._cert_path)
            conf_update(self._thing_id, 'pvt_key', self._pvt_key_path)
            conf_update(self._thing_id, 'pub_key', self._pub_key_path)
        except ClientError as e:
            print("Error in creating root certificate!")
            print(str(e))


def _list_thing(next_grp='', limit_val=100, recur=True):
    __boto3_client = boto3.client('iot')
    if next_grp:
        if isinstance(next_grp, str):
            thing_list_response = __boto3_client.list_things(nextToken=next_grp,
                                                   maxResults=limit_val)
        else:
            print('Thing name should be string, hence check the argument')
    else:
        thing_list_response = __boto3_client.list_things()

    if 'nextToken' in thing_list_response.keys():
        last_eva = thing_list_response['nextToken']
    else:
        last_eva = None
    return last_eva, thing_list_response['things']


def _update_things_counter_frm_cloud():
    thing_type_name = conf.get('iot_metadata', 'thing_type', fallback='Soil_Sensor,Sprinkler').split(',')
    max_ss = 0
    max_sp = 0
    last_processed_thing_name, thing_list = _list_thing()
    print('Total Thing(s) retrieved: ', len(thing_list))
    print('Last extracted thing name: ', last_processed_thing_name)
    for thing_dict in thing_list:
        if thing_dict['thingTypeName'] == thing_type_name[0]:
            tmp_max_ss = int(thing_dict['thingName'].split('_')[-1])
            if tmp_max_ss > max_ss:
                max_ss = tmp_max_ss
        elif thing_dict['thingTypeName'] == thing_type_name[1]:
            tmp_max_sp = int(thing_dict['thingName'].split('_')[-1])
            if tmp_max_sp > max_sp:
                max_sp = tmp_max_sp

    conf_update('iot_metadata', 'ss_count', str(max_ss))
    conf_update('iot_metadata', 'sp_count', str(max_sp))
    return max_ss, max_sp


def thing_creator(thing_name, thing_type_name, live_counter):
    key_modified = thing_name.split('_')
    if key_modified[-1].isnumeric() and int(key_modified[-1]) > int(live_counter):
        print('New thing creation requested - ', thing_name)
        ThingRegister(thing_name, thing_type_name)
        live_counter = key_modified[-1]
    elif key_modified[-1].isalpha():
        print('Provided thing id does not end with an integer hence appending an int value')
        live_counter += 1
        thing_name = thing_name + '_' + str(live_counter)
        ThingRegister(thing_name, thing_type_name)
    else:
        print('{} Thing already registered.'.format(thing_name))
    return live_counter


def mapping_extractor(mapper_must=False):
    section, option = conf.get('agri_iot', 'thing_map', fallback='Soil_Sensor_Mapping,ss_sp_map').split(',')
    thing_to_register = json.loads(conf.get(section, option, fallback='{}'))
    if thing_to_register:
        print(thing_to_register)
    elif mapper_must:
        print("Thing mapping does not exist!")
        exit(781)
    return thing_to_register


def _thing_cert_creation(thing_mapping_dict):
    sp_live_counter = conf.getint('iot_metadata', 'sp_count', fallback=0)
    ss_live_counter = conf.getint('iot_metadata', 'ss_count', fallback=0)
    thing_type = conf.get('iot_metadata', 'thing_type', fallback='Soil_Sensor,Sprinkler').split(',')
    for k, v in thing_mapping_dict.items():
        sp_live_counter = thing_creator(k, thing_type[1], sp_live_counter)
        conf_update('iot_metadata', 'sp_count', str(sp_live_counter))
        for tmp_ss_id in v:
            ss_live_counter = thing_creator(tmp_ss_id, thing_type[0], ss_live_counter)
        conf_update('iot_metadata', 'ss_count', str(ss_live_counter))
        conf_write()


if __name__ == '__main__':
    max_ss, max_sp = _update_things_counter_frm_cloud()
    # Check whether things have to be created using conf
    create_things_using_conf_mapping = conf.getboolean('agri_iot', 'create_things_using_conf_mapping', fallback=False)
    if create_things_using_conf_mapping:
        temp_thing_map = conf.get('agri_iot', 'thing_map', fallback='')
        if temp_thing_map:
            things_to_register_dict = mapping_extractor(True)
            _thing_cert_creation(things_to_register_dict)
        else:
            print("Configuration for thing registration aren't complete!")
            exit(780)
    else:
        ss_to_create = conf.getint('agri_iot', 'ss_create', fallback=0)
        sp_to_create = conf.getint('agri_iot', 'sp_create', fallback=0)
        if ss_to_create == 0 or sp_to_create == 0:
            print('No new things to create')
        else:
            temp_thing_map = dict()
            ss_key = conf.get('Soil_Sensor', 'id_key', fallback='SS')
            sp_key = conf.get('Sprinkler', 'id_key', fallback='SP')
            if ss_to_create > 1 and ss_to_create % ss_to_create != 0:
                ss_to_create += ss_to_create % ss_to_create
            for _ in range(sp_to_create):
                max_sp += 1
                temp_sp_id = sp_key + '_' + str(max_sp)
                tmp_ss_id_list = list()
                for _ in range(int(ss_to_create / sp_to_create)):
                    max_ss += 1
                    tmp_ss_id_list.append(ss_key + '_' + str(max_ss))
                temp_thing_map[temp_sp_id] = tmp_ss_id_list.copy()
            print(temp_thing_map)
            existing_conf_map = mapping_extractor()
            existing_conf_map.update(temp_thing_map)
            conf_update('Soil_Sensor_Mapping', 'ss_sp_map', json.dumps(existing_conf_map))
            _thing_cert_creation(temp_thing_map)






