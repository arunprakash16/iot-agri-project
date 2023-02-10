import sys
import json

sys.path.append('../../')

from src import conf
from src.setup.aws_service_setup import create_iot_typ_grp_orchestrator, create_dynamodb_tb, create_kinesis_stream


iot_setup_needed = conf.getboolean('iot_metadata', 'setup', fallback=False)

if iot_setup_needed:
    thing_groups = conf.get('iot_metadata', 'thing_grp', fallback='').split(',')
    thing_types = conf.get('iot_metadata', 'thing_type', fallback='').split(',')
    if len(thing_groups) > 0 or len(thing_types) > 0:
        create_iot_typ_grp_orchestrator(thing_groups, thing_types)
    else:
        print('No new thing group or type to be created')

dynamo_db_setup_needed = conf.getboolean('dynamo_db', 'setup', fallback=False)

if dynamo_db_setup_needed:
    tb_to_create = conf.get('dynamo_db', 'tb_name_def_options', fallback='')
    if len(tb_to_create) > 0:
        tb_create_meta_dict = json.loads(tb_to_create)
        try:
            for tb_detail in tb_create_meta_dict['table_details']:
                tb_name = conf.get('dynamo_db', tb_detail['table_name'], raw=True)
                attr_def = conf.get('dynamo_db', tb_detail['attr_def'], raw=True)
                key_schema = conf.get('dynamo_db', tb_detail['key_schema'], raw=True)
                if 'prov_throughput' in tb_detail:
                    prov_throughput = conf.get('dynamo_db', tb_detail['prov_throughput'], raw=True)
                    create_dynamodb_tb(tb_name,
                                       json.loads(attr_def)['attribute'],
                                       json.loads(key_schema)['attribute'],
                                       json.loads(prov_throughput))
                else:
                    create_dynamodb_tb(tb_name,
                                       json.loads(attr_def)['attribute'],
                                       json.loads(key_schema)['attribute'])
        except KeyError as e:
            print('table_details option is required for dynamo table creation using config. Below is the sample \
            with provisioning details')
            print('tb_name_def_options = {"table_details": [{"table_name": "ss_raw_tb", "attr_def": "attr_def", \
            "key_schema": "key_schema", "prov_throughput": "prov_throughput"}]}')
            print('If prov_throughput not provided then default 1,1 will be used')

kinesis_setup_needed = conf.getboolean('kinesis_stream', 'setup', fallback=False)

if kinesis_setup_needed:
    stream = conf.get('kinesis_stream', 'stream', fallback='')
    stream_mode = conf.get('kinesis_stream', 'stream_mode', raw=True, fallback='')
    shard_count = conf.getint('kinesis_stream', 'shard_count', fallback=1)
    if len(stream) > 0 and len(stream_mode) > 0:
        create_kinesis_stream(stream, json.loads(stream_mode), shard_count)

