# Imports Boto3 for aws services / client creations
import boto3
# Imports ClientError class to check for the exceptions raised in boto3
from botocore.exceptions import ClientError
# Imports time to facilitate wait / sleep until service / client creation is completed
import time

iot = 'iot'
dynamo = 'dynamodb'
kinesis = 'kinesis'
iam = 'iam'


def grp_thing_desc(client, thing_grp_name):
    """
    Describes the thing group and returns the status
    :param client: IoT client object
    :param thing_grp_name: thingGroupName
    :return: String / None, Dict
    """
    status = None
    try:
        resp = client.describe_thing_group(thingGroupName=thing_grp_name)
        status = resp['status']
    except Exception as e:
        print(e)

    return status, resp


def thing_typ_desc(client, thing_type_name):
    """
    Describes the thing type and returns the status
    :param client: IoT client object
    :param thing_type_name: thingTypeName
    :return: String / None, Dict
    """
    status = None
    try:
        resp = client.describe_thing_type(thingTypeName=thing_type_name)
        status = resp['thingTypeMetadata']['deprecated']
    except Exception as e:
        print(e)

    return status, resp


def check_status(client, name, fun_name):
    not_created = True
    counter = 0
    while not_created:
        time.sleep(0.2)
        state, resp = fun_name(client, name)
        if state == 'ACTIVE' or counter > 10:
            not_created = False
        counter += 1
    return resp


def _create_thing_grp(iot_client, thing_grp):
    """
    Creates thing group and waits until the creation process complete or for the timeout
    :param thing_grp: thingGroupName. Required.
    :return: None
    """
    if thing_grp:
        try:
            thing_grp_response = iot_client.create_thing_group(thingGroupName=thing_grp)
            print('Requested thing group creation in progress')
            resp = check_status(iot_client, thing_grp, grp_thing_desc)
            if resp:
                thing_grp_name = thing_grp_response['thingGroupName']
                # arn - Amazon Resource Name
                thing_grp_arn = thing_grp_response['thingGroupArn']
                print('Thing group {} has been created and {} is the arn'.format(thing_grp_name, thing_grp_arn))
            else:
                print('Thing group creation may not be successful')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print('Requested {} IoT Thing group already exist.'.format(thing_grp))
                print('Status of {}: {}'.format(thing_grp, grp_thing_desc(iot_client, thing_grp)))
            else:
                print(e.response['Error']['Code'])
        except Exception as e:
            print('Exception: ', e)
            thing_grp_status = grp_thing_desc(iot_client, thing_grp)
            # print(thing_grp_status)
    else:
        print('Provide valid thing group name')


def _create_thing_typ(iot_client, thing_type):
    """
    Creates thing type and waits until the creation process completes or for the timeout
    :param thing_type: thingTypeName
    :return: None
    """
    if thing_type:
        try:
            thing_type_response = iot_client.create_thing_type(thingTypeName=thing_type)
            print('Requested thing type creation in progress')
            resp = check_status(iot_client, thing_type, thing_typ_desc)
            if resp:
                thing_type_name = thing_type_response['thingTypeName']
                # arn - Amazon Resource Name
                thing_type_arn = thing_type_response['thingTypeArn']
                print('Thing type {} has been created and {} is the arn'.format(thing_type_name, thing_type_arn))
            else:
                print('Thing type creation may not be successful')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print('Requested {} IoT Thing type already exist.'.format(thing_type))
                print('Status of {}: {}'.format(thing_type, thing_typ_desc(iot_client, thing_type)))
            else:
                print(e.response['Error']['Code'])
        except Exception as e:
            print('Exception: ', e)
            thing_grp_status = thing_typ_desc(iot_client, thing_type)
            # print(thing_grp_status)
    else:
        print('Provide valid thing group name')


def _list_thing(client, next_grp='', limit_val=100, recur=True):
    if next_grp:
        if isinstance(next_grp, str):
            thing_list_response = client.list_things(nextToken=next_grp,
                                                   maxResults=limit_val)
        else:
            print('Thing name should be string, hence check the argument')
    else:
        thing_list_response = client.list_things()

    if 'nextToken' in thing_list_response.keys():
        last_eva = thing_list_response['nextToken']
    else:
        last_eva = None
    return last_eva, thing_list_response['things']


def create_iot_typ_grp_orchestrator(thing_group_names, thing_types):
    iot_client = boto3.client(iot)
    for thing_group_name in thing_group_names:
        _create_thing_grp(iot_client, thing_group_name)
    for thing_type in thing_types:
        _create_thing_typ(iot_client, thing_type)


def create_dynamodb_tb(tb_name, attr_def, key_schema, provisioning=''):
    """
    Creates tables in dynamo db using boto3 module
    :param tb_name: TableName to be created (argument for create_table). Required
    :param attr_def: Basic AttributeDefinitions of the table (argument for create_table). Required
    :param key_schema: KeySchema - partition & sort key details of the table (argument for create_table). Required
    :param provisioning: ProvisionedThroughput - capacity details of the table (argument for create_table). Optional
    :return:None
    """
    if tb_name and attr_def and key_schema:
        dynamodb = boto3.resource(dynamo)
        try:
            if provisioning:
                table = dynamodb.create_table(TableName=tb_name,
                                              AttributeDefinitions=attr_def,
                                              KeySchema=key_schema,
                                              ProvisionedThroughput=provisioning)
            else:
                provisioning = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
                table = dynamodb.create_table(TableName=tb_name,
                                              AttributeDefinitions=attr_def,
                                              KeySchema=key_schema,
                                              ProvisionedThroughput=provisioning)
            # Wait until the table exists.
            table.wait_until_exists()
            # Print out some data about the table.
            print('{} created and has {} records'.format(tb_name, table.item_count))
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print('Requested {} dynamodb table already exist.'.format(tb_name))
            else:
                print(e.response['Error']['Code'])
        except Exception as e:
            print('Exception: ', e)
    else:
        print('Not all mandatory params provided, hence check')


def _list_tables(db_client, tb_name='', limit_val=100):
    """
    List the tables associated to the current account
    :param tb_name: Table name from which the extraction to start with, optional.
    :param limit_val: Number of tables to be listed, optional
    :return: str (Last table name retrieved), list(tables)
    """
    if tb_name:
        if isinstance(tb_name, str):
            tb_list_response = db_client.list_tables(ExclusiveStartTableName=tb_name,
                                                    Limit=limit_val)
        else:
            print('Table name should be string, hence check the argument')
    else:
        tb_list_response = db_client.list_tables()

    if 'LastEvaluatedTableName' in tb_list_response.keys():
        last_eva =  tb_list_response['LastEvaluatedTableName']
    else:
        last_eva = None
    return last_eva, tb_list_response['TableNames']


def stream_create_check(stream, st_name, counter):
    """
    Checks whether provide stream is active or not
    :param stream: kinesis client object
    :param st_name: StreamName
    :param counter: For debugging print statement
    :return: Bool True / False
    """
    stream_created = False
    stream_status = stream.describe_stream(StreamName=st_name)
    if counter == 0:
        print(stream_status)
    if stream_status['StreamDescription']['StreamStatus'] == 'ACTIVE':
        print(stream_status)
        print('{} stream has been created with following details:'.format(stream_status['StreamDescription']['StreamName']))
        print('StreamARN: ', stream_status['StreamDescription']['StreamARN'])
        print('StreamModeDetails: ', stream_status['StreamDescription']['StreamModeDetails']['StreamMode'])
        # print('ShardId: ', stream_status['StreamDescription']['Shards']['ShardId'])
        print('StreamCreationTimestamp: ', stream_status['StreamDescription']['StreamCreationTimestamp'])
        print('EncryptionType: ', stream_status['StreamDescription']['EncryptionType'])
        stream_created = True
    return stream_created


def _list_streams(st_client, st_name='', limit_val=100):
    """
    List the tables associated to the current account
    :param st_name: Kinesis stream name from which the extraction to start with, optional.
    :param limit_val: Number of tables to be listed, optional
    :return: bool (Has more streams), list(streams)
    """
    if st_name:
        if isinstance(st_name, str):
            st_list_response = st_client.list_streams(ExclusiveStartStreamName=st_name,
                                                      Limit=limit_val)
        else:
            print('Stream name should be string, hence check the argument')
    else:
        st_list_response = st_client.list_streams()

    return st_list_response['HasMoreStreams'], st_list_response['StreamNames']


def create_kinesis_stream(stream_name, stream_mode, shard_cnt=1):
    """
    Creates data stream in kinesis service using boto3 module
    :param stream_name: StreamName (argument for create_stream). Required.
    :param stream_mode: StreamModeDetails (argument for create_stream). Required.
    :param shard_cnt: ShardCount (argument for create_stream). Defaults to 1
    :return: None
    """
    if stream_name and stream_mode:
        kinesis_st = boto3.client(kinesis)
        try:
            stream = kinesis_st.create_stream(StreamName=stream_name, ShardCount=shard_cnt, StreamModeDetails=stream_mode)
            stream_not_created = True
            counter = 0
            while stream_not_created:
                counter += 1
                time.sleep(0.2)
                if stream_create_check(kinesis_st, stream_name, counter) or counter >= 10:
                    stream_not_created = False

            if counter >= 10:
                print('Stream creation status check timeout, post {} attempts'.format(counter))
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print('Requested {} kinesis stream already exist.'.format(stream_name))
            else:
                print(e.response['Error']['Code'])
        except Exception as e:
            print('Exception: ', e)
    else:
        print('Not all mandatory params provided, hence check')


def _delete_tb(db_client, tb_name):
    # __tb = db_client.Table(tb_name)
    _tb_delete_response = db_client.delete_table(TableName=tb_name)


def _delete_st(st_client, st_name, consumer_del=True):
    _st_delete_response = st_client.delete_stream(StreamName=st_name,
                                                  EnforceConsumerDeletion=consumer_del)


def _delete_thing(iot_client, thing_name):
    _thing_delete_response = iot_client.delete_thing(thingName=thing_name)


def clean_up_orchestrator():
    dynamodb = boto3.client(dynamo)
    st_client = boto3.client(kinesis)
    iot_client = boto3.client(iot)

    last_processed_tb_name, tb_list = _list_tables(dynamodb)
    print('Total tables retrieved: ', len(tb_list))
    print('Last extracted table name: ', last_processed_tb_name)
    # print('Table names: ', ', '.join(tb_list))
    print('Table names: ')
    for tb_name in tb_list:
        print('Deleting: ', tb_name)
        _delete_tb(dynamodb, tb_name)

    more_streams, st_list = _list_streams(st_client)
    print('Total Streams retrieved: ', len(st_list))
    print('Has more streams to be listed: ', more_streams)
    # print('Stream names: ', ', '.join(st_list))
    print('Stream names: ')
    for st_name in st_list:
        print('Deleting: ', st_name)
        _delete_st(st_client, st_name)

    last_processed_thing_name, thing_list = _list_thing(iot_client)
    print('Total Thing(s) retrieved: ', len(thing_list))
    print('Last extracted thing name: ', last_processed_thing_name)
    # print('Thing(s) name: ', ', '.join(thing_list))
    print('Thing(s) name: ')
    for thing_dict in thing_list:
        try:
            print('Deleting: ', thing_dict['thingName'])
            _delete_thing(iot_client, thing_dict['thingName'])
        except Exception as e:
            print('{} deletion is unsuccessful.'.format(thing_dict['thingName']))
