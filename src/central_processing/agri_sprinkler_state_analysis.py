import os
import json
import decimal
from datetime import datetime, timedelta, timezone
import base64

import pandas as pd
import requests

import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.types import DYNAMODB_CONTEXT


def fetch_weather_data(lat, long, api_key):
    """
    Returns air temperature and humidity
    :param lat:
    :param long:
    :return:
    sample out of weather api:
    {'coord': {'lon': 103.7744, 'lat': 78.6701}, 'weather': [{'id': 804, 'main': 'Clouds',
    'description': 'overcast clouds', 'icon': '04n'}], 'base': 'stations', 'main': {'temp': 244.9, 'feels_like': 237.9,
    'temp_min': 244.9, 'temp_max': 244.9, 'pressure': 1023, 'humidity': 91, 'sea_level': 1023, 'grnd_level': 916},
    'visibility': 10000, 'wind': {'speed': 5.11, 'deg': 169, 'gust': 10.13}, 'clouds': {'all': 100}, 'dt': 1671204173,
    'sys': {'sunrise': 0, 'sunset': 0}, 'timezone': 25200, 'id': 0, 'name': '', 'cod': 200}
    """
    weather_uri = 'https://api.openweathermap.org/data/2.5/weather?'
    weather_api_uri = weather_uri + 'lat={lat}&lon={lon}&appid={API_key}&units=imperial'\
        .format(lat=lat, lon=long, API_key=api_key)
    weather_resp = requests.get(weather_api_uri).json()
    air_temp = 0
    air_humidity = 0
    if weather_resp['cod'] == 200:
        air_temp = weather_resp['main']['temp']
        air_humidity = weather_resp['main']['humidity']
    return air_temp, air_humidity


def merge_weather_data(tb_data):
    unique_coords = tb_data[['lat', 'long']].drop_duplicates(subset=['lat', 'long'])
    temp_list = list()
    humidity_list = list()
    api_key = os.environ.get('weather_api_key')
    for idx in unique_coords.index:
        air_temp, air_humidity = fetch_weather_data(unique_coords.iloc[idx]['lat'],
                                                    unique_coords.iloc[idx]['long'],
                                                    api_key)
        temp_list.append(air_temp)
        humidity_list.append(air_humidity)
    unique_coords['temperature'] = temp_list
    unique_coords['humidity'] = humidity_list
    out_df = pd.merge(tb_data, unique_coords, on=['lat', 'long'], how='inner')
    return out_df


def process_data(ss_weather_data_df, ss_raw_data):
    # Loads data into pandas dataframe
    ss_raw_data_df = pd.DataFrame.from_dict(ss_raw_data)
    # Joining soil sensor raw data with weather data
    ss_raw_data_prov_df = pd.merge(ss_raw_data_df, ss_weather_data_df, on='device_id', how='inner')
    # Averaging all 4 attributes at sprinkler level and converting to python dictionary
    ss_final_avg = ss_raw_data_prov_df.groupby('mapped_sprinkler_id')[['soil_moisture',
                                                                       'soil_temperature',
                                                                       'temperature',
                                                                       'humidity']].mean().mean(axis=1).to_dict()
    return ss_final_avg


def _publish_msg_to_topic(topic, msg):
    __iot_client = boto3.client('iot-data', endpoint_url=os.environ.get('iot_end_point'))
    __iot_client.publish(topic=topic, qos=1, payload=json.dumps(msg))


def __extract_raw_data(stream_data):
    out_list = list()
    for a_msg in stream_data:
        out_list.append(json.loads(base64.b64decode(a_msg["kinesis"]["data"])))
    return out_list


def __extract_sprinkler_state(sp_data):
    sp_state = dict()
    for a_row in sp_data.index:
        # print(type(a_row), a_row)
        sp_state[sp_data.iloc[a_row]['device_id']] = {'sprinkler_state': sp_data.iloc[a_row]['sprinkler_state'],
                                                      'type': sp_data.iloc[a_row]['type'],
                                                      'lat': sp_data.iloc[a_row]['lat'],
                                                      'long': sp_data.iloc[a_row]['lat']}
    return sp_state


def __extract_latest_details(_temp_data_list):
    df = pd.DataFrame.from_dict(_temp_data_list)
    df["Rank"] = df.groupby("device_id")["timestamp"].rank(method='first', ascending=False)
    return df[df["Rank"] == 1].reset_index(drop=True)


def __db_selective_fetch(tb_handle_obj, key, st_dt, end_dt=None, extract_latest=False):
    if end_dt is None:
        end_dt = st_dt
    tb_resp = tb_handle_obj.scan(FilterExpression=Attr(key).between(st_dt, end_dt))
    if extract_latest:
        return __extract_latest_details(tb_resp['Items'])
    else:
        return tb_resp['Items']


def __db_scan(__client):
    has_more_data = True
    tb_data_extract = list()
    while has_more_data:
        temp_out = __client.scan()
        tb_data_extract.extend(temp_out['Items'])
        if temp_out['Count'] == temp_out['ScannedCount']:
            has_more_data = False
    return __extract_latest_details(tb_data_extract)


def insert_batch_data(tb_obj, tb_data, decimal_update=False):
    if decimal_update:
        # Inhibit Inexact Exceptions
        DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
        # Inhibit Rounded Exceptions
        DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0
    with tb_obj.batch_writer() as batch:
        for data in tb_data:
            if decimal_update:
                for k, v in data.items():
                    if isinstance(v, float):
                        data[k] = decimal.Decimal(v)
            batch.put_item(Item=data)


def lambda_handler(event, context):
    # TODO implement
    __dynamo = boto3.resource('dynamodb')
    base_sprinkler_topic = 'iot_command/agri/sprinkler/zone_'
    cut_off = float(os.environ.get('avg_cut_off'))
    # To fetch sprinkler states
    sp_state_tb = __dynamo.Table('sp_provision_state_tb')
    # To fetch sprinkler mapping and lat & long
    ss_tb = __dynamo.Table('ss_provision_tb')
    # To load weather data into the table by attaching it with soil sensor device id
    ss_weather_tb = __dynamo.Table('ss_weather_tb')
    process_start_time = datetime.now(timezone.utc)
    end_date_range = process_start_time.strftime("%Y-%m-%d %H:%M:%S")
    start_date_range = (process_start_time - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        sp_tb_data = __db_selective_fetch(sp_state_tb, 'timestamp', start_date_range, end_date_range, True)
    except KeyError:
        print('Key Error! Hence switching back to full table scan')
        sp_tb_data = __db_scan(sp_state_tb)
    sp_states = __extract_sprinkler_state(sp_tb_data)
    # To fetch soil sensor co-ords to extract weather details from weather api
    ss_prov_data = __db_scan(ss_tb)
    # To-Do
    # Fetch weather data using SS prov data df and load it back to the dataframe
    updated_data = merge_weather_data(ss_prov_data)
    # Average sensor & weather data at sprinkler level
    sp_avg_data = process_data(updated_data[['device_id', 'mapped_sprinkler_id', 'temperature', 'humidity']],
                               __extract_raw_data(event['Records']))
    # Check against a cut-off threshold e.g. 75
    # If its greater than 75 then turn on the sprinkler else off
    sp_state_data = list()
    for sprinkler_id, avg_value in sp_avg_data.items():
        current_sprinkler_state = sp_states[sprinkler_id]['sprinkler_state']
        publish_data = False
        msg = dict()
        state = current_sprinkler_state
        if avg_value > cut_off:
            if current_sprinkler_state == 'OFF':
                print('{} sprinkler state will be switched to ON state'.format(sprinkler_id))
                publish_data = True
                state = 'ON'
            else:
                print('{} sprinkler state will continued to be in ON state'.format(sprinkler_id))
        else:
            if current_sprinkler_state == 'ON':
                print('{} sprinkler state will be switched to OFF state'.format(sprinkler_id))
                publish_data = True
                state = 'OFF'
            else:
                print('{} sprinkler state will continue to be in OFF state'.format(sprinkler_id))
        msg['device_id'] = sprinkler_id
        msg['timestamp'] = end_date_range
        msg['sprinkler_state'] = state
        if publish_data:
            msg['request'] = 'status_change_command'
            tmp_topic = base_sprinkler_topic + sprinkler_id.split('_')[-1]
            # Send commands to relevant sprinkler topic where state change is needed
            _publish_msg_to_topic(tmp_topic, msg)
        # For each sprinkler generate the state information for loading into sp_provision_state_tb
        msg['type'] = sp_states[sprinkler_id]['type']
        msg['lat'] = sp_states[sprinkler_id]['lat']
        msg['long'] = sp_states[sprinkler_id]['long']
        if 'request' in msg.keys():
            del msg['request']
        sp_state_data.append(msg)
    insert_batch_data(sp_state_tb, sp_state_data)
    # Load weather data (ss - device_id, lat, long, temp, humidity) in ss_weather_tb
    no_of_rows = updated_data.shape[0]
    weather_ts = list()
    for _ in range(no_of_rows):
        weather_ts.append(end_date_range)
    updated_data['timestamp'] = weather_ts
    insert_batch_data(ss_weather_tb, updated_data[['device_id', 'timestamp', 'lat',
                                                   'long', 'temperature', 'humidity']].to_dict('records'), True)
    return {
        'statusCode': 200,
        'body': json.dumps('Completed processing the past 5 minutes data generated from soil sensor \
        and sent commands back to sprinkler!')
    }
