from datetime import datetime, timedelta, timezone

import pandas as pd
import matplotlib.pyplot as plt

import boto3
from boto3.dynamodb.conditions import Attr


html_template_file = '../../templates/farm_water_analysis_template.html'
html_out_file = '../../templates/farm_water_analysis.html'
bar_chart_file = '../../templates/farm_water_usage_plot.png'

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
        # print(len(tb_resp['Items']))
        return tb_resp['Items']


def __db_scan(__client):
    has_more_data = True
    tb_data_extract = list()
    while has_more_data:
        temp_out = __client.scan()
        tb_data_extract.extend(temp_out['Items'])
        if temp_out['Count'] == temp_out['ScannedCount']:
            has_more_data = False
    return tb_data_extract


def __sp_data_analysis(data):
    grouped_data_list = list()
    process_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = pd.DataFrame.from_dict(data).sort_values(by=["timestamp"], ascending=True).reset_index()
    df = df[['device_id', 'sprinkler_state', 'lat', 'long', 'timestamp']]
    temp_sub_grp_dict = dict()
    for idx in df.index:
        _id = df.iloc[idx]['device_id']
        temp_sub_grp_val = temp_sub_grp_dict.get(_id, {})
        if len(temp_sub_grp_val) == 0 and df.iloc[idx]['sprinkler_state'] != 'OFF':
            temp_sub_grp_val = {'device_id': _id,
                                'start_time': df.iloc[idx]['timestamp'],
                                'prev_state': df.iloc[idx]['sprinkler_state'],
                                'lat': df.iloc[idx]['lat'],
                                'long': df.iloc[idx]['long'],
                                'end_time': '',
                                'current_state': '',
                                'sp_on_duration': 0.0}
            temp_sub_grp_dict[_id] = temp_sub_grp_val
            # print(_id, df.iloc[idx]['lat'], df.iloc[idx]['long'])
        elif len(temp_sub_grp_val) != 0 and temp_sub_grp_val['prev_state'] != df.iloc[idx]['sprinkler_state']:
            temp_sub_grp_val['end_time'] = df.iloc[idx]['timestamp']
            temp_sub_grp_val['current_state'] = df.iloc[idx]['sprinkler_state']
            time_diff = datetime.strptime(temp_sub_grp_val['end_time'], '%Y-%m-%d %H:%M:%S') - \
                        datetime.strptime(temp_sub_grp_val['start_time'], '%Y-%m-%d %H:%M:%S')
            temp_sub_grp_val['sp_on_duration'] = time_diff.total_seconds()
            grouped_data_list.append(temp_sub_grp_val)
            del temp_sub_grp_dict[_id]

    # Setting up current batch time as end time for the ones for which state change has not been triggered
    for k in temp_sub_grp_dict:
        temp_sub_grp_dict[k]['end_time'] = process_start_time
        temp_sub_grp_dict[k]['current_state'] = 'OFF'
        time_diff = datetime.strptime(temp_sub_grp_dict[k]['end_time'], '%Y-%m-%d %H:%M:%S') - \
                    datetime.strptime(temp_sub_grp_dict[k]['start_time'], '%Y-%m-%d %H:%M:%S')
        temp_sub_grp_dict[k]['sp_on_duration'] = time_diff.total_seconds()
        grouped_data_list.append(temp_sub_grp_dict[k])
    return pd.DataFrame.from_dict(grouped_data_list)


def __process_data(sp_analysis_data):
    html_data = ''

    min_data = sp_analysis_data[['device_id', 'lat', 'long', 'sp_on_duration']][sp_analysis_data.sp_on_duration ==
                                                                                sp_analysis_data.sp_on_duration.min()]
    max_data = sp_analysis_data[['device_id', 'lat', 'long', 'sp_on_duration']][sp_analysis_data.sp_on_duration ==
                                                                                sp_analysis_data.sp_on_duration.max()]

    low_cords = '{} latitude, {} longitude'.format(min_data.iloc[0]['lat'], min_data.iloc[0]['long'])
    low_sp = '{} sprinkler'.format(min_data.iloc[0]['device_id'])
    low_sec = str(min_data.iloc[0]['sp_on_duration'])
    high_cords = '{} latitude, {} longitude'.format(max_data.iloc[0]['lat'], max_data.iloc[0]['long'])
    high_sp = '{} sprinkler'.format(max_data.iloc[0]['device_id'])
    high_sec = str(max_data.iloc[0]['sp_on_duration'])
    with open(html_template_file, 'r') as in_html:
        html_data = in_html.read().replace('{low_cords}', '{} latitude, {} longitude'.format(min_data.iloc[0]['lat'],
                                                                                             min_data.iloc[0]['long']))\
            .replace('{low_sp}','{} sprinkler'.format(min_data.iloc[0]['device_id']))\
            .replace('{low_sec}', str(min_data.iloc[0]['sp_on_duration']))\
            .replace('{high_cords}', '{} latitude, {} longitude'.format(max_data.iloc[0]['lat'],
                                                                        max_data.iloc[0]['long']))\
            .replace('{high_sp}', '{} sprinkler'.format(max_data.iloc[0]['device_id']))\
            .replace('{high_sec}', str(max_data.iloc[0]['sp_on_duration']))

    print('Low water consumption area is -> {} latitude, {} longitude.'.format(min_data.iloc[0]['lat'],
                                                                               min_data.iloc[0]['long']))
    print('Associated sprinkler is {} and it was on for {} seconds'.format(min_data.iloc[0]['device_id'],
                                                                           min_data.iloc[0]['sp_on_duration']))
    print('High water consumption area is -> {} latitude, {} longitude.'.format(max_data.iloc[0]['lat'],
                                                                                max_data.iloc[0]['long']))
    print('Associated sprinkler is {} and it was on for {} seconds'.format(max_data.iloc[0]['device_id'],
                                                                           max_data.iloc[0]['sp_on_duration']))

    # print(sp_analysis_data)
    # print(sp_analysis_data.sort_values(by=['sp_on_duration']))
    # print(sp_analysis_data[['device_id', 'lat', 'long']])
    summarized_sp_analysis_data = sp_analysis_data[['device_id', 'lat', 'long', 'sp_on_duration']]\
        .groupby(['device_id', 'lat', 'long'])['sp_on_duration']\
        .sum()\
        .to_frame()\
        .sort_values(by=['sp_on_duration'])\
        .reset_index()
    # print(summarized_sp_analysis_data)
    # print(summarized_sp_analysis_data[['device_id', 'lat', 'long', 'total_sp_on_duration']])
    lat_long_list = summarized_sp_analysis_data[['device_id', 'lat', 'long']]
    on_duration_list = summarized_sp_analysis_data['sp_on_duration']
    x_chart_size = len(lat_long_list)
    # setting figure size by using figure() function
    plt.figure(figsize=(x_chart_size + 10, x_chart_size + 10))
    plt.bar(['{}:{},\n{}'.format(_id, round(lat, 4), round(long, 4))
             for _id, lat, long in lat_long_list.values.tolist()],
            [str(dur_sec)
             for dur_sec in on_duration_list.values.tolist()])
    plt.xlabel("Device_id - Latitude, Longitude")
    plt.ylabel("Water usage in seconds")
    # plt.show()
    # plt.savefig('../../templates/farm_water_usage_plot.png')
    plt.savefig(bar_chart_file)
    with open(html_out_file, 'w') as html_out:
        html_out.write(html_data)


def __upload_files_to_s3(bucket_name='your-farm-stat'):
    # base_path = 'your_farm_analysis_' + datetime.now(timezone.utc).strftime("%Y-%m-%d_%H:%M:%S")
    index_html = 'index.html'
    __s3_conn = boto3.resource('s3')
    __s3_conn.meta.client.upload_file(html_out_file, bucket_name, index_html)
    __s3_conn.meta.client.upload_file(bar_chart_file, bucket_name, 'farm_water_usage_plot.png')
    # bucket_website = __s3_conn.BucketWebsite(bucket_name)
    # bucket_website.put(WebsiteConfiguration={'ErrorDocument': {'Key': 'index_template.html'},
    #                                          'IndexDocument': {'Suffix': index_html}})
    # bucket_website.load()


def irrigation_analysis():
    # TODO implement
    __dynamo = boto3.resource('dynamodb')
    # To fetch sprinkler states
    sp_state_tb = __dynamo.Table('sp_provision_state_tb')
    # To fetch soil sensor sprinkler mapping
    ss_prov_tb = __dynamo.Table('ss_provision_tb')
    # To fetch weather data
    ss_weather_tb = __dynamo.Table('ss_weather_tb')
    process_start_time = datetime.now(timezone.utc)
    end_date_range = process_start_time.strftime("%Y-%m-%d %H:%M:%S")
    start_date_range = (process_start_time - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    # end_date_range = '2023-01-02 15:23:34'
    # print(start_date_range, end_date_range)
    try:
        sp_tb_data = __db_selective_fetch(sp_state_tb, 'timestamp', start_date_range, end_date_range)
    except KeyError:
        print('Key Error! Hence switching back to full table scan')
        sp_tb_data = __db_scan(sp_state_tb)

    try:
        weather_tb_data = __db_selective_fetch(ss_weather_tb, 'timestamp', start_date_range, end_date_range)
    except KeyError:
        print('Key Error! Hence switching back to full table scan')
        weather_tb_data = __db_scan(ss_weather_tb)

    ss_tb_data = __extract_latest_details(__db_scan(ss_prov_tb))
    # print(sp_tb_data)
    # print(weather_tb_data)
    sp_analysis_data = __sp_data_analysis(sp_tb_data)
    __process_data(sp_analysis_data)
    __upload_files_to_s3()


if __name__ == '__main__':
    irrigation_analysis()
