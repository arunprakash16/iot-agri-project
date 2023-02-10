import datetime
from configparser import ConfigParser
import os


relative_config_path = '../../conf/config.cfg'
conf = ConfigParser()


def get_file_path(root_section, option):
    data_exist = False
    temp_file_path = ''
    if conf.has_option(root_section, option):
        data_exist = True
        temp_file_path = conf.get(root_section, option)
    return data_exist, temp_file_path


def check_file_existence(file_path, hard_stop=False, msg_prefix=''):
    file_exist = False
    if os.path.exists(file_path):
        file_exist = True
    elif hard_stop:
        print(msg_prefix + ' does not exist, hence skipping further process!')
        exit(777)
    return file_exist


def conf_file_read():
    if check_file_existence(relative_config_path, True, 'Configuration file'):
        conf.read(relative_config_path)


def write_file(file_name, file_content):
    tmp_dir = file_name.split('/')
    print('/'.join(tmp_dir[:-1]))
    if not check_file_existence('/'.join(tmp_dir[:-1])):
        os.mkdir('/'.join(tmp_dir[:-1]))
    with open(file_name, 'w') as out_file:
        out_file.write(file_content)


def conf_update(section, key, value):
    try:
        conf[section][key] = value
    except KeyError:
        conf.add_section(section)
        conf.set(section, key, value)


def conf_write():
    os.rename(relative_config_path, '../../conf/config' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '.cfg')
    with open(relative_config_path, 'w') as conf_update:
        conf.write(conf_update)
    conf_file_read()

conf_file_read()
