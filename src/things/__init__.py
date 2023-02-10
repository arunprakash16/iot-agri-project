import logging
import random
import multiprocessing as mp
import sys
import time

sys.path.append('../../')

from src import conf
from src.things.SoilSensor import SoilSensor
from src.things.Sprinkler import sprinkler_process
from src.things.ThingsSetup import mapping_extractor

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
# Things object holder
ss_things_obj_list = list()


def process_status_check(process_list):
    for a_process in process_list:
        a_process.wait()


if __name__ == '__main__':
    print("Number of processors: ", mp.cpu_count())
    ss_pub_topic = conf.get('agri_iot', 'sensor_topic')
    sp_sub_topic = conf.get('agri_iot', 'sprinkler_topic')
    ss_count = conf.getint('iot_metadata', 'ss_count')
    sp_count = conf.getint('iot_metadata', 'sp_count')
    ss_sp_map = mapping_extractor()
    ss_prov_topic = conf.get('agri_iot', 'ss_provisioning_topic')
    sp_prov_topic = conf.get('agri_iot', 'sp_provisioning_topic')

    for sp, ss_list in ss_sp_map.items():
        sp_lat = random.uniform(-90, 90)
        sp_long = random.uniform(-180, 180)
        # temp_sp = Sprinkler(sp, sp_lat, sp_long, sp_sub_topic, sp_prov_topic)
        sprinkler_process(sp, sp_lat, sp_long, sp_sub_topic, sp_prov_topic)
        # sp_things_obj_list.append(temp_sp)
        for ss in ss_list:
            print('Initializing {} soil_sensor and mapping to {} sprinkler.'.format(ss, sp))
            temp_ss = SoilSensor(ss,
                                 random.uniform(sp_lat - 20, sp_lat + 20),
                                 random.uniform(sp_long - 30, sp_long + 30),
                                 sp,
                                 ss_pub_topic,
                                 ss_prov_topic)
            ss_things_obj_list.append(temp_ss)
            # temp_ss.publish_scheduler()

    # temp_ss = SoilSensor('SS_1',
    #                      random.uniform(-90, 90),
    #                      random.uniform(-180, 180),
    #                      'SP_' + str(random.randint(1, 1)),
    #                      ss_pub_topic,
    #                      sp_prov_topic)
    # things_obj_list.append(temp_ss)
    # Running all the things thru multiprocessing
    mp_pool = mp.Pool(ss_count + sp_count)
    loop_cont = True
    # trigger_batch = True
    while loop_cont:
        process_list = list()
        try:
            now = time.time()
            for ss_obj in ss_things_obj_list:
                result = mp_pool.apply_async(ss_obj.publish_scheduler(now + 30))
                process_list.append(result)
            # process_status_check(process_list)
            # for sp_obj in sp_things_obj_list:
            #     mp_pool.apply_async(sp_obj.subscribe_data_check)
            time.sleep(20)
        except KeyboardInterrupt:
            process_status_check(process_list)
            mp_pool.join()
            loop_cont = False
