from src.parser import MariaDBlogParser, TomcatAccesslogParser, ControllerParser, URLparser, utils, FeatureExtractor
from src.parser.exceptions import *
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
import os
import glob
import datetime
from datetime import timedelta
import re
from collections import OrderedDict

PROJECT_ROOT_PATH: str = os.path.abspath('..')
LOGGER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'logger.yaml')
ACCESSLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'accesslog.yaml')
DBLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'dblog.yaml')
REFERER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'referer.yaml')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class Main:
    def __init__(self):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('main')

    def make_user_action_info_table(self):
        data_dirpath: str = os.path.join(PROJECT_ROOT_PATH, 'outputs', 'sequence')
        info: dict = {'date': [],
                      'access_users_num': [],
                        'membership_join_num': [],
                        'reservation_users_num': [],
                        'action_sequence_len_min': [],
                        'action_sequence_len_max': [],
                        'action_sequence_len_mean': [],
                        'action_sequence_len_median': [],
                        'first_access_location_modest': [],
                        'first_access_location_least': [],
                        'departure_location_modest': [],
                        'departure_location_least': [],
                        'access_location_modest': [],
                        'access_location_least': [],
                        'stay_time_min': [],
                        'stay_time_max': [],
                        'stay_time_mean': [],
                        'stay_time_median': []}

        for idx, filename in enumerate(os.listdir(data_dirpath)):
            filepath: str = os.path.join(data_dirpath, filename)
            extractor = FeatureExtractor.Extractor(path=filepath)
            info['date'].append(extractor.date)
            info['access_users_num'].append(extractor.access_users_num)
            info['membership_join_num'].append(extractor.membership_join_num)
            info['reservation_users_num'].append(extractor.reservation_users_num)
            info['action_sequence_len_min'].append(extractor.action_sequence_len_min)
            info['action_sequence_len_max'].append(extractor.action_sequence_len_max)
            info['action_sequence_len_mean'].append(extractor.action_sequence_len_mean)
            info['action_sequence_len_median'].append(extractor.action_sequence_len_median)
            info['first_access_location_modest'].append(extractor.first_access_location_modest)
            info['first_access_location_least'].append(extractor.first_access_location_least)
            info['departure_location_modest'].append(extractor.departure_location_modest)
            info['departure_location_least'].append(extractor.departure_location_least)
            info['access_location_modest'].append(extractor.access_location_modest)
            info['access_location_least'].append(extractor.access_location_least)
            info['stay_time_min'].append(extractor.stay_time_min)
            info['stay_time_max'].append(extractor.stay_time_max)
            info['stay_time_mean'].append(extractor.stay_time_mean)
            info['stay_time_median'].append(extractor.stay_time_median)

        result = pd.DataFrame(info)
        print(result.info())
        print('')
        print(result.head())
        result.to_csv(os.path.join(PROJECT_ROOT_PATH, 'tests', 'referer2action_info_table.csv'), encoding='euc-kr')


if __name__ == '__main__':
    main = Main()
    main.make_referer2action_info_table()

