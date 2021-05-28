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
REFERER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'sequence.yaml')

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
                      'stay_time_median': [],
                      '0': [],
                      '1': [],
                      '2': [],
                      '3': [],
                      '4': [],
                      '5': [],
                      '6': [],
                      '7': [],
                      '8': [],
                      '9': [],
                      '10': [],
                      '11': [],
                      '12': [],
                      '13': [],
                      '14': [],
                      '15': [],
                      '16': [],
                      '17': [],
                      '18': [],
                      '19': [],
                      '20': [],
                      '21': [],
                      '22': [],
                      '23': []}

        for filepath in glob.glob(os.path.join(data_dirpath, '*.csv')):
            filename: str = os.path.basename(filepath)
            self.logger.info(f'{filename} start')
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
            for timezone in range(24):
                info[str(timezone)].append(extractor.get_access_users_num_in_timezone(timezone=timezone))

        result = pd.DataFrame(info)
        result.sort_values(by='date', ascending=True, inplace=True)
        result.reset_index(inplace=True, drop=True)
        result.to_csv(os.path.join(PROJECT_ROOT_PATH, 'outputs', 'DailyAccessUserInfo', 'DailyAccessUserInfo_20210528.csv'), encoding='euc-kr')
        print(result.info(), '\n')
        print(result.head())


if __name__ == '__main__':
    main = Main()
    main.make_user_action_info_table()

