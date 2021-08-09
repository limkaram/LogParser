from src.parser import MariaDBlogParser
from src.parser import TomcatAccesslogParser
from src.parser import ControllerParser
from src.parser import URLparser
from src.parser import utils
from src.parser import DailyInfoFeatureExtractor
from src.parser import UserInfoFeatureExtractor
from src.parser import SeparateUserWithStaytimeThreshold
from src.parser.exceptions import *
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
import os
import glob
import time
import datetime
from datetime import timedelta
import re
from collections import OrderedDict

PROJECT_ROOT_PATH: str = os.path.abspath('..')
LOGGER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'logger.yaml')
ACCESSLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'accesslog.yaml')
DBLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'dblog.yaml')
REFERER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'userinfo.yaml')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class Main:
    def __init__(self):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('main')

    def accesslog_parsing(self):
        filepath = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\20210629\\*access*')[0]
        # filepath: str = glob.glob(os.path.join(PROJECT_ROOT_PATH, 'tests', '*test_utf8*'))[0]
        parsed_info_ls: list = []
        controller = TomcatAccesslogParser.Controller()

        # s_t = time.time()
        # with open(filepath) as f:
        #     for line_num, text in enumerate(f):
        #         try:
        #             parsed_info: dict = controller.parsing(text)
        #             parsed_info_ls.append(parsed_info)
        #         except UnicodeDecodeError as e:
        #             self.logger.error(
        #                 f'{e} :: [line_num : {line_num} / text : {text}]')
        #         except AttributeError as e:
        #             self.logger.error(
        #                 f'{e} :: [line_num : {line_num} / text : {text}]')
        # e_t = time.time()
        # total_t1 = e_t - s_t

        s_t = time.time()
        with open(filepath, 'rb') as f:
            for line_num, text in enumerate(f):
                encoding_type = controller.get_encoding_type(text)

                try:
                    parsed_info: dict = controller.parsing(text.decode(encoding_type))
                    parsed_info_ls.append(parsed_info)
                except UnicodeDecodeError as e:
                    self.logger.error(
                        f'{e} :: [line_num : {line_num} / encoding : {encoding_type} / text : {text}]')
                except AttributeError as e:
                    self.logger.error(
                        f'{e} :: [line_num : {line_num} / encoding : {encoding_type} / text : {text}]')
        e_t = time.time()
        total_t2 = e_t - s_t
        print(f'line check : {total_t2}')

        # df = pd.DataFrame(parsed_info_ls)
        # print(df.head())
        # print('')
        # print(df.info())
        # print('')

    def temp(self):
        columns_name = {'host': 'ip',
                         'firstAccessTime': 'first_access_time',
                         'totalResponseTime': 'total_request_processing_time',
                         'totalLength': 'total_transfer_bytes',
                         'agent': 'user_agent',
                         'os': 'os',
                         'osVersion': 'os_version',
                         'browser': 'browser',
                         'browserVersion': 'browser_version',
                         'device': 'device',
                         'deviceBrand': 'device_brand',
                         'deviceModel': 'device_model',
                         'isMobile': 'is_mobile',
                         'isBot': 'is_bot',
                         'totalStayTime': 'total_stay_time',
                         'reservation': 'reservation',
                         'cancel': 'cancel',
                         'actionSequence': 'actions'}

        df = pd.read_csv(r'C:\Users\skns\PycharmProjects\logparser\tests\SpecificUserInfo_20210201-20210630.csv')
        df = df.drop(columns=[i for i in df.columns if i.startswith('Unnamed')])
        df = df.rename(columns=columns_name)
        df.to_csv(r'C:\Users\skns\PycharmProjects\logparser\tests\SpecificUserInfo_20210201-20210630.csv', index=False)
        df = pd.read_csv(r'C:\Users\skns\PycharmProjects\logparser\tests\SpecificUserInfo_20210201-20210630.csv')
        print(df.info())
        print('')
        print(df.head())


if __name__ == '__main__':
    main = Main()
    # main.accesslog_parsing()
    main.temp()
    # main.file_size_check()

