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

    def file_size_check(self):
        jan_dir_ls = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\202101*')
        jeb_dir_ls = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\202102*')
        mar_dir_ls = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\202103*')
        apr_dir_ls = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\202104*')
        may_dir_ls = glob.glob('C:\\Users\\skns\\LogCollector\\logcollector_v2.0\\data\\PINX\\202104*')

        for month, dir_ls in enumerate([jan_dir_ls, jeb_dir_ls, mar_dir_ls, apr_dir_ls, may_dir_ls]):
            total_file_size = 0
            print(f'[{month+1}]')
            for dirpath in dir_ls:
                access_log_file_path = glob.glob(os.path.join(dirpath, '*access*'))[0]
                access_log_file_name = os.path.basename(access_log_file_path)
                access_log_file_size = os.path.getsize(access_log_file_path)
                total_file_size += access_log_file_size
                # print({'filename': access_log_file_name, 'filesize': access_log_file_size})
            print(f'total_file_size : {total_file_size * 1e-9:.2f} GB')
            print('')

    def accesslog_parsing(self):
        self.logger.info('access log parsing start')
        accesslog_config: dict = yaml.load(open(ACCESSLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        data_path: str = accesslog_config['accesslog_dirpath']
        save_path: str = accesslog_config['save_dirpath']
        print(accesslog_config['log_regex'])

        for dirname in os.listdir(data_path):
            save_filename: str = f'accesslog_{dirname}.csv'
            # if save_filename in os.listdir(save_path):
            #     self.logger.info(f'already exist the parsed data. pass [{dirname}] file parsing process')
            #     continue

            print(f'{dirname} parsing start')
            try:
                filepath: str = glob.glob(os.path.join(data_path, dirname, '*access*'))[0]
            except IndexError as e:
                self.logger.error(e)
                continue
            parsed_info_ls: list = []
            controller = TomcatAccesslogParser.Controller()

            with open(filepath, 'rb') as f:
                for line_num, text in enumerate(f):
                    encoding_type = controller.get_encoding_type(text)

                    # if encoding_type in ['Windows-1254']:
                    #     continue
                    print(text.decode(encoding_type))
                    try:
                        parsed_info: dict = controller.parsing(text.decode(encoding_type))
                        parsed_info_ls.append(parsed_info)
                    except UnicodeDecodeError as e:
                        self.logger.error(f'{e} :: {dirname} :: [{line_num}, {encoding_type}, {text}]')
                        continue
            pprint(parsed_info_ls)
            df = pd.DataFrame(parsed_info_ls)
            print(df.head())
            print(df.info())
            df.to_csv(os.path.join(save_path, save_filename), encoding='euc-kr')
            print('')


if __name__ == '__main__':
    main = Main()
    main.accesslog_parsing()
    # main.file_size_check()

