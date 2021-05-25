from src.parser import MariaDBlogParser, TomcatAccesslogParser, ControllerParser, URLparser, utils
from src.parser.exceptions import *
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
import os
import glob
import datetime
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

# TODO: logging


class Main:
    def __init__(self):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('main')

    def accesslog_parsing(self):
        self.logger.info('access log parsing start')
        accesslog_config: dict = yaml.load(open(ACCESSLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        data_path: str = accesslog_config['accesslog_dirpath']
        save_path: str = accesslog_config['save_dirpath']

        for dirname in os.listdir(data_path):
            save_filename: str = f'accesslog_{dirname}.csv'
            if save_filename in os.listdir(save_path):
                self.logger.info(f'already exist the parsed data. pass [{dirname}] file parsing process')
                continue

            print(f'{dirname} parsing start')
            try:
                filepath: str = glob.glob(os.path.join(data_path, dirname, '*access*'))[0]
            except IndexError as e:
                self.logger.error(e)
                continue
            parsed_info: list = []
            controller = TomcatAccesslogParser.Controller()

            with open(filepath, 'rb') as f:
                for line_num, text in enumerate(f):
                    encoding_type = controller.get_encoding_type(text)

                    if encoding_type in ['Windows-1254']:
                        continue

                    try:
                        parsed_text: dict = controller.parsing(text.decode(encoding_type))
                        parsed_info.append(parsed_text)
                    except UnicodeDecodeError as e:
                        self.logger.error(f'{e} :: {dirname} :: [{line_num}, {encoding_type}, {text}]')
                        continue
            df = pd.DataFrame(parsed_info)
            print(df.head())
            print(df.info())
            df.to_csv(os.path.join(save_path, save_filename))
            print('')

    def dblog_parsing(self, end_line):
        self.logger.info('mariaDB log parsing start')
        dblog_config: dict = yaml.load(open(DBLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        data_path: str = dblog_config['log_filepath']
        controller = MariaDBlogParser.Controller()
        result: list = []
        record: OrderedDict = OrderedDict()
        continuous_append_thread_id: str = str()

        for dirname in os.listdir(data_path):
            print(f'{dirname} parsing start')
            filepath: str = glob.glob(os.path.join(data_path, dirname, '*general*'))[0]

            for line_num, text in enumerate(controller.gzip_readline(filepath)):
                try:
                    if line_num == end_line:
                        break

                    if (len(text) == 0) or (line_num < dblog_config['start_headline']):
                        continue

                    time: str = controller.get_time(text)
                    thread_id: str = controller.get_id(text)
                    command: str = controller.get_command(text)

                    if time:
                        if thread_id in record.keys():
                            result.append(record.pop(thread_id))

                        argument = text.replace(time, '').replace(thread_id, '').replace(command, '').strip()
                        record[thread_id] = {'time': time,
                                             'thread_id': thread_id,
                                             'command': command,
                                             'argument': argument}
                    else:
                        if thread_id:
                            if thread_id in record.keys():
                                continuous_append_thread_id = thread_id
                                argument = text.replace(thread_id, '').replace(command, '').strip()
                                record[thread_id]['argument'] += '\n' + argument
                            else:
                                # TODO: 최초 할당받은 스레드 없이 등장하는 라인 있음(대체로 할당 받을시 timestamp가 있음)
                                argument = text.replace(thread_id, '').replace(command, '').strip()
                                record[thread_id] = {'time': '-',
                                                     'thread_id': thread_id,
                                                     'command': command,
                                                     'argument': argument}
                        else:
                            record[continuous_append_thread_id]['argument'] += '\n' + text.strip()
                except KeyError as e:
                    print(f'error line : {line_num} :: {e}')

            for key, value in record.items():
                result.append(value)
            result.sort(key=lambda x: x['time'], reverse=False)
            df = pd.DataFrame(result, columns=['time', 'thread_id', 'command', 'argument'])

            print(df.head())
            print(df.info())
            # controller.df2file(df, os.path.join('..', 'outputs', 'result_20210414.csv'))

    def javacontoller_parsing(self):
        controller = ControllerParser.Controller()
        urls: list = []

        for filename in os.listdir(os.path.join(PROJECT_ROOT_PATH, 'data', 'javacontrollers')):
            if filename.lower().startswith('admin') or filename.lower().startswith('test'):
                continue

            print(f'===========filename : {filename}===========')
            each_file_urls: list = []
            root_url: str = ''

            for line in controller.java_readline(os.path.join(PROJECT_ROOT_PATH, 'data', 'javacontrollers', filename)):
                if '@RequestMapping' in line:
                    if len(root_url) == 0:
                        root_url = controller.get_url(line)
                    elif len(root_url) > 0:
                        sub_url: str = controller.get_url(line)
                        full_url: str = root_url + sub_url
                        each_file_urls.append(full_url)

            urls += each_file_urls

        result: dict = {'prefix_depth': [], 'url_element': []}

        for url in urls:
            url_elements = url.lstrip('/').split('/')
            for prefix_idx, url_element in enumerate(url_elements):
                result['prefix_depth'].append(prefix_idx)
                result['url_element'].append(url_element)

        df = pd.DataFrame(result).drop_duplicates()
        df.to_csv(os.path.join(PROJECT_ROOT_PATH, 'outputs', 'parsed_url_elements',
                               f'urls_{datetime.datetime.now().strftime("%Y%m%d")}.csv'))

    def referer2action(self):
        config: dict = yaml.load(open(REFERER_CONFIG_PATH, encoding='utf-8'), Loader=yaml.FullLoader)
        files_path: str = config['files_dirpath']
        save_path: str = config['save_dirpath']
        description_filepath: str = config['description_filepath']
        unnecessaries: list = config['unnecessaries']
        urls_description_df = pd.read_csv(description_filepath, index_col=0, encoding='euc-kr').reset_index(drop=True)
        print('#### urls_description_df ####')
        print(urls_description_df.head(), '\n')

        # os.path.join(save_path, f'referer2action_{filename.replace("accesslog_", "")}'

        for filename in os.listdir(files_path):
            if f'referer2action_{filename.replace("accesslog_", "")}' in os.listdir(save_path):
                self.logger.info(f'already exist the parsed data. pass [{filename}] file parsing process')
                continue
            self.logger.info(f'{filename} start')
            accesslog_df = pd.read_csv(os.path.join(files_path, filename), index_col=0)
            accesslog_df = accesslog_df[['ip', 'time', 'referer']].dropna(axis=0)
            print('#### accesslog_df info ####')
            print(accesslog_df.info(), '\n')

            result: OrderedDict = OrderedDict()
            result['ip'] = []
            result['first_access_time'] = []
            result['actions'] = []

            for ip in accesslog_df['ip'].unique():
                temp_df = accesslog_df.loc[accesslog_df['ip'] == ip, :]
                temp_df = temp_df.drop_duplicates()
                temp_df = temp_df.sort_values(by='time')
                temp_df = temp_df.drop_duplicates(['referer'])
                first_access_time = temp_df['time'].iloc[0]
                referers: list = temp_df.referer.values
                one_ip_actions: list = []

                for referer in referers:
                    action: str = ''
                    parser = URLparser.Parser(referer)

                    try:
                        for prefix_depth, url_element in parser.url_elements.values:
                            condition1 = (urls_description_df['prefix_depth'] == prefix_depth)
                            condition2 = (urls_description_df['url_element'] == url_element)
                            try:
                                action += urls_description_df['description'].loc[condition1 & condition2].values[
                                              0] + ' '
                            except IndexError:
                                self.logger.error(f'fail : {(prefix_depth, url_element)}')
                                continue
                        one_ip_actions.append(action)
                    except PrefixURLNotFoundError:
                        if referer in unnecessaries:
                            continue
                        elif referer.startswith('android-app'):
                            continue
                        elif referer.startswith('554fcae493e564ee0dc75bdf2ebf94caads|a'):
                            continue
                        elif referer.startswith('45ea207d7a2b68c49582d2d22adf953aads|a'):
                            continue

                        matched_text = re.match(r'https?:\/\/[ ]?((www\.)?[-a-zA-Z가-힣0-9@:%._\+~#=]{1,256})', referer)

                        if matched_text is not None:
                            domain = matched_text.group(1)
                            one_ip_actions.append(domain)
                        else:
                            one_ip_actions.append(referer)
                        continue
                    except TypeError as e:
                        print(f'!ERROR :: {e}')
                        print(f'ip : {ip}')
                        print(f'referer : {referer}')

                result['ip'].append(ip)
                result['first_access_time'].append(first_access_time)
                result['actions'].append(' -> '.join(one_ip_actions))
                pd.DataFrame(result).to_csv(os.path.join(save_path, f'referer2action_{filename.replace("accesslog_", "")}'), encoding='euc-kr')
            print('')

    def merge_csv(self):
        root_path = os.path.join(PROJECT_ROOT_PATH, 'outputs', 'referer2action_dataset')
        utils.merge_csv(root_path)


if __name__ == '__main__':
    main = Main()
    main.accesslog_parsing()
    main.referer2action()
