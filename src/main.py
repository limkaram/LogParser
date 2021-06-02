from src.parser import MariaDBlogParser, TomcatAccesslogParser, ControllerParser, URLparser, utils, FeatureExtractor
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

# TODO: logging, DailyAccessUserInfo stay_time threshold separated 로직 적용 필요


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

        for filename in os.listdir(files_path):
            if f'ActionSequence_{filename.replace("accesslog_", "")}' in os.listdir(save_path):
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
                first_access_time: str = temp_df['time'].iloc[0]
                referers: list = temp_df['referer'].values
                access_times: list = temp_df['time'].values
                one_ip_actions: list = []
                previous_time: str = first_access_time

                for idx, (access_time, referer) in enumerate(zip(access_times, referers)):
                    action: str = ''
                    parser = URLparser.Parser(referer)

                    try:
                        url_elements_info = parser.url_elements.values

                        for prefix_depth, url_element in url_elements_info:
                            condition1 = (urls_description_df['prefix_depth'] == prefix_depth)
                            condition2 = (urls_description_df['url_element'] == url_element)
                            try:
                                action += urls_description_df['description'].loc[condition1 & condition2].values[
                                              0] + ' '
                            except IndexError:
                                self.logger.error(f'fail : {(prefix_depth, url_element)}')
                                continue

                        if len(one_ip_actions) > 0:
                            stay_sec = parser.get_time_difference(previous_time, access_time)
                            one_ip_actions.append(stay_sec)
                            previous_time = access_time

                        one_ip_actions.append(action.strip())
                    except PrefixURLNotFoundError:
                        if referer in unnecessaries:
                            continue
                        elif referer.startswith('android-app'):
                            continue
                        elif referer.startswith('554fcae493e564ee0dc75bdf2ebf94caads|a'):
                            continue
                        elif referer.startswith('45ea207d7a2b68c49582d2d22adf953aads|a'):
                            continue
                        elif 'safeframe.googlesyndication.com' in referer:
                            continue

                        matched_text = re.match(r'https?:\/\/[ ]?((www\.)?[-a-zA-Z가-힣0-9@:%._\+~#=]{1,256})', referer)

                        if len(one_ip_actions) > 0:
                            stay_sec = parser.get_time_difference(previous_time, access_time)
                            one_ip_actions.append(stay_sec)
                            previous_time = access_time

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
                pd.DataFrame(result).to_csv(os.path.join(save_path, f'ActionSequence_{filename.replace("accesslog_", "")}'), encoding='euc-kr')
            print('')

    def make_user_action_info_table(self):
        data_dirpath: str = os.path.join(PROJECT_ROOT_PATH, 'outputs', 'SeparatedSequence')
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
                      'reservation_rate': [],
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
            info['reservation_rate'].append(extractor.reservation_rate)
            for timezone in range(24):
                info[str(timezone)].append(extractor.get_access_users_num_in_timezone(timezone=timezone))

        result = pd.DataFrame(info)
        result.sort_values(by='date', ascending=True, inplace=True)
        result.reset_index(inplace=True, drop=True)
        result.to_csv(
            os.path.join(PROJECT_ROOT_PATH, 'outputs', 'DailyAccessUserInfo', 'DailyAccessUserInfo_20210601.csv'),
            encoding='euc-kr')
        print(result.info(), '\n')
        print(result.head())

    def merge_csv(self):
        root_path = os.path.join(PROJECT_ROOT_PATH, 'outputs', 'parsed')
        utils.merge_csv(root_path, dst=os.path.join(PROJECT_ROOT_PATH, 'tests', 'parsed_20201209-20210531.csv'))

    def separate_action_sequence_with_stay_time_threshold(self):
        for filepath in glob.glob(os.path.join(PROJECT_ROOT_PATH, 'outputs', 'sequence', '*.csv')):
            filename: str = os.path.basename(filepath)
            self.logger.info(f'{filename} start')
            df: pd.DataFrame = pd.read_csv(filepath, encoding='euc-kr', index_col=0).dropna(axis=0)

            separated_info: dict = {'user_index': [], 'first_access_time': [], 'actions': [], 'total_stay_time': []}
            user_index: int = 0

            for first_access_time, sequence in zip(df['first_access_time'], df['actions']):
                users: dict = SeparateUserWithStaytimeThreshold.Separator(first_access_time,
                                                                          sequence,
                                                                          threshold=120).separated_users_info

                for access_time, actions in users.items():
                    separated_info['user_index'].append(user_index)
                    separated_info['first_access_time'].append(access_time)
                    separated_info['actions'].append(' -> '.join(actions))
                    separated_info['total_stay_time'].append(utils.calculate_stay_time(actions))
                    user_index += 1

            result = pd.DataFrame(separated_info)
            result.to_csv(os.path.join(PROJECT_ROOT_PATH, 'outputs', 'SeparatedSequence',
                                       filename.replace('ActionSequence', 'SeparatedActionSequence')),
                          encoding='euc-kr')
            print(result.info())
            print('')
            print(result.head())


if __name__ == '__main__':
    main = Main()
    # main.accesslog_parsing()
    # main.referer2action()
    # main.separate_action_sequence_with_stay_time_threshold()
    main.merge_csv()
    # main.make_user_action_info_table()