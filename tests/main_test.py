from src.parser import MariaDBlogParser, TomcatAccesslogParser
from src.user_agents import parse
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
import os
import glob
import math
from collections import OrderedDict, deque

DATA_PATH = os.path.join('..', 'data', 'general.log-20210328.gz')
LOGGER_CONFIG_PATH = os.path.join('..', 'conf', 'logger.yaml')
DBLOG_CONFIG_PATH = os.path.join('..', 'conf', 'dblog.yaml')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)


# TODO: 리팩토링, controller 객체지향화, logging 적용, Pandas DataFrame화, Pyspark 활용 DB화

class Main:
    def __init__(self):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('main')

    def accesslog_parsing(self):
        self.logger.info('access log parsing start')
        ROOT_PATH = os.path.abspath('..')
        dirs = os.listdir(os.path.join(ROOT_PATH, 'data'))
        ip_count: OrderedDict = OrderedDict()

        for dirname in dirs:
            print(f'{dirname} start')
            filepath: str = glob.glob(os.path.join(ROOT_PATH, 'data', dirname, '*.log'))[0]
            parsed_info: list = []
            controller = TomcatAccesslogParser.Controller()

            try:
                # TODO : 특정 accesslog 파일 decoding utf-8, cp949, utf-16, euc-kr 모두 안되는 문제 해결 필요
                for line_num, text in enumerate(controller.readline(filepath)):
                    try:
                        parsed_info.append(controller.parsing(text))
                    except AttributeError as e:
                        print(f'{line_num+1}, {text} :: {e}')
                        break
            except UnicodeDecodeError as e:
                print(e, '\n')
                continue

            df = pd.DataFrame(parsed_info)
            ip_count[dirname] = len(df['ip'].unique())

            # user agent parser module test
            for idx, ua_text in enumerate(df['user_agent'].values):
                if idx == 100:
                    break
                print(ua_text)
                a = parse(ua_text)
                print(f'raw : {a}')
                print(f'os : {a.os}')
                print(f'device : {a.device}')
                print(f'ua_string : {a.ua_string}')
                print(f'browser : {a.browser}')
                print('')

        print('Consider only the number of unique IP address')
        mean = sum(ip_count.values()) / len(ip_count)
        standard_deviation = math.sqrt(sum([(i - mean) ** 2 for i in ip_count.values()]) / len(ip_count))
        print(f'mean : {mean}')
        print(f'std deviation : {standard_deviation}\n')

    def dblog_parsing(self, end_line):
        self.logger.info('mariaDB log parsing start')
        controller = MariaDBlogParser.Controller()
        dblog_config: dict = yaml.load(open(DBLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        result: list = []
        record: OrderedDict = OrderedDict()
        continuous_append_thread_id: str = str()

        for line_num, text in enumerate(controller.gzip_readline(DATA_PATH)):
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
        print(df.head(50))
        print(df.info())
        pprint(df[df['time'] == '-'].values)
        # controller.df2file(df, os.path.join('..', 'outputs', 'result_20210414.csv'))


if __name__ == '__main__':
    main = Main()
    # main.dblog_parsing(end_line=380000)
    main.accesslog_parsing()
