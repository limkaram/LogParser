from src.parser import MariaDBlogParser, TomcatAccesslogParser
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
import os
import glob
from collections import OrderedDict

PROJECT_ROOT_PATH: str = os.path.abspath('..')
LOGGER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'logger.yaml')
ACCESSLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'accesslog.yaml')
DBLOG_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'dblog.yaml')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

# TODO: logging 적용


class Main:
    def __init__(self):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('main')

    def accesslog_parsing(self):
        fail = []  # temp
        success = []  # temp
        self.logger.info('access log parsing start')
        accesslog_config: dict = yaml.load(open(ACCESSLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        data_path: str = accesslog_config['log_filepath']

        for dirname in os.listdir(data_path):
            print(f'{dirname} parsing start')
            try:
                filepath: str = glob.glob(os.path.join(data_path, dirname, '*access*'))[0]
            except IndexError as e:
                print(f'error :: {e}')
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
            df.to_csv(os.path.join(PROJECT_ROOT_PATH, 'outputs', f'accesslog_{dirname}.csv'))
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


if __name__ == '__main__':
    main = Main()
    # main.dblog_parsing(end_line=380000)
    main.accesslog_parsing()
