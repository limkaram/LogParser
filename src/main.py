from src.package import controller
import os
from pprint import pprint
import logging
import logging.config
import pandas as pd
import yaml
from collections import defaultdict

DATA_PATH = os.path.join('..', 'data', 'general.log-20210328.gz')
LOGGER_CONFIG_PATH = os.path.join('..', 'conf', 'logger.yaml')
GLOBAL_CONFIG_PATH = os.path.join('..', 'conf', 'config.yaml')


# TODO: 리팩토링, controller 객체지향화, logging 적용, Pandas DataFrame화, Pyspark 활용 DB화

class Main:
    def __init__(self):
        self.log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        self.global_config: dict = yaml.load(open(GLOBAL_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(self.log_config)
        self.logger = logging.getLogger('main')

        result: list = []
        record: defaultdict = defaultdict(str)

        for line_num, text in enumerate(controller.gzip_readline(DATA_PATH)):
            if (len(text) == 0) or (line_num < self.global_config['start_headline']):
                continue

            time_v: str = controller.get_time(text)
            id_v: str = controller.get_id(text)
            command_v: str = controller.get_command(text)

            if time_v and id_v and command_v:
                if len(record.keys()) > 0:
                    result.append(record)

                record = defaultdict(str)
                record['time'] = controller.change_dateformat(time_v, format=global_config["dateformat"])
                record['id'] = id_v
                record['command'] = command_v
                record['argument'] = text.replace(time_v, '').replace(id_v, '').replace(command_v, '').strip()
            elif (time_v is None) and id_v and command_v:
                if len(record.keys()) > 0:
                    result.append(record)

                record = defaultdict(str)
                record['time'] = result[-1]['time']
                record['id'] = id_v
                record['command'] = command_v
                record['argument'] = text.replace(id_v, '').replace(command_v, '').strip()
            elif (time_v is None) and (id_v is None) and (command_v is None):
                record['argument'] += '\n' + text

        df = pd.DataFrame(result)
        controller.df2file(df, os.path.join('..', 'outputs', 'result_20210414.csv'))


if __name__ == '__main__':
    Main()
