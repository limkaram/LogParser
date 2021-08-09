from src.parser import utils, URLparser
from src.parser.exceptions import *
import datetime
from datetime import datetime
import re
import pandas as pd
import os
import numpy as np
from typing import Generator
from pprint import pprint
import yaml
import logging
import logging.config

PROJECT_ROOT_PATH: str = os.path.abspath('..')
USERINFO_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'userinfo.yaml')
LOGGER_CONFIG_PATH: str = os.path.join(PROJECT_ROOT_PATH, 'conf', 'logger.yaml')


class Extractor:
    def __init__(self, accesslog_df: pd.DataFrame, url_description_df: pd.DataFrame):
        log_config: dict = yaml.load(open(LOGGER_CONFIG_PATH), Loader=yaml.FullLoader)
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('UserInfoFeatrueExtractor')
        userinfo_config: dict = yaml.load(open(USERINFO_CONFIG_PATH, encoding='utf-8'), Loader=yaml.FullLoader)
        self.unnecessaries: list = userinfo_config['unnecessaries']
        accesslog_df['length'] = pd.to_numeric(accesslog_df['length'].str.replace('-', '0'),
                                               errors='coerce',
                                               downcast='integer')
        self.accesslog_df: pd.DataFrame = accesslog_df
        self.url_description_df: pd.DataFrame = url_description_df
        self._host = None
        self._agent = None
        self._user_specific_info: dict = {'host': [],
                                         'firstAccessTime': [],
                                         'totalResponseTime': [],
                                         'totalLength': [],
                                         'agent': [],
                                         'os': [],
                                         'osVersion': [],
                                         'browser': [],
                                         'browserVersion': [],
                                         'device': [],
                                         'deviceBrand': [],
                                         'deviceModel': [],
                                         'isMobile': [],
                                         'isBot': [],
                                         'totalStayTime': [],
                                         'reservation': [],
                                         'cancel': [],
                                         'actionSequence': []}

    @property
    def _get_user_classifying_elements(self) -> np.ndarray:
        return self.accesslog_df.groupby(by=['host', 'agent'], as_index=False)['userID'].count()[['host', 'agent']].values

    @property
    def _condition(self) -> bool:
        return (self.accesslog_df['host'] == self.host_) & (self.accesslog_df['agent'] == self.agent_)

    def _get_each_user_info(self):
        for idx, (host, agent) in enumerate(self._get_user_classifying_elements):
            self.host_ = host
            self.agent_ = agent
            self.user_df = self.accesslog_df.loc[self._condition]
            self._user_specific_info['host'].append(self.host)
            self._user_specific_info['firstAccessTime'].append(self.first_access_time)
            self._user_specific_info['totalResponseTime'].append(self.total_response_time)
            self._user_specific_info['totalLength'].append(self.total_transfer_bytes)
            self._user_specific_info['agent'].append(self.user_agent)
            self._user_specific_info['os'].append(self.os)
            self._user_specific_info['osVersion'].append(self.os_version)
            self._user_specific_info['browser'].append(self.browser)
            self._user_specific_info['browserVersion'].append(self.browser_version)
            self._user_specific_info['device'].append(self.device)
            self._user_specific_info['deviceBrand'].append(self.device_brand)
            self._user_specific_info['deviceModel'].append(self.device_model)
            self._user_specific_info['isMobile'].append(self.is_mobile)
            self._user_specific_info['isBot'].append(self.is_bot)
            self._user_specific_info['totalStayTime'].append(self.total_stay_time)
            self._user_specific_info['reservation'].append(self.reservation)
            self._user_specific_info['cancel'].append(self.cancel)
            self._user_specific_info['actionSequence'].append(self.action_sequence)

    @property
    def user_specific_info_df(self) -> pd.DataFrame:
        self._get_each_user_info()

        return pd.DataFrame(self._user_specific_info)

    @property
    def host(self):
        return self.host_

    @property
    def first_access_time(self):
        return self.user_df['time'].iloc[0]

    @property
    def total_response_time(self) -> int:
        return self.user_df['responseTime'].sum()

    @property
    def total_transfer_bytes(self) -> int:
        return self.user_df['length'].sum()

    @property
    def user_agent(self) -> str:
        return self.agent_

    @property
    def os(self) -> str:
        return self.user_df['os'].iloc[0]

    @property
    def os_version(self) -> str:
        return self.user_df['osVersion'].iloc[0]

    @property
    def browser(self) -> str:
        return self.user_df['browser'].iloc[0]

    @property
    def browser_version(self) -> str:
        return self.user_df['browserVersion'].iloc[0]

    @property
    def device(self) -> str:
        return self.user_df['device'].iloc[0]

    @property
    def device_brand(self) -> str:
        return self.user_df['deviceBrand'].iloc[0]

    @property
    def device_model(self) -> str:
        return self.user_df['deviceModel'].iloc[0]

    @property
    def is_mobile(self) -> bool:
        return self.user_df['isMobile'].iloc[0]

    @property
    def is_bot(self) -> bool:
        return self.user_df['isBot'].iloc[0]

    @property
    def total_stay_time(self) -> int:
        return sum(map(int, re.findall(r'([0-9]+)[s]', self.action_sequence)))

    @property
    def action_sequence(self) -> str:
        if len(self.user_df['referer'].unique()) == 1 and self.user_df['referer'].unique()[0] == '-':
            return '-'

        temp_df = self.user_df[['time', 'referer']]
        temp_df = temp_df.drop_duplicates()
        temp_df = temp_df.sort_values(by='time')
        one_user_actions: list = []
        previous_time: str = self.first_access_time

        for access_time, referer in zip(temp_df['time'], temp_df['referer']):
            print(access_time, referer)
            action: str = ''
            parser = URLparser.Parser(referer)

            try:
                url_elements_info = parser.url_elements.values

                for prefix_depth, url_element in url_elements_info:
                    condition1 = (self.url_description_df['prefix_depth'] == prefix_depth)
                    condition2 = (self.url_description_df['url_element'] == url_element)
                    try:
                        action += self.url_description_df['description'].loc[condition1 & condition2].values[0] + ' '
                    except IndexError as e:
                        self.logger.error(f'{e} :: fail : {(prefix_depth, url_element)}')
                        action += '[UNK]'
                        continue

                if len(one_user_actions) > 0:
                    stay_sec = parser.get_time_difference(previous_time, access_time)
                    one_user_actions.append(stay_sec)
                    previous_time = access_time

                one_user_actions.append(action.strip())
            except PrefixURLNotFoundError:
                if referer in self.unnecessaries:
                    continue

                if len(one_user_actions) > 0:
                    stay_sec = parser.get_time_difference(previous_time, access_time)
                    one_user_actions.append(stay_sec)
                    previous_time = access_time

                one_user_actions.append(referer)
            except TypeError as e:
                print(f'!ERROR :: {e} :: referer {referer}')
        return ' -> '.join(one_user_actions)

    @property
    def reservation(self) -> bool:
        if '객실예약 완료' in self.action_sequence:
            return True

        return False

    @property
    def cancel(self) -> int:
        if '예약취소' in self.action_sequence:
            return True

        return False





