from src.parser import utils
from src.parser.exceptions import *
import datetime
from datetime import datetime
import re
import pandas as pd
import os
import numpy as np
from typing import Generator
from pprint import pprint


class Extractor:
    def __init__(self, parsed_df: pd.DataFrame, sequence_df: pd.DataFrame):
        self.parsed_df: pd.DataFrame = parsed_df
        self.parsed_df['transfer_bytes'] = self.parsed_df['transfer_bytes'].str.replace('-', '0')
        self.parsed_df['transfer_bytes'] = self.parsed_df['transfer_bytes'].astype('int64')
        self.sequence_df: pd.DataFrame = sequence_df.dropna(axis=0).reset_index(drop=True)
        self.ip_: str = ''
        self.first_access_time_: str = ''
        self.actions_: str = ''
        self.user_specific_info: dict = {'user_index': [],
                                         'ip': [],
                                         'first_access_time': [],
                                         'first_access_location': [],
                                         'total_request_processing_time': [],
                                         'total_transfer_bytes': [],
                                         'user_agent': [],
                                         'os': [],
                                         'os_version': [],
                                         'browser': [],
                                         'browser_version': [],
                                         'device': [],
                                         'device_brand': [],
                                         'device_model': [],
                                         'total_stay_time': [],
                                         'reservation': [],
                                         'cancel': [],
                                         'actions': [],
                                         }

    def _get_each_user_info(self):
        for idx, (ip, first_access_time, actions) in enumerate(zip(self.ip, self.first_access_time, self.actions)):
            self.ip_ = ip
            self.first_access_time_ = first_access_time
            self.actions_ = actions

            self.user_specific_info['user_index'].append(idx)
            self.user_specific_info['ip'].append(self.ip_)
            self.user_specific_info['first_access_time'].append(self.first_access_time_)
            self.user_specific_info['first_access_location'].append(self.first_access_location)
            self.user_specific_info['total_request_processing_time'].append(self.total_request_processing_time)
            self.user_specific_info['total_transfer_bytes'].append(self.total_transfer_bytes)
            self.user_specific_info['user_agent'].append(self.user_agent)
            self.user_specific_info['os'].append(self.os)
            self.user_specific_info['os_version'].append(self.os_version)
            self.user_specific_info['browser'].append(self.browser)
            self.user_specific_info['browser_version'].append(self.browser_version)
            self.user_specific_info['device'].append(self.device)
            self.user_specific_info['device_brand'].append(self.device_brand)
            self.user_specific_info['device_model'].append(self.device_model)
            self.user_specific_info['total_stay_time'].append(self.total_stay_time)
            self.user_specific_info['reservation'].append(self.reservation)
            self.user_specific_info['cancel'].append(self.cancel)
            self.user_specific_info['actions'].append(self.actions_)

    @property
    def user_specific_info_df(self) -> pd.DataFrame:
        self._get_each_user_info()

        return pd.DataFrame(self.user_specific_info)

    @property
    def date(self) -> str:
        before_time_format: str = '%Y-%m-%d %H:%M:%S'
        after_time_format: str = '%Y%m%d'
        p_date = datetime.strptime(self.parsed_df['time'].loc[0], before_time_format).strftime(after_time_format)
        s_date = datetime.strptime(self.sequence_df['first_access_time'].loc[0], before_time_format).strftime(after_time_format)

        if p_date != s_date:
            raise TwoDataFrameNotSameDateError(f'Two df are not same date')

        return s_date

    @property
    def ip(self) -> Generator[str, None, None]:
        for ip in self.sequence_df['ip']:
            yield ip

    @property
    def first_access_time(self) -> Generator[str, None, None]:
        for time in self.sequence_df['first_access_time']:
            yield time

    @property
    def actions(self) -> Generator[str, None, None]:
        for actions in self.sequence_df['actions']:
            yield actions

    @property
    def _condition(self):
        return (self.parsed_df['ip'] == self.ip_) & (self.parsed_df['time'] == self.first_access_time_)

    @property
    def first_access_location(self) -> str:
        return self.actions_.split(' -> ')[0]

    @property
    def total_request_processing_time(self) -> int:
        return sum(self.parsed_df.loc[self.parsed_df['ip'] == self.ip_, 'request_processing_time'])

    @property
    def total_transfer_bytes(self) -> int:
        return sum(self.parsed_df.loc[self.parsed_df['ip'] == self.ip_, 'transfer_bytes'])

    @property
    def user_agent(self) -> str:
        return self.parsed_df.loc[self._condition, 'user_agent'].iloc[0]

    @property
    def os(self) -> str:
        return self.parsed_df.loc[self._condition, 'os'].iloc[0]

    @property
    def os_version(self) -> str:
        return self.parsed_df.loc[self._condition, 'os_version'].iloc[0]

    @property
    def browser(self) -> str:
        return self.parsed_df.loc[self._condition, 'browser'].iloc[0]

    @property
    def browser_version(self) -> str:
        return self.parsed_df.loc[self._condition, 'browser_version'].iloc[0]

    @property
    def device(self) -> str:
        return self.parsed_df.loc[self._condition, 'device'].iloc[0]

    @property
    def device_brand(self) -> str:
        return self.parsed_df.loc[self._condition, 'device_brand'].iloc[0]

    @property
    def device_model(self) -> str:
        return self.parsed_df.loc[self._condition, 'device_model'].iloc[0]

    @property
    def total_stay_time(self) -> int:
        return sum(map(int, re.findall(r'([0-9]+)[s]', self.actions_)))

    @property
    def reservation(self) -> int:
        if ('예약 결제' in self.actions_) or ('예약 완료' in self.actions_):
            return 1

        return 0

    @property
    def cancel(self) -> int:
        if '예약취소' in self.actions_:
            return 1

        return 0





