from src.parser import utils
import datetime
import re
import pandas as pd
import os
import numpy as np
from collections import Counter


class Extractor:
    def __init__(self, path: str):
        self._path: str = path
        self._filename: str = os.path.basename(path)
        self._df: pd.DataFrame = pd.read_csv(path, encoding='euc-kr', index_col=0).dropna(axis=0)
        self._action_sequences_info: dict = self._get_action_sequences_info()

    def _get_action_sequences_info(self) -> dict:
        sequences_actions_list: list = []
        sequences_len_list: list = []
        sequences_stay_time_list: list = []

        for sequence in self._df['actions'].values:
            sequence_info: dict = self.get_each_action_sequence_info(sequence)
            sequences_actions_list.append(sequence_info['actions'])
            sequences_len_list.append(sequence_info['actions_len'])
            sequences_stay_time_list.append(sum(sequence_info['stay_times']))

        return {'sequences_actions': sequences_actions_list,
                'sequences_len': sequences_len_list,
                'sequences_stay_time': sequences_stay_time_list}

    def get_access_users_num_in_timezone(self, timezone: int) -> int:
        return (self._df['first_access_time'].apply(lambda x: utils.str2datetime(x, only='hour')) == timezone).sum()

    @staticmethod
    def get_each_action_sequence_info(sequence: str) -> dict:
        pattern: str = r'[-][>][ ]([0-9]+)[s][ ][-][>]'
        actions_list: list = [i.strip() for i in re.sub(pattern, ' -> ', sequence).split(' -> ')]
        stay_time_list: list = [int(i) for i in re.findall(pattern, sequence)]

        return {'actions': actions_list, 'actions_len': len(actions_list), 'stay_times': stay_time_list}

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def date(self) -> str:
        return datetime.datetime.strptime(self._df['first_access_time'].iloc[0], '%Y-%m-%d %H:%M:%S')\
                .strftime('%Y-%m-%d')

    @property
    def access_users_num(self) -> int:
        return len(self._df)

    @property
    def membership_join_num(self) -> int:
        return self._df['actions'].str.contains('멤버십 가입 완료').sum()

    @property
    def reservation_users_num(self) -> int:
        pay_cond = self._df['actions'].str.contains(r'예약 결제')
        rev_cond = self._df['actions'].str.contains(r'예약 완료')

        return len(self._df.loc[pay_cond | rev_cond, 'actions'])

    @property
    def action_sequence_len_min(self) -> int:
        return int(np.min(self._action_sequences_info['sequences_len']))

    @property
    def action_sequence_len_max(self) -> int:
        return int(np.max(self._action_sequences_info['sequences_len']))

    @property
    def action_sequence_len_mean(self) -> float:
        return float(np.mean(self._action_sequences_info['sequences_len']))

    @property
    def action_sequence_len_median(self) -> float:
        return float(np.median(self._action_sequences_info['sequences_len']))

    @property
    def first_access_location_modest(self) -> str:
        counter = Counter([i[0] for i in self._action_sequences_info['sequences_actions']])
        modest_action: str = counter.most_common()[0][0]
        return modest_action

    @property
    def first_access_location_least(self) -> str:
        counter = Counter([i[0] for i in self._action_sequences_info['sequences_actions']])
        least_action: str = counter.most_common()[-1][0]
        return least_action

    @property
    def departure_location_modest(self) -> str:
        counter = Counter([i[-1] for i in self._action_sequences_info['sequences_actions']])
        modest_action: str = counter.most_common()[0][0]
        return modest_action

    @property
    def departure_location_least(self) -> str:
        counter = Counter([i[-1] for i in self._action_sequences_info['sequences_actions']])
        least_action: str = counter.most_common()[-1][0]
        return least_action

    @property
    def access_location_modest(self) -> str:
        all_actions: list = []

        for actions in self._action_sequences_info['sequences_actions']:
            all_actions += actions

        modest_action: str = Counter(all_actions).most_common()[0][0]
        return modest_action

    @property
    def access_location_least(self) -> str:
        all_actions: list = []

        for actions in self._action_sequences_info['sequences_actions']:
            all_actions += actions

        least_action: str = Counter(all_actions).most_common()[-1][0]
        return least_action

    @property
    def stay_time_min(self) -> int:
        return int(np.min(self._action_sequences_info['sequences_stay_time']))

    @property
    def stay_time_max(self) -> int:
        return int(np.max(self._action_sequences_info['sequences_stay_time']))

    @property
    def stay_time_mean(self) -> float:
        return float(np.mean(self._action_sequences_info['sequences_stay_time']))

    @property
    def stay_time_median(self) -> float:
        return float(np.median(self._action_sequences_info['sequences_stay_time']))

