import os
import glob
import datetime
import pandas as pd
import re

def merge_csv(root: str, dst: str):
    dtype_dict: dict = {'host': str,
                        'firstAccessTime': str,
                        'totalResponseTime': int,
                        'totalLength': int,
                        'agent': str,
                        'os': str,
                        'osVersion': str,
                        'browser': str,
                        'browserVersion': str,
                        'device': str,
                        'deviceBrand': str,
                        'deviceModel': str,
                        'isMobile': bool,
                        'isBot': bool,
                        'totalStayTime': int,
                        'reservation': bool,
                        'cancel': bool,
                        'actionSequence': str}

    df = pd.DataFrame()

    for filepath in glob.glob(os.path.join(root, '*.csv')):
        each_df = pd.read_csv(filepath, dtype=dtype_dict)
        df = pd.concat([df, each_df])

    df.to_csv(dst, index=False)
    print(df.head())
    print(df.info())

def str2datetime(text: str, only: str=None):
    if only == 'year':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').year
    elif only == 'month':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').month
    elif only == 'day':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').day
    elif only == 'hour':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').hour
    elif only == 'minute':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').minute
    elif only == 'second':
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S').second
    else:
        return datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S')


def calculate_stay_time(sequence: list or str) -> int:
    total_stay_time: int = 0
    actions: list = []

    if type(sequence) == list:
        actions: list = sequence
    elif type(sequence) == str:
        actions: list = sequence.split(' -> ')

    for action in actions:
        stay_time_format = re.match(r'([0-9]+)[s]', action)
        if stay_time_format is not None:
            total_stay_time += int(stay_time_format.group(1))

    return total_stay_time
