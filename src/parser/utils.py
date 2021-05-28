import os
import datetime
import pandas as pd

def merge_csv(root: str, filename: str):
    df = pd.DataFrame()

    for f in os.listdir(root):
        filepath = os.path.join(root, f)
        temp_df = pd.read_csv(filepath, encoding='euc-kr', index_col=0)
        df = pd.concat([df, temp_df])

    print(df.head())
    print(df.info())
    df.to_csv(os.path.join(root, filename), encoding='euc-kr')

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


