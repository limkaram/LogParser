import datetime

import pandas as pd
import gzip
import os
import re
from dateutil.parser import parse
from typing import *
from pprint import pprint


class Controller:
    @staticmethod
    def gzip_readline(path: str) -> Generator[str, None, None]:
        with gzip.open(path, 'r') as f:
            for line in f.readlines():
                yield line.decode('UTF-8').strip()

    @staticmethod
    def get_time(text: str) -> str or None:
        time_format_search = re.search(r'([0-9]{6}[ ]+[0-9]{1,2}[:][0-9]{2}[:][0-9]{2})', text)

        if time_format_search is None:
            return None

        return time_format_search.group(1).strip()

    @staticmethod
    def get_id(text: str) -> str or None:
        id_format_search = re.search(r'([0-9]{4,5})[ ]+[A-Z][a-z]+', text)

        if id_format_search is None:
            return None

        return id_format_search.group(1).strip()

    @staticmethod
    def get_command(text: str) -> str or None:
        id_format_search = re.search(r'[0-9]{4,5}[ ]+([A-Z][a-z]+)', text)

        if id_format_search is None:
            return None

        return id_format_search.group(1).strip()

    @staticmethod
    def df2file(df: pd.DataFrame, path: str):
        ftype = re.search(r'[.]([A-Za-z&]{1,})', os.path.basename(path)).group(1)

        if ftype == 'csv':
            df.to_csv(path)
        elif ftype == 'parquet':
            df.to_parquet(path)
        else:
            raise Exception(f'Undefined file extension :: {ftype}')

    @staticmethod
    def change_dateformat(text: str, format: str) -> str:
        text = datetime.datetime.now().strftime('%Y')[:2] + text
        year: str = text[:4]
        month: str = text[4:6]
        day: str = text[6:8]
        hhmmss = text.replace(year + month + day, '').strip().split(':')
        hour: str = hhmmss[0]
        minute: str = hhmmss[1]
        second: str = hhmmss[2]

        return datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second)).strftime(format)
