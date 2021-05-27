import re
import pandas as pd
from src.parser.exceptions import *
from src.parser import utils
from collections import OrderedDict
from datetime import timedelta


class Parser:
    def __init__(self, referer: str):
        self.referer = referer

    def _parse(self) -> str:
        _searched = re.match(r'https?:\/\/www\.thepinx\.co\.kr\/.+\.px', self.referer)

        if _searched is None:
            raise PrefixURLNotFoundError(f'input[{self.referer}] is not matched with regex')
        else:
            full_url = _searched.group(0)
            return re.sub(r'https?:\/\/www\.thepinx\.co\.kr\/', '', full_url)

    @property
    def parsed_url(self):
        return self._parse()

    @property
    def url_elements(self) -> OrderedDict or pd.DataFrame:
        _temp: OrderedDict = OrderedDict()
        _temp['prefix_depth'] = []
        _temp['url_element'] = []

        for prefix_depth, url_element in enumerate(self.parsed_url.split('/')):
            _temp['prefix_depth'].append(prefix_depth)
            _temp['url_element'].append(url_element)

        return pd.DataFrame(_temp)

    @staticmethod
    def get_time_difference(before: str, after: str) -> str:
        last_t = utils.str2datetime(after)
        pre_t = utils.str2datetime(before)
        stay_t = last_t - timedelta(hours=pre_t.hour, minutes=pre_t.minute, seconds=pre_t.second)
        convert_time_to_sec = (stay_t.hour * 3600) + (stay_t.minute * 60) + stay_t.second

        return str(convert_time_to_sec) + 's'




