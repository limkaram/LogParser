import re
import pandas as pd
from src.parser.exceptions import *
from collections import OrderedDict


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



