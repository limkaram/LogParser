import re
import os
import yaml
import datetime
import chardet
from dateutil import parser
from collections import OrderedDict
from user_agents import parse
from typing import *

ACCESSLOG_CONFIG_PATH: str = os.path.join('..', 'conf', 'accesslog.yaml')


class Controller:
    def __init__(self):
        self.config: dict = yaml.load(open(ACCESSLOG_CONFIG_PATH), Loader=yaml.FullLoader)
        self.dateformat: str = self.config['dateformat']

    @staticmethod
    def get_encoding_type(text: str) -> str:
        return chardet.detect(text)['encoding']

    @staticmethod
    def _parse_useragent_string(user_agent_string: str):
        ua_info = parse(user_agent_string)
        extracted_info: OrderedDict = OrderedDict()
        extracted_info['os'] = ua_info.os.family
        extracted_info['osVersion'] = ua_info.os.version_string
        extracted_info['browser'] = ua_info.browser.family
        extracted_info['browserVersion'] = ua_info.browser.version_string
        extracted_info['device'] = ua_info.device.family
        extracted_info['deviceBrand'] = ua_info.device.brand
        extracted_info['deviceModel'] = ua_info.device.model
        extracted_info['isMobile'] = ua_info.is_mobile or ua_info.is_tablet
        extracted_info['isBot'] = ua_info.is_bot

        return extracted_info

    def parsing(self, text: str) -> dict:
        regex: str = self.config['log_regex']
        parsed_info: dict = re.search(regex, text).groupdict()
        parsed_info['time'] = self._change_dateformat(parsed_info['time'])
        detail_ua_info: dict = self._parse_useragent_string(parsed_info['agent'])
        parsed_info.update(detail_ua_info)

        return parsed_info

    def _change_dateformat(self, text: str) -> str:
        date_regex = re.search(r'([0-9]{1,2})[\/]([A-Za-z]+)[\/]([0-9]{4})[:]([0-9]{2})[:]([0-9]{2})[:]([0-9]{2})', text)
        day: int = int(date_regex.group(1))
        month: int = int(parser.parse(date_regex.group(2)).month)
        year: int = int(date_regex.group(3))
        hour: int = int(date_regex.group(4))
        minute: int = int(date_regex.group(5))
        second: int = int(date_regex.group(6))

        return datetime.datetime(year, month, day, hour, minute, second).strftime(self.dateformat)




