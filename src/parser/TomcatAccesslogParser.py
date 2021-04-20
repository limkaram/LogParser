import re
import os
import yaml
import datetime
from dateutil import parser
from collections import OrderedDict
from user_agents import parse
from typing import *

CONFIG_PATH: str = os.path.join('..', 'conf', 'accesslog.yaml')


class Controller:
    def __init__(self):
        self.config: dict = yaml.load(open(CONFIG_PATH), Loader=yaml.FullLoader)
        self.dateformat: str = self.config['dateformat']

    @staticmethod
    def readline(path: str) -> Generator[str, None, None]:
        with open(path, 'r', encoding='UTF-8') as f:
            for line in f.readlines():
                yield line

    @staticmethod
    def _parse_useragent_string(user_agent_string: str):
        return parse(user_agent_string)

    def parsing(self, text: str) -> dict:
        # TODO : regex pattern text get from config file(.yaml)
        regex: str = self.config['log_regex']
        parsed_regex = re.search(regex, text)
        parsed_info: OrderedDict = OrderedDict()

        parsed_info['ip'] = parsed_regex.group(1)
        parsed_info['user_id'] = parsed_regex.group(2)
        parsed_info['user_auth'] = parsed_regex.group(3)
        parsed_info['time'] = self._change_dateformat(parsed_regex.group(4))
        parsed_info['http_respond_code'] = parsed_regex.group(5)
        parsed_info['request_processing_time'] = parsed_regex.group(6)
        parsed_info['transfer_bytes'] = parsed_regex.group(7)
        parsed_info['http_start_line'] = parsed_regex.group(8)
        parsed_info['referer'] = parsed_regex.group(9)
        parsed_info['user_agent'] = parsed_regex.group(10)
        ua_info = self._parse_useragent_string(parsed_info['user_agent'])
        parsed_info['os'] = ua_info.os.family
        parsed_info['os_version'] = ua_info.os.version_string
        parsed_info['browser'] = ua_info.browser.family
        parsed_info['browser_version'] = ua_info.browser.version_string
        parsed_info['device'] = ua_info.device.family
        parsed_info['device_brand'] = ua_info.device.brand
        parsed_info['device_model'] = ua_info.device.model
        parsed_info['skt_client_identity'] = parsed_regex.group(11)
        parsed_info['wap_profile'] = parsed_regex.group(12)

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




