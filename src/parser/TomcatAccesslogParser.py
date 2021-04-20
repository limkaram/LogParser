import re
import os
import yaml
from collections import OrderedDict
from typing import *

CONFIG_PATH: str = os.path.join('..', 'conf', 'accesslog.yaml')


class Controller:
    def __init__(self):
        self.config: dict = yaml.load(open(CONFIG_PATH), Loader=yaml.FullLoader)

    @staticmethod
    def readline(path: str) -> Generator[str, None, None]:
        with open(path, 'r', encoding='UTF-8') as f:
            for line in f.readlines():
                yield line

    @staticmethod
    def get_ipaddress(text: str) -> str or None:
        ip_format_search = re.search(r'([0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3})', text)

        if ip_format_search is None:
            return None

        return ip_format_search.group(1)

    def parsing(self, text: str) -> dict:
        # TODO : regex pattern text get from config file(.yaml)
        parsed_info: OrderedDict = OrderedDict()
        regex: str = self.config['log_regex']
        parsed_regex = re.search(regex, text)

        parsed_info['ip'] = parsed_regex.group(1)
        parsed_info['user_id'] = parsed_regex.group(2)
        parsed_info['user_auth'] = parsed_regex.group(3)
        parsed_info['time'] = parsed_regex.group(4)
        parsed_info['http_respond_code'] = parsed_regex.group(5)
        parsed_info['request_processing_time'] = parsed_regex.group(6)
        parsed_info['transfer_bytes'] = parsed_regex.group(7)
        parsed_info['http_start_line'] = parsed_regex.group(8)
        parsed_info['referer'] = parsed_regex.group(9)
        parsed_info['user_agent'] = parsed_regex.group(10)
        parsed_info['skt_client_identity'] = parsed_regex.group(11)
        parsed_info['wap_profile'] = parsed_regex.group(12)

        return parsed_info

    def change_dateformat(self, text: str, format: str) -> str:
        # TODO : write method(format should get from config file(.yaml))
        pass




