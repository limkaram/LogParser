import re
from typing import *
from pprint import pprint


class Controller:
    def __init__(self):
        pass

    @staticmethod
    def java_readline(path: str) -> Generator[str, None, None]:
        with open(path, encoding='UTF-8') as f:
            for line in f.readlines():
                yield line.strip()

    @staticmethod
    def get_url(text: str) -> str or None:
        searched_text = re.search(r'@RequestMapping[(].*["](.+)["]', text)

        if searched_text is None:
            return None

        return searched_text.group(1)
