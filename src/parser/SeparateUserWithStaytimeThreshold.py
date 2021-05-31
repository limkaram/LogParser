import re
import datetime
from datetime import timedelta
from src.parser import utils
from pprint import pprint

class Separator:
    def __init__(self, first_access_time, sequence, threshold: int = 120):
        self.first_access_time: str = first_access_time
        self.actions: list = sequence.split(' -> ')
        self.threshold: int = threshold

    def _separate(self):
        stay_times: list = []
        access_time: datetime.datetime = utils.str2datetime(self.first_access_time)
        result: dict = {}
        each_user_actions: list = []
        stay_time: int = 0

        for idx, action in enumerate(self.actions):
            stay_time_format = re.match(r'([0-9]+)[s]', action)

            if stay_time_format is not None:
                stay_time = int(stay_time_format.group(1))
                stay_times.append(stay_time)

                if stay_time > self.threshold:
                    result[access_time.strftime('%Y-%m-%d %H:%M:%S')] = each_user_actions
                    access_time = access_time + timedelta(seconds=sum(stay_times))
                    each_user_actions = []

            if (stay_time_format is not None) and (stay_time > self.threshold):
                pass
            else:
                each_user_actions.append(action)

        result[access_time.strftime('%Y-%m-%d %H:%M:%S')] = each_user_actions

        return result

    @property
    def separated_users_info(self):
        return self._separate()

