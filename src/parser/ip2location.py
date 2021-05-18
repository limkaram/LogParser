import pandas as pd
import os

class ip2location:
    def __init__(self):
        self.df = None

    def read_csv(self, path: str) -> pd.DataFrame:
        self.df = pd.read_csv(path)
        return pd.read_csv(path)

    @property
    def ip(self) -> list:
        return self.df['ip'].values


test_ip = ['119.204.102.20', '66.249.75.19', '39.7.231.132']
test_ = ip2location()
df = test_.read_csv(os.path.join('..', '..', 'data', 'GeoLite2-City-Blocks-IPv4.csv'))
print(df['network'].head(30))
print('')
print(df['network'].tail(30))

ip_list = df['network'].values
result = []

for ip in ip_list:
    if ip.startswith('119.204'):
        result.append(ip)

print(result)


