import os
import pandas as pd

def merge_csv(root: str):
    df = pd.DataFrame()

    for filename in os.listdir(root):
        filepath = os.path.join(root, filename)
        temp_df = pd.read_csv(filepath, encoding='euc-kr', index_col=0)
        df = pd.concat([df, temp_df])

    print(df.head())
    print(df.info())
    df.to_csv(os.path.join(root, 'merged_referer2action_20210117-20210519.csv'), encoding='euc-kr')