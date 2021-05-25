import os
import pandas as pd

def merge_csv(root: str, filename: str):
    df = pd.DataFrame()

    for f in os.listdir(root):
        filepath = os.path.join(root, f)
        temp_df = pd.read_csv(filepath, encoding='euc-kr', index_col=0)
        df = pd.concat([df, temp_df])

    print(df.head())
    print(df.info())
    df.to_csv(os.path.join(root, filename), encoding='euc-kr')