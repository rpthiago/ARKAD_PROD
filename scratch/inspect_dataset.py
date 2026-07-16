import pandas as pd
import os

f = "b365_base_lean.csv"
if os.path.exists(f):
    df = pd.read_csv(f)
    total = len(df)
    non_zero_xg_h = (df['xG_H_FT'] > 0).sum()
    non_zero_xg_a = (df['xG_A_FT'] > 0).sum()
    print(f"Total rows: {total}")
    print(f"Non-zero xG_H_FT: {non_zero_xg_h} ({non_zero_xg_h/total*100:.2f}%)")
    print(f"Non-zero xG_A_FT: {non_zero_xg_a} ({non_zero_xg_a/total*100:.2f}%)")
else:
    print("File not found")
