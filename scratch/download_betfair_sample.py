import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from futpythontrader_client import get_dataframe_safe
import pandas as pd

print("Downloading a sample of Betfair historical data...")
df = get_dataframe_safe("betfair")
print("Dataframe shape:", df.shape)
if not df.empty:
    print("Columns:", df.columns.tolist()[:30])
    cs_cols = [c for c in df.columns if "CS" in c or "Score" in c]
    print("CS related columns:", cs_cols)
else:
    print("Downloaded dataframe is empty or failed.")
