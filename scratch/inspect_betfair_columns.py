import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from futpythontrader_client import get_dataframe_safe
import pandas as pd

print("Downloading Betfair dataset...")
df = get_dataframe_safe("betfair")
print("Dataframe shape:", df.shape)

if not df.empty:
    # Save to disk
    df.to_csv("betfair_historical.csv", index=False)
    print("Saved to betfair_historical.csv!")
    
    # Print non-odd columns
    non_odd_cols = [c for c in df.columns if not c.startswith("Odd_")]
    print("\nNon-odd columns:")
    print(non_odd_cols)
    
    print("\nFirst row sample (non-odd columns):")
    first_row = df.iloc[0]
    print({c: first_row[c] for c in non_odd_cols if c in df.columns})
else:
    print("Failed to download Betfair historical data.")
