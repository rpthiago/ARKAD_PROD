import pandas as pd
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", nrows=5)
cs_cols = [c for c in df.columns if '0x3' in str(c) or '03' in str(c) or 'cs' in str(c).lower()]
print("CS cols in B365 CSV:", cs_cols)
