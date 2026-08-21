import pandas as pd
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", nrows=5)
u_cols = [c for c in df.columns if 'under' in str(c).lower() or '25' in str(c).lower()]
print("Under/25 cols in B365 CSV:", u_cols)
