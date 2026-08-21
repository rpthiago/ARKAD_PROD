from futpythontrader_client import get_daily_dataframe
df = get_daily_dataframe("betfair", "2026-08-16")
print("Todas as colunas em 2026-08-16:", list(df.columns))
h_cols = [c for c in df.columns if 'h' in c.lower() or 'home' in c.lower() or 'odd' in c.lower()]
print("Colunas de H/Home/Odd:", h_cols)
