import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import coleta_lay_cs_aovivo
from futpythontrader_client import get_daily_dataframe
import lay_2x0_rf_v2_strategy as strat

hist = coleta_lay_cs_aovivo._hist_df()
df_day = get_daily_dataframe("betfair", "2026-08-20")

payload = df_day.to_dict("records")
res = strat.predict_and_evaluate_live(payload, hist)

reasons = [r.get("Reason") for r in res]
print(pd.Series(reasons).value_counts(), flush=True)
