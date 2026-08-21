import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import coleta_lay_cs_aovivo
from futpythontrader_client import get_daily_dataframe
import lay_2x0_rf_v2_strategy as strat

hist = coleta_lay_cs_aovivo._hist_df()
df_day = get_daily_dataframe("betfair", "2026-08-20")

payload = df_day.to_dict("records")

print(f"Total jogos no payload: {len(payload)}", flush=True)
res = strat.predict_and_evaluate_live(payload, hist)
print(f"Total retornados por predict_and_evaluate_live: {len(res)}", flush=True)

if res:
    print("Primeiro resultado:", res[0])
