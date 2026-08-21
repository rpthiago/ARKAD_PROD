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

for r in res[:15]:
    h = r.get("Home"); a = r.get("Away"); odd = r.get("Odd_CS_2x0_Lay") or r.get("Odd_CS_2x0")
    prob = r.get("Prob_ML")
    ev = strat._ev_lay(prob or 0.0, float(odd) if odd else 1.0)
    reason = r.get("Reason")
    print(f"{h} x {a} | Odd Lay 2x0: {odd} | Prob RF: {prob} | EV Lay: {ev:+.4f} | Motivo: {reason}")
