import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import coleta_lay_cs_aovivo
from futpythontrader_client import get_daily_dataframe

hist = coleta_lay_cs_aovivo._hist_df()
print(f"[+] Base histórica carregada: {len(hist):,} jogos", flush=True)

strategies = [
    ("Lay 2x0", "lay_2x0_rf_v2_strategy"),
    ("Lay 0x2", "lay_0x2_rf_v2_strategy"),
    ("Lay 0x0", "lay_0x0_rf_v2_strategy"),
    ("Lay 1x0", "lay_1x0_rf_v2_strategy")
]

for name, mod_name in strategies:
    tot_sinais = 0
    try:
        mod = __import__(mod_name, fromlist=["predict_and_evaluate_live"])
        for day in range(1, 21):
            d_str = f"2026-08-{day:02d}"
            df_day = get_daily_dataframe("betfair", d_str)
            if df_day is None or df_day.empty: continue
            payload = df_day.to_dict("records")
            res = mod.predict_and_evaluate_live(payload, hist)
            aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
            tot_sinais += len(aprovados)
        print(f"* {name} ({mod_name}): {tot_sinais} sinais gerados em Agosto (01 a 20/08)", flush=True)
    except Exception as e:
        print(f"x ERRO em {name} ({mod_name}): {e}", flush=True)
