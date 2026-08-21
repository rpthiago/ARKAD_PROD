import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import coleta_lay_cs_aovivo
from futpythontrader_client import get_daily_dataframe

print("=== AUDITANDO ESTRATÉGIAS RF V2 COM A FÓRMULA DE EV LAY CORRIGIDA (HOJE: 20/08/2026) ===", flush=True)

hist = coleta_lay_cs_aovivo._hist_df()
df_day = get_daily_dataframe("betfair", "2026-08-20")

payload = df_day.to_dict("records")

strategies = [
    ("Lay 2x0", "lay_2x0_rf_v2_strategy"),
    ("Lay 0x2", "lay_0x2_rf_v2_strategy"),
    ("Lay 0x0", "lay_0x0_rf_v2_strategy"),
    ("Lay 1x0", "lay_1x0_rf_v2_strategy")
]

for name, mod_name in strategies:
    try:
        mod = __import__(mod_name, fromlist=["predict_and_evaluate_live"])
        res = mod.predict_and_evaluate_live(payload, hist)
        aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
        print(f"\n[+] {name} ({mod_name}): {len(aprovados)} sinais aprovados hoje!", flush=True)
        for a in aprovados[:5]:
            print(f"    -> {a.get('Home')} x {a.get('Away')} | Odd: {a.get('Odd')} | EV: {a.get('EV'):.4f}", flush=True)
    except Exception as e:
        print(f"x ERRO em {name}: {e}", flush=True)
