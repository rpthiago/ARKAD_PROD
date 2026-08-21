import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

df_day = get_daily_dataframe("betfair", "2026-08-20")

red_matches = [
    ("Drita", "Inter"),
    ("Hearts", "Rapid"),
    ("Hajduk", "Rakow"),
    ("Real Santander", "Barranquilla")
]

print("=== VERIFICANDO AS ODDS DE UNDER 2.5 DOS 4 JOGOS QUE DERAM 2X2 ===", flush=True)

for h_sub, a_sub in red_matches:
    row = df_day[(df_day["Home"].astype(str).str.contains(h_sub, case=False, na=False)) & (df_day["Away"].astype(str).str.contains(a_sub, case=False, na=False))]
    if not row.empty:
        r = row.iloc[0]
        o_2x2 = r.get("Odd_CS_2x2_Lay")
        o_u25 = r.get("Odd_Under25_FT_Back")
        o_h = r.get("Odd_H_Back")
        o_a = r.get("Odd_A_Back")
        print(f"* {r.get('Home')} x {r.get('Away')}: Odd Lay 2x2: {o_2x2} | Odd Under 2.5: {o_u25} | Odd H: {o_h} | Odd A: {o_a}", flush=True)
    else:
        print(f"x Não encontrado: {h_sub} x {a_sub}", flush=True)
