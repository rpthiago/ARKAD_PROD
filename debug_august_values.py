import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

print("=== DIAGNÓSTICO DE VALORES E ODDS DE LAY 0X3 EM AGOSTO ===", flush=True)

valid_count = 0
sample_matches = []

for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df = get_daily_dataframe("betfair", d_str)
        if df is None or df.empty: continue
        
        for idx, row in df.iterrows():
            odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
            odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
            odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
            odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
            
            home = str(row.get("Home", row.get("Home_Team", "")))
            away = str(row.get("Away", row.get("Away_Team", "")))
            
            if 0.0 < odd_h <= 2.20 and odd_h < odd_a and 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0:
                valid_count += 1
                sample_matches.append(f"[{d_str}] {home} x {away} (Odd H: {odd_h}, U25: {odd_u25}, Lay 0x3: {odd_0x3})")
    except Exception as e:
        print(f"Erro em {d_str}: {e}", flush=True)

print(f"\n[+] Total de jogos que atendem (Odd H <= 2.20 & U25 <= 2.10 & Lay 0x3 in [14, 35]): {valid_count}", flush=True)
for s in sample_matches[:20]:
    print(f"   * {s}", flush=True)
