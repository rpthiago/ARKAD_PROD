import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

date_str = "2026-08-16"
df_day = get_daily_dataframe("betfair", date_str)

sinais = []
if not df_day.empty:
    for idx, row in df_day.iterrows():
        odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
        odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
        odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
        odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
        xg_a = float(row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or 1.0)
        
        home = str(row.get("Home", row.get("Home_Team", "")))
        away = str(row.get("Away", row.get("Away_Team", "")))
        
        if 0.0 < odd_h <= 2.20 and odd_h < odd_a and 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and xg_a <= 1.10:
            sinais.append(f"{home} x {away} (Odd 0x3: {odd_0x3:.2f})")
            
print(f"=== TESTE FINAL DA PÁGINA LAY 0X3 EM 16/08 ===", flush=True)
print(f"[+] Total de Sinais Aprovados: {len(sinais)} JOGOS", flush=True)
for s in sinais:
    print(f"   * {s}", flush=True)
