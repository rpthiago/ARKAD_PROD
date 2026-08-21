import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

date_str = "2026-08-16"
df_day = get_daily_dataframe("betfair", date_str)

print(f"=== ANÁLISE DETALHADA DE COLUNAS E REGRAS LAY 0X3 EM {date_str} ===", flush=True)

targets = ["Chapecoense", "Progreso", "Luqueno", "Cajamarca", "Mirassol", "Central Cordoba", "Bahia", "Flamengo"]

for idx, row in df_day.iterrows():
    home = str(row.get("Home", row.get("Home_Team", "")))
    away = str(row.get("Away", row.get("Away_Team", "")))
    
    if any(t.lower() in home.lower() for t in targets):
        odd_h_val = row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H')
        odd_a_val = row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A')
        odd_u25_val = row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25')
        odd_0x3_val = row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3')
        
        print(f"\nMatch: {home} x {away}", flush=True)
        print(f"  Odd_H: {odd_h_val}", flush=True)
        print(f"  Odd_A: {odd_a_val}", flush=True)
        print(f"  Odd_U25: {odd_u25_val}", flush=True)
        print(f"  Odd_0x3: {odd_0x3_val}", flush=True)
