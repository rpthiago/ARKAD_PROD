import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

print("=== VERIFICANDO QUAIS CONDIÇÕES ESTÃO FALHANDO EM AGOSTO ===", flush=True)

df_all_list = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df = get_daily_dataframe("betfair", d_str)
    if df is not None and not df.empty:
        df["d_str"] = d_str
        df_all_list.append(df)

df_all = pd.concat(df_all_list, ignore_index=True)
print(f"Total de jogos baixados da API em Agosto: {len(df_all)}", flush=True)

c_odd_h = 0
c_odd_ha = 0
c_odd_u25 = 0
c_odd_0x3 = 0

for idx, row in df_all.iterrows():
    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    if 0.0 < odd_h <= 2.20: c_odd_h += 1
    if 0.0 < odd_h < odd_a: c_odd_ha += 1
    if 0.0 < odd_u25 <= 2.10: c_odd_u25 += 1
    if 14.0 <= odd_0x3 <= 35.0: c_odd_0x3 += 1

print(f"Jogos com Odd_H <= 2.20           : {c_odd_h}", flush=True)
print(f"Jogos com Odd_H < Odd_A          : {c_odd_ha}", flush=True)
print(f"Jogos com Odd_U25 <= 2.10        : {c_odd_u25}", flush=True)
print(f"Jogos com Odd_CS_0x3_Lay em [14,35]: {c_odd_0x3}", flush=True)

c_h_u25 = 0
c_h_0x3 = 0
c_u25_0x3 = 0
for idx, row in df_all.iterrows():
    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    if (0.0 < odd_h <= 2.20) and (0.0 < odd_u25 <= 2.10): c_h_u25 += 1
    if (0.0 < odd_h <= 2.20) and (14.0 <= odd_0x3 <= 35.0): c_h_0x3 += 1
    if (0.0 < odd_u25 <= 2.10) and (14.0 <= odd_0x3 <= 35.0): c_u25_0x3 += 1

print(f"Jogos com (Odd_H <= 2.20 AND Odd_U25 <= 2.10)              : {c_h_u25}", flush=True)
print(f"Jogos com (Odd_H <= 2.20 AND Odd_Lay_0x3 em [14,35])        : {c_h_0x3}", flush=True)
print(f"Jogos com (Odd_U25 <= 2.10 AND Odd_Lay_0x3 em [14,35])      : {c_u25_0x3}", flush=True)
