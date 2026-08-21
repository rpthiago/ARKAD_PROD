import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2

print("=== VERIFICANDO O QUE A PÁGINA LAY 2X2 MOSTRA HOJE (20/08/2026) ===", flush=True)

df_day = get_daily_dataframe("betfair", "2026-08-20")

odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
if not odd_u25_col:
    odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

sinais = []
for _, r in df_day.iterrows():
    home = str(r.get("Home", r.get("Home_Team", "")))
    away = str(r.get("Away", r.get("Away_Team", "")))
    
    o_2x2 = pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce') if odd_2x2_col else 0.0
    o_u25 = pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce') if odd_u25_col else None
    o_h = pd.to_numeric(r.get(odd_h_col[0]), errors='coerce') if odd_h_col else None
    o_a = pd.to_numeric(r.get(odd_a_col[0]), errors='coerce') if odd_a_col else None
    
    o_2x2 = float(o_2x2) if pd.notna(o_2x2) else 0.0
    o_u25 = float(o_u25) if pd.notna(o_u25) else None
    o_h = float(o_h) if pd.notna(o_h) else None
    o_a = float(o_a) if pd.notna(o_a) else None
    
    ok, motivo = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
    if ok:
        sinais.append({"Home": home, "Away": away, "Odd Lay 2x2": o_2x2, "Odd Under 2.5 FT": o_u25, "Motivo": motivo})

df_sinais = pd.DataFrame(sinais)
print(f"\n[+] TOTAL DE SINAIS NA TELA HOJE: {len(df_sinais)}", flush=True)
print(df_sinais.to_string(index=False), flush=True)
