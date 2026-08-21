import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2

date_str = "2026-08-15"
df_day = get_daily_dataframe("betfair", date_str)

print(f"=== ANÁLISE COMPLETA DO DIA {date_str} PARA LAY 2X2 ===", flush=True)

if not df_day.empty:
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if 'under25_ft' in str(c).lower() or 'under 2.5' in str(c).lower()]
    if not odd_u25_col:
        odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]
    
    sinais = []
    
    for idx, r in df_day.iterrows():
        o_2x2 = pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce') if odd_2x2_col else 0.0
        o_u25 = pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce') if odd_u25_col else None
        o_h = pd.to_numeric(r.get(odd_h_col[0]), errors='coerce') if odd_h_col else None
        o_a = pd.to_numeric(r.get(odd_a_col[0]), errors='coerce') if odd_a_col else None
        
        o_2x2 = float(o_2x2) if pd.notna(o_2x2) else 0.0
        o_u25 = float(o_u25) if pd.notna(o_u25) else None
        o_h = float(o_h) if pd.notna(o_h) else None
        o_a = float(o_a) if pd.notna(o_a) else None
        
        ok, motivo = validar_entrada_lay2x2(
            odd_lay_2x2=o_2x2,
            odd_under25=o_u25,
            odd_h=o_h,
            odd_a=o_a
        )
        
        if ok:
            home = str(r.get("Home", r.get("Home_Team", "")))
            away = str(r.get("Away", r.get("Away_Team", "")))
            liga = str(r.get("League", r.get("Div", "")))
            sinais.append({
                "Home": home, "Away": away, "League": liga,
                "Odd_Lay_2x2": o_2x2, "Odd_U25": o_u25, "Motivo": motivo
            })
            
    print(f"\n[+] SINAIS APROVADOS EM 15/08: {len(sinais)} JOGOS", flush=True)
    df_s = pd.DataFrame(sinais)
    if not df_s.empty:
        print(df_s.to_string(), flush=True)
