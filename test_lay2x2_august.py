import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2, ODD_LAY_2X2_MIN, ODD_LAY_2X2_MAX

print(f"=== TESTANDO LAY 2X2 QUANT EM AGOSTO (COM TETO {ODD_LAY_2X2_MAX:.2f}) ===", flush=True)

tot_2x2 = 0
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df_day = get_daily_dataframe("betfair", d_str)
    if df_day is None or df_day.empty: continue
    
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col: odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

    day_sinais = []
    for _, r in df_day.iterrows():
        o_2x2 = float(pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce')) if odd_2x2_col and pd.notna(r.get(odd_2x2_col[0])) else 0.0
        o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
        o_h = float(pd.to_numeric(r.get(odd_h_col[0]), errors='coerce')) if odd_h_col and pd.notna(r.get(odd_h_col[0])) else None
        o_a = float(pd.to_numeric(r.get(odd_a_col[0]), errors='coerce')) if odd_a_col and pd.notna(r.get(odd_a_col[0])) else None
        
        ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
        if ok:
            home = str(r.get("Home", r.get("Home_Team", "")))
            away = str(r.get("Away", r.get("Away_Team", "")))
            day_sinais.append(f"{home} x {away} (Odd 2x2: {o_2x2:.2f})")
            
    if day_sinais:
        print(f"\n[{d_str}] {len(day_sinais)} Sinais Aprovados no Lay 2x2:", flush=True)
        for s in day_sinais:
            print(f"   * {s}", flush=True)
        tot_2x2 += len(day_sinais)

print(f"\n[+] TOTAL DE SINAIS LAY 2X2 EM AGOSTO (TETO 20.0): {tot_2x2} SINAIS", flush=True)
