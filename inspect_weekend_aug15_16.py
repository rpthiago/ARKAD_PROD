import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
from metodo_saldo_menor_strategy import evaluate_game, normalize_live_data, check_entry_conditions
from metodo_lay2x2_strategy import validar_entrada_lay2x2

print("=== DIAGNÓSTICO DOS DIAS 15/08 E 16/08/2026 PARA TODOS OS MÉTODOS ===", flush=True)

for date_str in ["2026-08-15", "2026-08-16"]:
    df_day = get_daily_dataframe("betfair", date_str)
    if df_day is None or df_day.empty:
        print(f"\n[{date_str}] Sem jogos no feed.", flush=True)
        continue
        
    print(f"\n" + "="*75, flush=True)
    print(f"=== ANÁLISE DE {date_str} ({len(df_day)} JOGOS NA BETFAIR) ===", flush=True)
    print("="*75, flush=True)
    
    # 1. LAY 0X3
    lay0x3_app = []
    for idx, r in df_day.iterrows():
        odd_h = float(r.get('Odd_H_FT_Back', 0.0) or r.get('Odd_H_FT', 0.0) or r.get('Odd_H', 0.0) or 0.0)
        odd_a = float(r.get('Odd_A_FT_Back', 0.0) or r.get('Odd_A_FT', 0.0) or r.get('Odd_A', 0.0) or 0.0)
        odd_u25 = float(r.get('Odd_Under25_FT_Back', 0.0) or r.get('Odd_Under25_FT', 0.0) or r.get('Odd_Under25', 0.0) or 0.0)
        odd_0x3 = float(r.get('Odd_CS_0x3_Lay', 0.0) or r.get('Odd_CS_0x3', 0.0) or 0.0)
        xg_a = float(r.get('A_xGF_r5', 0.0) or r.get('Media_Gols_Pro_Visitante', 0.0) or r.get('xG_A_FT', 1.0) or 1.0)
        
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        
        if 0.0 < odd_h <= 2.20 and odd_h < odd_a and 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and xg_a <= 1.10:
            lay0x3_app.append(f"{home} x {away} (Odd Lay: {odd_0x3:.2f})")
            
    print(f"\n[+] LAY 0X3 VISITANTE: {len(lay0x3_app)} SINAIS APROVADOS", flush=True)
    for a in lay0x3_app[:10]:
        print(f"   * {a}", flush=True)
    if len(lay0x3_app) > 10:
        print(f"   ... e mais {len(lay0x3_app)-10} jogos.", flush=True)

    # 2. LAY 2X2
    lay2x2_app = []
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col:
        odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]
    
    for idx, r in df_day.iterrows():
        o_2x2 = pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce') if odd_2x2_col else 0.0
        o_u25 = pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce') if odd_u25_col else None
        o_h = pd.to_numeric(r.get(odd_h_col[0]), errors='coerce') if odd_h_col else None
        o_a = pd.to_numeric(r.get(odd_a_col[0]), errors='coerce') if odd_a_col else None
        
        o_2x2 = float(o_2x2) if pd.notna(o_2x2) else 0.0
        o_u25 = float(o_u25) if pd.notna(o_u25) else None
        o_h = float(o_h) if pd.notna(o_h) else None
        o_a = float(o_a) if pd.notna(o_a) else None
        
        ok, motivo = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        if ok:
            lay2x2_app.append(f"{home} x {away} (Odd Lay: {o_2x2:.2f})")
            
    print(f"\n[+] LAY 2X2 QUANT: {len(lay2x2_app)} SINAIS APROVADOS", flush=True)
    for a in lay2x2_app[:10]:
        print(f"   * {a}", flush=True)
    if len(lay2x2_app) > 10:
        print(f"   ... e mais {len(lay2x2_app)-10} jogos.", flush=True)

    # 3. SALDO MENOR
    sm_app = []
    for idx, r in df_day.iterrows():
        g = r.to_dict()
        norm = normalize_live_data(g)
        ok, rsn = check_entry_conditions(norm, check_betmines=False)
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        if ok:
            sm_app.append(f"{home} x {away}")
            
    print(f"\n[+] MÉTODO SALDO MENOR: {len(sm_app)} SINAIS APROVADOS", flush=True)
    for a in sm_app[:10]:
        print(f"   * {a}", flush=True)
    if len(sm_app) > 10:
        print(f"   ... e mais {len(sm_app)-10} jogos.", flush=True)
