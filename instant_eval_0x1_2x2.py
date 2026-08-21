import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2

print("=== BACKTEST INSTANTÂNEO DE AGOSTO (01 A 20/08) ===", flush=True)

lay2x2_ops = []

for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df_day = get_daily_dataframe("betfair", d_str)
    if df_day is None or df_day.empty: continue
    
    # -------------------------------------------------------------
    # LAY 2X2 (TETO 20.0)
    # -------------------------------------------------------------
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col: odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

    for _, r in df_day.iterrows():
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        
        gh_val = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else (r.get("Home_Score") if pd.notna(r.get("Home_Score")) else r.get("gols_mandante"))
        ga_val = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else (r.get("Away_Score") if pd.notna(r.get("Away_Score")) else r.get("gols_visitante"))
        
        o_2x2 = float(pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce')) if odd_2x2_col and pd.notna(r.get(odd_2x2_col[0])) else 0.0
        o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
        o_h = float(pd.to_numeric(r.get(odd_h_col[0]), errors='coerce')) if odd_h_col and pd.notna(r.get(odd_h_col[0])) else None
        o_a = float(pd.to_numeric(r.get(odd_a_col[0]), errors='coerce')) if odd_a_col and pd.notna(r.get(odd_a_col[0])) else None
        
        ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
        if ok:
            if gh_val is not None and ga_val is not None and pd.notna(gh_val) and pd.notna(ga_val):
                try:
                    gh_i = int(float(gh_val)); ga_i = int(float(ga_val))
                    is_2x2 = (gh_i == 2 and ga_i == 2)
                    res = "GREEN" if not is_2x2 else "RED"
                    pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
                    lay2x2_ops.append({"Data": d_str, "Confronto": f"{home} x {away}", "Odd_Lay_2x2": o_2x2, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL_R$": pnl})
                except Exception: pass

df_2x2 = pd.DataFrame(lay2x2_ops)

print("\n" + "="*80, flush=True)
print("=== LAY 2X2 QUANT (TETO 20.00) EM AGOSTO ===", flush=True)
print("="*80, flush=True)
if not df_2x2.empty:
    tot = len(df_2x2)
    grn = (df_2x2["Resultado"] == "GREEN").sum()
    red = (df_2x2["Resultado"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = df_2x2["PnL_R$"].sum()
    print(f"Total de Operações Aprovadas : {tot}", flush=True)
    print(f"Greens                       : {grn} ({wr:.2f}%)", flush=True)
    print(f"Reds                         : {red}", flush=True)
    print(f"Lucro Líquido Acumulado (R$) : R$ {pnl:,.2f}", flush=True)

df_2x2.to_excel("Backtest_Instantaneo_Lay2x2_Agosto.xlsx", index=False)
