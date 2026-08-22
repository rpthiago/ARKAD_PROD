import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2
import coleta_lay_cs_aovivo

print("=== AUDITORIA COMPLETA DO DIA 15/08/2026 (BACKTEST VS SINAIS AO VIVO) ===", flush=True)

target_date = "2026-08-15"

# 1. Carregar Base Histórica (Backtest)
df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")
df_hist_day = df_hist[df_hist["d_str"] == target_date].copy()
print(f"[+] Total de jogos na Base Histórica em {target_date}: {len(df_hist_day)} jogos", flush=True)

# 2. Carregar Payload da Betfair API (Sinais ao Vivo)
df_bf_day = get_daily_dataframe("betfair", target_date)
print(f"[+] Total de jogos na Betfair API em {target_date}: {len(df_bf_day)} jogos\n", flush=True)

hist_base = coleta_lay_cs_aovivo._hist_df()

# -------------------------------------------------------------------------
# A. LAY 2X2 QUANT
# -------------------------------------------------------------------------
print("="*80, flush=True)
print("1. JOGOS SELECIONADOS NO LAY 2X2 QUANT (15/08/2026)", flush=True)
print("="*80, flush=True)
lay2x2_selected = []
for idx, r in df_bf_day.iterrows():
    h = str(r.get("Home", r.get("Home_Team", "")))
    a = str(r.get("Away", r.get("Away_Team", "")))
    liga = str(r.get("League", r.get("Div", "")))
    
    odd_2x2_col = [c for c in df_bf_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_bf_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col: odd_u25_col = [c for c in df_bf_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_bf_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_bf_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]
    
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
        row_h = df_hist_day[(df_hist_day["Home"].astype(str).str.lower() == h.lower()) & (df_hist_day["Away"].astype(str).str.lower() == a.lower())]
        placar = "N/D"
        res = "N/D"
        if not row_h.empty:
            gh = row_h.iloc[0].get("Goals_H_FT"); ga = row_h.iloc[0].get("Goals_A_FT")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                placar = f"{gh_i}x{ga_i}"
                res = "RED" if (gh_i == 2 and ga_i == 2) else "GREEN"
                
        lay2x2_selected.append({
            "Horário": str(r.get("Time", ""))[:5], "Liga": liga, "Mandante": h, "Visitante": a,
            "Odd Lay 2x2": o_2x2, "Odd Under 2.5 FT": o_u25, "Placar": placar, "Resultado": res
        })

df_l2x2 = pd.DataFrame(lay2x2_selected)
print(df_l2x2.to_string(index=False) if not df_l2x2.empty else "Nenhum jogo selecionado.")

# -------------------------------------------------------------------------
# B. LAY 0X3 VISITANTE
# -------------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("2. JOGOS SELECIONADOS NO LAY 0X3 VISITANTE (15/08/2026)", flush=True)
print("="*80, flush=True)
lay0x3_selected = []
for idx, r in df_bf_day.iterrows():
    h = str(r.get("Home", r.get("Home_Team", "")))
    a = str(r.get("Away", r.get("Away_Team", "")))
    liga = str(r.get("League", r.get("Div", "")))
    
    odd_0x3_col = [c for c in df_bf_day.columns if '0x3' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_bf_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    odd_a_col = [c for c in df_bf_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]
    
    o_0x3 = pd.to_numeric(r.get(odd_0x3_col[0]), errors='coerce') if odd_0x3_col else 0.0
    o_u25 = pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce') if odd_u25_col else None
    o_a = pd.to_numeric(r.get(odd_a_col[0]), errors='coerce') if odd_a_col else None
    
    o_0x3 = float(o_0x3) if pd.notna(o_0x3) else 0.0
    o_u25 = float(o_u25) if pd.notna(o_u25) else None
    o_a = float(o_a) if pd.notna(o_a) else 0.0
    
    xg_a_val = r.get('A_xGF_r5') or r.get('Media_Gols_Pro_Visitante') or r.get('xG_A_FT') or r.get('xg_a')
    xg_a_r5 = float(xg_a_val) if pd.notna(xg_a_val) else 1.0
    
    # Regra oficial Lay 0x3
    if (0.0 < o_u25 <= 2.10) and (14.0 <= o_0x3 <= 35.0) and (o_a >= 1.85 or o_a == 0.0) and (xg_a_r5 <= 1.10):
        row_h = df_hist_day[(df_hist_day["Home"].astype(str).str.lower() == h.lower()) & (df_hist_day["Away"].astype(str).str.lower() == a.lower())]
        placar = "N/D"
        res = "N/D"
        if not row_h.empty:
            gh = row_h.iloc[0].get("Goals_H_FT"); ga = row_h.iloc[0].get("Goals_A_FT")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                placar = f"{gh_i}x{ga_i}"
                res = "RED" if (gh_i == 0 and ga_i == 3) else "GREEN"
                
        lay0x3_selected.append({
            "Horário": str(r.get("Time", ""))[:5], "Liga": liga, "Mandante": h, "Visitante": a,
            "Odd Lay 0x3": o_0x3, "Odd Under 2.5": o_u25, "Odd Visitante": o_a, "Placar": placar, "Resultado": res
        })

df_l0x3 = pd.DataFrame(lay0x3_selected)
print(df_l0x3.to_string(index=False) if not df_l0x3.empty else "Nenhum jogo passou no filtro de Proteção xG / Favorito.")

# -------------------------------------------------------------------------
# C. LAY 0X1 (RF V2 & TRADER)
# -------------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("3. JOGOS SELECIONADOS NO LAY 0X1 (15/08/2026)", flush=True)
print("="*80, flush=True)
lay0x1_selected = []
for idx, r in df_bf_day.iterrows():
    h = str(r.get("Home", r.get("Home_Team", "")))
    a = str(r.get("Away", r.get("Away_Team", "")))
    liga = str(r.get("League", r.get("Div", "")))
    
    odd_0x1_col = [c for c in df_bf_day.columns if '0x1' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_bf_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    
    o_0x1 = float(pd.to_numeric(r.get(odd_0x1_col[0]), errors='coerce')) if odd_0x1_col and pd.notna(r.get(odd_0x1_col[0])) else 0.0
    o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
    
    if (6.0 <= o_0x1 <= 9.5) or (10.0 <= o_0x1 <= 18.0 and o_u25 and o_u25 <= 2.10):
        row_h = df_hist_day[(df_hist_day["Home"].astype(str).str.lower() == h.lower()) & (df_hist_day["Away"].astype(str).str.lower() == a.lower())]
        placar = "N/D"
        res = "N/D"
        if not row_h.empty:
            gh = row_h.iloc[0].get("Goals_H_FT"); ga = row_h.iloc[0].get("Goals_A_FT")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                placar = f"{gh_i}x{ga_i}"
                res = "RED" if (gh_i == 0 and ga_i == 1) else "GREEN"
        lay0x1_selected.append({
            "Horário": str(r.get("Time", ""))[:5], "Liga": liga, "Mandante": h, "Visitante": a,
            "Odd Lay 0x1": o_0x1, "Placar": placar, "Resultado": res
        })

df_l0x1 = pd.DataFrame(lay0x1_selected)
print(df_l0x1.to_string(index=False) if not df_l0x1.empty else "Nenhum jogo selecionado.")

# -------------------------------------------------------------------------
# D. LAY 1X0 RF V2
# -------------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("4. JOGOS SELECIONADOS NO LAY 1X0 RF V2 (15/08/2026)", flush=True)
print("="*80, flush=True)
try:
    mod_1x0 = __import__("lay_1x0_rf_v2_strategy", fromlist=["predict_and_evaluate_live"])
    res_1x0 = mod_1x0.predict_and_evaluate_live(df_bf_day.to_dict("records"), hist_base)
    aprovados_1x0 = [g for g in (res_1x0 or []) if g.get("Decision") == "APOSTA"]
    
    lay1x0_selected = []
    for g in aprovados_1x0:
        h = g.get("Home"); a = g.get("Away")
        row_h = df_hist_day[(df_hist_day["Home"].astype(str).str.lower() == str(h).lower()) & (df_hist_day["Away"].astype(str).str.lower() == str(a).lower())]
        placar = "N/D"
        res = "N/D"
        if not row_h.empty:
            gh = row_h.iloc[0].get("Goals_H_FT"); ga = row_h.iloc[0].get("Goals_A_FT")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                placar = f"{gh_i}x{ga_i}"
                res = "RED" if (gh_i == 1 and ga_i == 0) else "GREEN"
        lay1x0_selected.append({
            "Horário": str(g.get("Time", ""))[:5], "Liga": g.get("League"), "Mandante": h, "Visitante": a,
            "Odd Lay 1x0": g.get("Odd_CS_1x0_Lay"), "Placar": placar, "Resultado": res
        })
    df_l1x0 = pd.DataFrame(lay1x0_selected)
    print(df_l1x0.to_string(index=False) if not df_l1x0.empty else "Nenhum jogo selecionado.")
except Exception as e:
    print(f"Erro em Lay 1x0: {e}")

# -------------------------------------------------------------------------
# E. LAY DRAW RF V2
# -------------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("5. JOGOS SELECIONADOS NO LAY DRAW (LAY EMPATE) (15/08/2026)", flush=True)
print("="*80, flush=True)
try:
    mod_draw = __import__("lay_draw_rf_v2_strategy", fromlist=["predict_and_evaluate_live"])
    res_draw = mod_draw.predict_and_evaluate_live(df_bf_day.to_dict("records"), hist_base)
    aprovados_draw = [g for g in (res_draw or []) if g.get("Decision") == "APOSTA"]
    
    laydraw_selected = []
    for g in aprovados_draw:
        h = g.get("Home"); a = g.get("Away")
        row_h = df_hist_day[(df_hist_day["Home"].astype(str).str.lower() == str(h).lower()) & (df_hist_day["Away"].astype(str).str.lower() == str(a).lower())]
        placar = "N/D"
        res = "N/D"
        if not row_h.empty:
            gh = row_h.iloc[0].get("Goals_H_FT"); ga = row_h.iloc[0].get("Goals_A_FT")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                placar = f"{gh_i}x{ga_i}"
                res = "RED" if (gh_i == ga_i) else "GREEN"
        laydraw_selected.append({
            "Horário": str(g.get("Time", ""))[:5], "Liga": g.get("League"), "Mandante": h, "Visitante": a,
            "Odd Lay Empate": g.get("Odd_D_FT"), "Placar": placar, "Resultado": res
        })
    df_ldraw = pd.DataFrame(laydraw_selected)
    print(df_ldraw.to_string(index=False) if not df_ldraw.empty else "Nenhum jogo selecionado.")
except Exception as e:
    print(f"Erro em Lay Draw: {e}")
