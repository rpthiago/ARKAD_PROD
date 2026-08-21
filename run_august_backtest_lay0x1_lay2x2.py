import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, requests

from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2
import coleta_lay_cs_aovivo

print("=== INICIANDO BACKTEST DE AGOSTO (01 A 20/08) PARA LAY 2X2 (TETO 20.0) E LAY 0X1 ===", flush=True)

# 1. Carregar placares ESPN API para Agosto
db_scores = {}
for day in range(1, 21):
    date_str = f"202608{day:02d}"
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates={date_str}&limit=1000"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            events = r.json().get('events', [])
            for ev in events:
                try:
                    comp = ev.get('competitions', [])[0]
                    competitors = comp.get('competitors', [])
                    h_comp = [c for c in competitors if c.get('homeAway') == 'home'][0]
                    a_comp = [c for c in competitors if c.get('homeAway') == 'away'][0]
                    h_name = h_comp.get('team', {}).get('displayName', '').lower().strip()
                    a_name = a_comp.get('team', {}).get('displayName', '').lower().strip()
                    status = comp.get('status', {}).get('type', {}).get('name', '')
                    if status in ['STATUS_FULL_TIME', 'STATUS_FINAL', 'STATUS_APPROVED', 'FULL_TIME']:
                        gh = int(h_comp.get('score', 0))
                        ga = int(a_comp.get('score', 0))
                        dt_fmt = f"2026-08-{day:02d}"
                        hk = ''.join(c for c in h_name if c.isalnum())
                        ak = ''.join(c for c in a_name if c.isalnum())
                        db_scores[(dt_fmt, hk, ak)] = (gh, ga)
                        db_scores[(dt_fmt, hk[:5], ak[:5])] = (gh, ga)
                except Exception: pass
    except Exception: pass

# 2. Carregar jogos diários e aplicar estratégias
lay2x2_ops = []
lay0x1_ops = []

hist = coleta_lay_cs_aovivo._hist_df()
cfg_0x1 = coleta_lay_cs_aovivo.MERCADOS["0x1"]

for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df_day = get_daily_dataframe("betfair", d_str)
    if df_day is None or df_day.empty: continue
    
    # -------------------------------------------------------------
    # A. LAY 2X2 QUANT (TETO 20.00)
    # -------------------------------------------------------------
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col: odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

    for _, r in df_day.iterrows():
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        
        # Resolver placar final
        gh, ga = None, None
        for gh_c in ["Goals_H_FT", "gols_mandante", "Home_Score"]:
            if gh_c in r and pd.notna(r[gh_c]):
                try: gh = int(float(r[gh_c])); break
                except: pass
        for ga_c in ["Goals_A_FT", "gols_visitante", "Away_Score"]:
            if ga_c in r and pd.notna(r[ga_c]):
                try: ga = int(float(r[ga_c])); break
                except: pass
        if gh is None or ga is None:
            hk = ''.join(c for c in home.lower() if c.isalnum())
            ak = ''.join(c for c in away.lower() if c.isalnum())
            if (d_str, hk, ak) in db_scores: gh, ga = db_scores[(d_str, hk, ak)]
            elif (d_str, hk[:5], ak[:5]) in db_scores: gh, ga = db_scores[(d_str, hk[:5], ak[:5])]
            
        o_2x2 = float(pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce')) if odd_2x2_col and pd.notna(r.get(odd_2x2_col[0])) else 0.0
        o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
        o_h = float(pd.to_numeric(r.get(odd_h_col[0]), errors='coerce')) if odd_h_col and pd.notna(r.get(odd_h_col[0])) else None
        o_a = float(pd.to_numeric(r.get(odd_a_col[0]), errors='coerce')) if odd_a_col and pd.notna(r.get(odd_a_col[0])) else None
        
        ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
        if ok and gh is not None and ga is not None:
            is_2x2 = (gh == 2 and ga == 2)
            res = "GREEN" if not is_2x2 else "RED"
            pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
            lay2x2_ops.append({
                "Data": d_str, "Home": home, "Away": away, "Odd_Lay_2x2": o_2x2,
                "Placar": f"{gh}x{ga}", "Resultado": res, "PnL_R$": pnl
            })
            
    # -------------------------------------------------------------
    # B. LAY 0X1 (TRADER & RF v2)
    # -------------------------------------------------------------
    sinais_0x1 = coleta_lay_cs_aovivo.sinais_do_dia(d_str, cfg_0x1)
    if sinais_0x1:
        for s in sinais_0x1:
            home = str(s.get("Mandante", ""))
            away = str(s.get("Visitante", ""))
            odd_lay = float(pd.to_numeric(s.get("Odd_lay_entrada", 0.0), errors="coerce") or 0.0)
            
            gh, ga = None, None
            hk = ''.join(c for c in home.lower() if c.isalnum())
            ak = ''.join(c for c in away.lower() if c.isalnum())
            if (d_str, hk, ak) in db_scores: gh, ga = db_scores[(d_str, hk, ak)]
            elif (d_str, hk[:5], ak[:5]) in db_scores: gh, ga = db_scores[(d_str, hk[:5], ak[:5])]
            
            if gh is not None and ga is not None and odd_lay > 1.0:
                is_0x1 = (gh == 0 and ga == 1)
                res = "GREEN" if not is_0x1 else "RED"
                pnl = 95.0 if not is_0x1 else -(odd_lay - 1.0) * 100.0
                lay0x1_ops.append({
                    "Data": d_str, "Home": home, "Away": away, "Odd_Lay_0x1": odd_lay,
                    "Método": s.get("Metodo", ""), "Prob": s.get("Prob", ""),
                    "Placar": f"{gh}x{ga}", "Resultado": res, "PnL_R$": pnl
                })

df_2x2 = pd.DataFrame(lay2x2_ops)
df_0x1 = pd.DataFrame(lay0x1_ops)

print("\n" + "="*80, flush=True)
print("=== RESUMO EXECUTIVO DO BACKTEST DE AGOSTO (01 A 20/08) ===", flush=True)
print("="*80, flush=True)

if not df_2x2.empty:
    tot_2x2 = len(df_2x2)
    grn_2x2 = (df_2x2["Resultado"] == "GREEN").sum()
    red_2x2 = (df_2x2["Resultado"] == "RED").sum()
    wr_2x2 = (grn_2x2 / tot_2x2) * 100.0
    pnl_2x2 = df_2x2["PnL_R$"].sum()
    print(f"\n[+] LAY 2X2 QUANT (TETO 20.00):", flush=True)
    print(f"    Total de Operações : {tot_2x2}", flush=True)
    print(f"    Greens             : {grn_2x2} ({wr_2x2:.2f}%)", flush=True)
    print(f"    Reds               : {red_2x2}", flush=True)
    print(f"    Lucro Líquido      : R$ {pnl_2x2:,.2f}", flush=True)

if not df_0x1.empty:
    tot_0x1 = len(df_0x1)
    grn_0x1 = (df_0x1["Resultado"] == "GREEN").sum()
    red_0x1 = (df_0x1["Resultado"] == "RED").sum()
    wr_0x1 = (grn_0x1 / tot_0x1) * 100.0
    pnl_0x1 = df_0x1["PnL_R$"].sum()
    print(f"\n[+] LAY 0X1 (TRADER & RF v2):", flush=True)
    print(f"    Total de Operações : {tot_0x1}", flush=True)
    print(f"    Greens             : {grn_0x1} ({wr_0x1:.2f}%)", flush=True)
    print(f"    Reds               : {red_0x1}", flush=True)
    print(f"    Lucro Líquido      : R$ {pnl_0x1:,.2f}", flush=True)

with pd.ExcelWriter("Backtest_Agosto_Lay2x2_e_Lay0x1_Completo.xlsx") as writer:
    if not df_2x2.empty: df_2x2.to_excel(writer, sheet_name="Lay_2x2_Quant", index=False)
    if not df_0x1.empty: df_0x1.to_excel(writer, sheet_name="Lay_0x1_IA", index=False)

print("\n[+] Planilha detalhada salva: Backtest_Agosto_Lay2x2_e_Lay0x1_Completo.xlsx", flush=True)
