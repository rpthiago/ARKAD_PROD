import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== INICIANDO BACKTEST EMPÍRICO 2026 PARA AS DEMAIS ESTRATÉGIAS ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# 1. Base 2026 Completo (01/01/2026 a 20/08/2026)
df_2026 = df_hist[(df_hist["d_str"] >= "2026-01-01") & (df_hist["d_str"] <= "2026-08-20")].copy()
# 2. Base Agosto 2026 (01/08 a 20/08)
df_august = df_hist[(df_hist["d_str"] >= "2026-08-01") & (df_hist["d_str"] <= "2026-08-20")].copy()

def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

def eval_dataset(df_in, period_name):
    df = df_in.copy()
    
    gh_ft = get_num(df, ["Goals_H_FT", "Home_Score", "Goals_H"])
    ga_ft = get_num(df, ["Goals_A_FT", "Away_Score", "Goals_A"])
    gh_ht = get_num(df, ["Goals_H_HT", "Goals_H_1H"])
    ga_ht = get_num(df, ["Goals_A_HT", "Goals_A_1H"])
    
    odd_d = get_num(df, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D"])
    odd_h = get_num(df, ["Odd_H_FT", "Odd_H_FT_Back", "Odd_H"])
    odd_a = get_num(df, ["Odd_A_FT", "Odd_A_FT_Back", "Odd_A"])
    odd_u25 = get_num(df, ["Odd_Under25_FT_Back", "Odd_Under25_FT", "Odd_Under25"])
    odd_1x0 = get_num(df, ["Odd_CS_1x0_Lay", "Odd_CS_1x0"])
    odd_o05ht = get_num(df, ["Odd_Over05_HT_Back", "Odd_Over05_HT", "Odd_Over05_1H"])
    
    res_list = []
    detailed_dfs = {}
    
    # -------------------------------------------------------------
    # 1. LAY DRAW (LAY EMPATE) — Odd D entre 3.0 e 5.50
    # -------------------------------------------------------------
    sub_draw = df[(odd_d >= 3.0) & (odd_d <= 5.50) & gh_ft.notna() & ga_ft.notna()].copy()
    ops_draw = []
    for idx, r in sub_draw.iterrows():
        g_h = int(gh_ft[idx]); g_a = int(ga_ft[idx])
        o = float(odd_d[idx])
        is_draw = (g_h == g_a)
        res = "GREEN" if not is_draw else "RED"
        pnl = 95.0 if not is_draw else -(o - 1.0) * 100.0
        ops_draw.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": o, "Placar": f"{g_h}x{g_a}", "Resultado": res, "PnL": pnl})
    
    df_draw = pd.DataFrame(ops_draw)
    detailed_dfs["Lay_Draw"] = df_draw
    if not df_draw.empty:
        tot = len(df_draw); grn = (df_draw["Resultado"]=="GREEN").sum(); red = (df_draw["Resultado"]=="RED").sum()
        res_list.append({"Estratégia": "Lay Draw (Lay Empate)", "Faixa Odds": "3.00 a 5.50", "Entradas": tot, "Greens": grn, "Reds": red, "Win Rate %": f"{(grn/tot)*100:.2f}%", "Lucro R$": f"R$ {df_draw['PnL'].sum():,.2f}"})

    # -------------------------------------------------------------
    # 2. LAY UNDER 2.5 — Odd Under 2.5 entre 1.50 e 2.50
    # -------------------------------------------------------------
    sub_u25 = df[(odd_u25 >= 1.50) & (odd_u25 <= 2.50) & gh_ft.notna() & ga_ft.notna()].copy()
    ops_u25 = []
    for idx, r in sub_u25.iterrows():
        g_h = int(gh_ft[idx]); g_a = int(ga_ft[idx])
        o = float(odd_u25[idx])
        tot_goals = g_h + g_a
        is_under = (tot_goals <= 2)
        res = "GREEN" if not is_under else "RED" # Lay Under 2.5 ganha se der Over (3+ gols)
        pnl = 95.0 if not is_under else -(o - 1.0) * 100.0
        ops_u25.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": o, "Placar": f"{g_h}x{g_a}", "Resultado": res, "PnL": pnl})
    
    df_u25 = pd.DataFrame(ops_u25)
    detailed_dfs["Lay_Under25"] = df_u25
    if not df_u25.empty:
        tot = len(df_u25); grn = (df_u25["Resultado"]=="GREEN").sum(); red = (df_u25["Resultado"]=="RED").sum()
        res_list.append({"Estratégia": "Lay Under 2.5 (Gols 3+)", "Faixa Odds": "1.50 a 2.50", "Entradas": tot, "Greens": grn, "Reds": red, "Win Rate %": f"{(grn/tot)*100:.2f}%", "Lucro R$": f"R$ {df_u25['PnL'].sum():,.2f}"})

    # -------------------------------------------------------------
    # 3. LAY 1X0 (CORRECT SCORE) — Odd entre 6.0 e 9.5
    # -------------------------------------------------------------
    sub_1x0 = df[(odd_1x0 >= 6.0) & (odd_1x0 <= 9.5) & gh_ft.notna() & ga_ft.notna()].copy()
    ops_1x0 = []
    for idx, r in sub_1x0.iterrows():
        g_h = int(gh_ft[idx]); g_a = int(ga_ft[idx])
        o = float(odd_1x0[idx])
        is_1x0 = (g_h == 1 and g_a == 0)
        res = "GREEN" if not is_1x0 else "RED"
        pnl = 95.0 if not is_1x0 else -(o - 1.0) * 100.0
        ops_1x0.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": o, "Placar": f"{g_h}x{g_a}", "Resultado": res, "PnL": pnl})
    
    df_1x0 = pd.DataFrame(ops_1x0)
    detailed_dfs["Lay_1x0"] = df_1x0
    if not df_1x0.empty:
        tot = len(df_1x0); grn = (df_1x0["Resultado"]=="GREEN").sum(); red = (df_1x0["Resultado"]=="RED").sum()
        res_list.append({"Estratégia": "Lay 1x0 (Correct Score)", "Faixa Odds": "6.00 a 9.50", "Entradas": tot, "Greens": grn, "Reds": red, "Win Rate %": f"{(grn/tot)*100:.2f}%", "Lucro R$": f"R$ {df_1x0['PnL'].sum():,.2f}"})

    # -------------------------------------------------------------
    # 4. LAY HOME TRADER — Odd H entre 1.80 e 2.80
    # -------------------------------------------------------------
    sub_lh = df[(odd_h >= 1.80) & (odd_h <= 2.80) & gh_ft.notna() & ga_ft.notna()].copy()
    ops_lh = []
    for idx, r in sub_lh.iterrows():
        g_h = int(gh_ft[idx]); g_a = int(ga_ft[idx])
        o = float(odd_h[idx])
        is_h_win = (g_h > g_a)
        res = "GREEN" if not is_h_win else "RED"
        pnl = 95.0 if not is_h_win else -(o - 1.0) * 100.0
        ops_lh.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": o, "Placar": f"{g_h}x{g_a}", "Resultado": res, "PnL": pnl})
    
    df_lh = pd.DataFrame(ops_lh)
    detailed_dfs["Lay_Home"] = df_lh
    if not df_lh.empty:
        tot = len(df_lh); grn = (df_lh["Resultado"]=="GREEN").sum(); red = (df_lh["Resultado"]=="RED").sum()
        res_list.append({"Estratégia": "Lay Home Trader", "Faixa Odds": "1.80 a 2.80", "Entradas": tot, "Greens": grn, "Reds": red, "Win Rate %": f"{(grn/tot)*100:.2f}%", "Lucro R$": f"R$ {df_lh['PnL'].sum():,.2f}"})

    # -------------------------------------------------------------
    # 5. OVER 0.5 HT (BACK) — Odd entre 1.30 e 1.70
    # -------------------------------------------------------------
    if odd_o05ht.notna().sum() > 0:
        sub_o05 = df[(odd_o05ht >= 1.30) & (odd_o05ht <= 1.70) & gh_ht.notna() & ga_ht.notna()].copy()
        ops_o05 = []
        for idx, r in sub_o05.iterrows():
            g_h_ht = int(gh_ht[idx]); g_a_ht = int(ga_ht[idx])
            o = float(odd_o05ht[idx])
            has_goal_ht = (g_h_ht + g_a_ht >= 1)
            res = "GREEN" if has_goal_ht else "RED"
            pnl = (o - 1.0) * 100.0 * 0.95 if has_goal_ht else -100.0
            ops_o05.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": o, "Placar_HT": f"{g_h_ht}x{g_a_ht}", "Resultado": res, "PnL": pnl})
        
        df_o05 = pd.DataFrame(ops_o05)
        detailed_dfs["Over_05_HT"] = df_o05
        if not df_o05.empty:
            tot = len(df_o05); grn = (df_o05["Resultado"]=="GREEN").sum(); red = (df_o05["Resultado"]=="RED").sum()
            res_list.append({"Estratégia": "Over 0.5 HT (Back)", "Faixa Odds": "1.30 a 1.70", "Entradas": tot, "Greens": grn, "Reds": red, "Win Rate %": f"{(grn/tot)*100:.2f}%", "Lucro R$": f"R$ {df_o05['PnL'].sum():,.2f}"})

    return pd.DataFrame(res_list), detailed_dfs

# Executar Agosto e Ano 2026
df_res_aug, _ = eval_dataset(df_august, "Agosto_2026")
df_res_2026, detailed_2026 = eval_dataset(df_2026, "Ano_2026_Completo")

print("\n" + "="*90, flush=True)
print("=== 1. RELATÓRIO OFICIAL DE AGOSTO DE 2026 (01 A 20/08) ===", flush=True)
print("="*90, flush=True)
print(df_res_aug.to_string(index=False), flush=True)

print("\n" + "="*90, flush=True)
print("=== 2. RELATÓRIO OFICIAL DO ANO DE 2026 COMPLETO (JANEIRO A AGOSTO) ===", flush=True)
print("="*90, flush=True)
print(df_res_2026.to_string(index=False), flush=True)

with pd.ExcelWriter("Backtest_2026_Metodos_Extras_Completo.xlsx") as writer:
    df_res_aug.to_excel(writer, sheet_name="Resumo_Agosto_2026", index=False)
    df_res_2026.to_excel(writer, sheet_name="Resumo_Ano_2026", index=False)
    for k, v in detailed_2026.items():
        if not v.empty:
            v.to_excel(writer, sheet_name=k[:31], index=False)

print("\n[+] Planilha detalhada salva com sucesso: Backtest_2026_Metodos_Extras_Completo.xlsx", flush=True)
