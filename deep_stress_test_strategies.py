import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== INICIANDO AUDITORIA E STRESS-TEST AVANÇADO (2025 E 2026) ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

# 1. Preparar colunas gerais
df_hist["gh_ft"] = get_num(df_hist, ["Goals_H_FT", "Home_Score", "Goals_H"])
df_hist["ga_ft"] = get_num(df_hist, ["Goals_A_FT", "Away_Score", "Goals_A"])
df_hist["odd_d"] = get_num(df_hist, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D"])
df_hist["odd_h"] = get_num(df_hist, ["Odd_H_FT", "Odd_H_FT_Back", "Odd_H"])
df_hist["odd_a"] = get_num(df_hist, ["Odd_A_FT", "Odd_A_FT_Back", "Odd_A"])
df_hist["odd_u25"] = get_num(df_hist, ["Odd_Under25_FT_Back", "Odd_Under25_FT", "Odd_Under25"])
df_hist["odd_1x0"] = get_num(df_hist, ["Odd_CS_1x0_Lay", "Odd_CS_1x0"])
df_hist["odd_2x0"] = get_num(df_hist, ["Odd_CS_2x0_Lay", "Odd_CS_2x0"])
df_hist["odd_0x2"] = get_num(df_hist, ["Odd_CS_0x2_Lay", "Odd_CS_0x2"])
df_hist["odd_0x0"] = get_num(df_hist, ["Odd_CS_0x0_Lay", "Odd_CS_0x0"])

def run_ops(df_sub, strat_type):
    ops = []
    for idx, r in df_sub.iterrows():
        gh = r["gh_ft"]; ga = r["ga_ft"]
        if pd.isna(gh) or pd.isna(ga): continue
        gh_i = int(gh); ga_i = int(ga)
        
        if strat_type == "lay_draw":
            odd = float(r["odd_d"])
            is_draw = (gh_i == ga_i)
            res = "GREEN" if not is_draw else "RED"
            pnl = 95.0 if not is_draw else -(odd - 1.0) * 100.0
            
        elif strat_type == "lay_1x0":
            odd = float(r["odd_1x0"])
            is_1x0 = (gh_i == 1 and ga_i == 0)
            res = "GREEN" if not is_1x0 else "RED"
            pnl = 95.0 if not is_1x0 else -(odd - 1.0) * 100.0
            
        elif strat_type == "lay_u25":
            odd = float(r["odd_u25"])
            is_under = (gh_i + ga_i <= 2)
            res = "GREEN" if not is_under else "RED"
            pnl = 95.0 if not is_under else -(odd - 1.0) * 100.0
            
        elif strat_type == "lay_home":
            odd = float(r["odd_h"])
            is_h_win = (gh_i > ga_i)
            res = "GREEN" if not is_h_win else "RED"
            pnl = 95.0 if not is_h_win else -(odd - 1.0) * 100.0
            
        ops.append({
            "Date": r["d_str"], "Mes": str(r["d_str"])[:7], "Ano": str(r["d_str"])[:4],
            "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd,
            "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl
        })
    return pd.DataFrame(ops)

# Filtros por estratégia
sub_draw = df_hist[(df_hist["odd_d"] >= 3.0) & (df_hist["odd_d"] <= 5.50) & df_hist["gh_ft"].notna()].copy()
sub_1x0 = df_hist[(df_hist["odd_1x0"] >= 6.0) & (df_hist["odd_1x0"] <= 9.50) & df_hist["gh_ft"].notna()].copy()
sub_u25 = df_hist[(df_hist["odd_u25"] >= 1.50) & (df_hist["odd_u25"] <= 2.50) & df_hist["gh_ft"].notna()].copy()
sub_home = df_hist[(df_hist["odd_h"] >= 1.80) & (df_hist["odd_h"] <= 2.80) & df_hist["gh_ft"].notna()].copy()

df_ops_draw = run_ops(sub_draw, "lay_draw")
df_ops_1x0 = run_ops(sub_1x0, "lay_1x0")
df_ops_u25 = run_ops(sub_u25, "lay_u25")
df_ops_home = run_ops(sub_home, "lay_home")

def calculate_stress_metrics(df_ops, name):
    if df_ops.empty: return {}
    
    # 2026
    df_26 = df_ops[(df_ops["Date"] >= "2026-01-01") & (df_ops["Date"] <= "2026-08-20")].copy()
    # 2025
    df_25 = df_ops[(df_ops["Date"] >= "2025-01-01") & (df_ops["Date"] <= "2025-12-31")].copy()
    
    def get_stats(df):
        if df.empty: return 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0
        tot = len(df)
        grn = (df["Resultado"] == "GREEN").sum()
        red = (df["Resultado"] == "RED").sum()
        wr = (grn / tot) * 100.0
        pnl = df["PnL"].sum()
        
        # Max Drawdown
        cum = df["PnL"].cumsum()
        peak = cum.cummax()
        dd = peak - cum
        max_dd = dd.max()
        
        # Profit Factor
        lucro_bruto = df[df["PnL"] > 0]["PnL"].sum()
        perda_bruta = abs(df[df["PnL"] < 0]["PnL"].sum())
        pf = (lucro_bruto / perda_bruta) if perda_bruta > 0 else np.nan
        
        # Max Consec Reds
        is_red = (df["Resultado"] == "RED").astype(int)
        consec_reds = (is_red * (is_red.groupby((is_red != is_red.shift()).cumsum()).cumcount() + 1)).max()
        
        return tot, grn, red, wr, pnl, max_dd, pf, consec_reds
        
    tot_26, grn_26, red_26, wr_26, pnl_26, dd_26, pf_26, cred_26 = get_stats(df_26)
    tot_25, grn_25, red_25, wr_25, pnl_25, dd_25, pf_25, cred_25 = get_stats(df_25)
    
    return {
        "Estratégia": name,
        "Entradas 2026": tot_26,
        "Win Rate 2026": f"{wr_26:.2f}%",
        "Lucro 2026": f"R$ {pnl_26:,.2f}",
        "Profit Factor 2026": f"{pf_26:.2f}",
        "Max Drawdown 2026": f"R$ {dd_26:,.2f}",
        "Pior Sequência Reds 2026": cred_26,
        "Entradas 2025 (Ano Anterior)": tot_25,
        "Win Rate 2025": f"{wr_25:.2f}%",
        "Lucro 2025": f"R$ {pnl_25:,.2f}",
        "Profit Factor 2025": f"{pf_25:.2f}"
    }

stress_summary = [
    calculate_stress_metrics(df_ops_1x0, "Lay 1x0 (Correct Score)"),
    calculate_stress_metrics(df_ops_draw, "Lay Draw (Lay Empate)"),
    calculate_stress_metrics(df_ops_u25, "Lay Under 2.5 (Gols 3+)"),
    calculate_stress_metrics(df_ops_home, "Lay Home Trader")
]

df_stress = pd.DataFrame(stress_summary)
print("\n" + "="*95, flush=True)
print("=== RELATÓRIO DE ROBUSTEZ E STRESS-TEST (2025 E 2026) ===", flush=True)
print("="*95, flush=True)
print(df_stress.to_string(index=False), flush=True)

# Consistência Mês a Mês em 2026
print("\n" + "="*95, flush=True)
print("=== CONSISTÊNCIA MÊS A MÊS EM 2026 (JANEIRO A AGOSTO) ===", flush=True)
print("="*95, flush=True)

for name, df_ops in [("Lay 1x0", df_ops_1x0), ("Lay Draw", df_ops_draw), ("Lay Under 2.5", df_ops_u25), ("Lay Home", df_ops_home)]:
    df_26 = df_ops[(df_ops["Date"] >= "2026-01-01") & (df_ops["Date"] <= "2026-08-20")].copy()
    m_agg = df_26.groupby("Mes").apply(lambda g: pd.Series({
        "Entradas": len(g),
        "Greens": (g["Resultado"]=="GREEN").sum(),
        "Reds": (g["Resultado"]=="RED").sum(),
        "Win Rate": f"{(g['Resultado']=='GREEN').mean()*100:.2f}%",
        "Lucro R$": f"R$ {g['PnL'].sum():,.2f}"
    })).reset_index()
    print(f"\n--- {name} ---")
    print(m_agg.to_string(index=False))

with pd.ExcelWriter("Relatorio_Stress_Test_2025_2026.xlsx") as writer:
    df_stress.to_excel(writer, sheet_name="Resumo_Robustez", index=False)

print("\n[+] Relatório de Stress-Test salvo com sucesso: Relatorio_Stress_Test_2025_2026.xlsx", flush=True)
