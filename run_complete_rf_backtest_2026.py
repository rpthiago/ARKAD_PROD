import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== EXECUTANDO BACKTEST DOS MODELOS RF V2 (LAY 2X0, LAY 0X2, LAY 0X0, LAY 1X0) ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# Funcao auxiliar para pegar odd com fallback
def get_col_float(df, col_names):
    for c in col_names:
        if c in df.columns and df[c].notna().any():
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

# 1. Backtest Agosto 2026 (01 a 20/08)
df_august = df_hist[(df_hist["d_str"] >= "2026-08-01") & (df_hist["d_str"] <= "2026-08-20")].copy()
# 2. Backtest Ano 2026 Completo (01/01 a 20/08)
df_2026 = df_hist[(df_hist["d_str"] >= "2026-01-01") & (df_hist["d_str"] <= "2026-08-20")].copy()

def run_strategy_backtest(df_input, name, target_h, target_a, odd_cols, u25_max=None, odd_min=6.0, odd_max=14.0):
    df = df_input.copy()
    df["Odd_Target"] = get_col_float(df, odd_cols)
    df["Odd_U25"] = get_col_float(df, ["Odd_Under25_FT_Back", "Odd_Under25_FT", "Odd_Under25"])
    
    cond = (df["Odd_Target"] >= odd_min) & (df["Odd_Target"] <= odd_max)
    if u25_max is not None:
        cond = cond & (df["Odd_U25"] > 0) & (df["Odd_U25"] <= u25_max)
        
    sub = df[cond].copy()
    
    ops = []
    for idx, r in sub.iterrows():
        gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
        ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
        if pd.notna(gh) and pd.notna(ga):
            gh_i = int(float(gh)); ga_i = int(float(ga))
            odd = float(r["Odd_Target"])
            is_hit = (gh_i == target_h and ga_i == target_a)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({
                "Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"),
                "Odd_Lay": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl
            })
            
    df_ops = pd.DataFrame(ops)
    if not df_ops.empty:
        tot = len(df_ops)
        grn = (df_ops["Resultado"] == "GREEN").sum()
        red = (df_ops["Resultado"] == "RED").sum()
        wr = (grn / tot) * 100.0
        pnl_val = df_ops["PnL"].sum()
        return {
            "Estratégia": name,
            "Total Entradas": tot,
            "Greens": grn,
            "Reds": red,
            "Win Rate %": f"{wr:.2f}%",
            "Lucro Acumulado R$": f"R$ {pnl_val:,.2f}"
        }, df_ops
    else:
        return {
            "Estratégia": name, "Total Entradas": 0, "Greens": 0, "Reds": 0, "Win Rate %": "0.00%", "Lucro Acumulado R$": "R$ 0.00"
        }, pd.DataFrame()

configs = [
    ("Lay 2x0 RF v2 (6.0 a 12.0)", 2, 0, ["Odd_CS_2x0_Lay", "Odd_CS_2x0"], 2.10, 6.0, 12.0),
    ("Lay 0x2 RF v2 (6.0 a 12.0)", 0, 2, ["Odd_CS_0x2_Lay", "Odd_CS_0x2"], 2.10, 6.0, 12.0),
    ("Lay 0x0 RF v2 (6.0 a 14.0)", 0, 0, ["Odd_CS_0x0_Lay", "Odd_CS_0x0"], 2.00, 6.0, 14.0),
    ("Lay 1x0 RF v2 (6.0 a 9.5)", 1, 0, ["Odd_CS_1x0_Lay", "Odd_CS_1x0"], None, 6.0, 9.5),
]

# A. Relatorio Agosto 2026
sum_aug = []
for name, th, ta, cols, u25, omin, omax in configs:
    st_res, _ = run_strategy_backtest(df_august, name, th, ta, cols, u25, omin, omax)
    sum_aug.append(st_res)

df_sum_aug = pd.DataFrame(sum_aug)
print("\n" + "="*85, flush=True)
print("=== 1. RELATÓRIO OFICIAL DE AGOSTO DE 2026 (01 A 20/08) ===", flush=True)
print("="*85, flush=True)
print(df_sum_aug.to_string(index=False), flush=True)

# B. Relatorio Ano 2026 Completo
sum_2026 = []
detailed_2026 = {}
for name, th, ta, cols, u25, omin, omax in configs:
    st_res, df_d = run_strategy_backtest(df_2026, name, th, ta, cols, u25, omin, omax)
    sum_2026.append(st_res)
    detailed_2026[name] = df_d

df_sum_2026 = pd.DataFrame(sum_2026)
print("\n" + "="*85, flush=True)
print("=== 2. RELATÓRIO OFICIAL DO ANO DE 2026 COMPLETO (JANEIRO A AGOSTO) ===", flush=True)
print("="*85, flush=True)
print(df_sum_2026.to_string(index=False), flush=True)

with pd.ExcelWriter("Backtest_2026_Metodos_RF_v2_Consolidado.xlsx") as writer:
    df_sum_aug.to_excel(writer, sheet_name="Resumo_Agosto_2026", index=False)
    df_sum_2026.to_excel(writer, sheet_name="Resumo_Ano_2026", index=False)
    for k, v in detailed_2026.items():
        if not v.empty:
            sheet_title = k.split("(")[0].strip().replace(" ", "_")
            v.to_excel(writer, sheet_name=sheet_title[:31], index=False)

print("\n[+] Planilha consolidada salva com sucesso: Backtest_2026_Metodos_RF_v2_Consolidado.xlsx", flush=True)
