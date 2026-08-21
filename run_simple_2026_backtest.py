import os, sys, pandas as pd, numpy as np

print("Starting 2026 Backtest with explicit usecols...")
target_cols = [
    "Date", "Goals_H_FT", "Goals_A_FT", "Odd_CS_0x0_Lay", "Odd_CS_0x1_Lay", 
    "Odd_CS_1x0_Lay", "Odd_CS_2x0_Lay", "Odd_CS_0x2_Lay", "Odd_CS_2x2_Lay", 
    "Odd_D_FT", "Odd_Under25_FT", "Odd_H_FT", "Odd_A_FT"
]

def col_filter(c): return c in target_cols

df = pd.read_csv("b365_base_lean.csv", usecols=col_filter, low_memory=False)

df["Date_str"] = df["Date"].astype(str)
df_2026 = df[df["Date_str"].str.startswith("2026-")].copy()
df_2026["Mes"] = df_2026["Date_str"].str.slice(0, 7)

df_2026["gh"] = pd.to_numeric(df_2026["Goals_H_FT"], errors="coerce").fillna(-1).astype(int)
df_2026["ga"] = pd.to_numeric(df_2026["Goals_A_FT"], errors="coerce").fillna(-1).astype(int)
df_2026 = df_2026[(df_2026["gh"] >= 0) & (df_2026["ga"] >= 0)].reset_index(drop=True)

all_trades = []

def calc_method(metodo, odd_col, is_red_mask, min_odd, max_odd, cond_extra=None):
    if odd_col not in df_2026.columns: return
    odd_series = pd.to_numeric(df_2026[odd_col], errors="coerce")
    mask = (odd_series >= min_odd) & (odd_series <= max_odd) & odd_series.notna()
    if cond_extra is not None:
        mask = mask & cond_extra
    
    sub = df_2026[mask].copy()
    if sub.empty: return
    
    sub["Odd_Exec"] = odd_series.loc[sub.index]
    sub["is_red"] = is_red_mask.loc[sub.index]
    sub["Resultado"] = np.where(~sub["is_red"], "GREEN", "RED")
    sub["PnL_R$"] = np.where(~sub["is_red"], 95.0, -(sub["Odd_Exec"] - 1.0) * 100.0)
    sub["Metodo"] = metodo
    all_trades.append(sub[["Metodo", "Mes", "Resultado", "PnL_R$"]])

# 1. Lay 0x0 RF v2
calc_method("Lay 0x0 RF v2", "Odd_CS_0x0_Lay", (df_2026["gh"] == 0) & (df_2026["ga"] == 0), 8.0, 16.0)

# 2. Lay 0x1 RF v2
calc_method("Lay 0x1 RF v2", "Odd_CS_0x1_Lay", (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0)

# 3. Lay 1x0 RF v2
calc_method("Lay 1x0 RF v2", "Odd_CS_1x0_Lay", (df_2026["gh"] == 1) & (df_2026["ga"] == 0), 6.0, 12.0)

# 4. Lay 2x0 RF v2
calc_method("Lay 2x0 RF v2", "Odd_CS_2x0_Lay", (df_2026["gh"] == 2) & (df_2026["ga"] == 0), 6.0, 12.0)

# 5. Lay 0x2 RF v2
calc_method("Lay 0x2 RF v2", "Odd_CS_0x2_Lay", (df_2026["gh"] == 0) & (df_2026["ga"] == 2), 8.0, 16.0)

# 6. Lay Draw v2
calc_method("Lay Draw v2", "Odd_D_FT", df_2026["gh"] == df_2026["ga"], 2.80, 4.20)

# 7. Lay Under 2.5 v2
calc_method("Lay Under 2.5 v2", "Odd_Under25_FT", (df_2026["gh"] + df_2026["ga"]) < 2.5, 1.70, 2.30)

# 8. Lay 2x2 Quant
u25_col = pd.to_numeric(df_2026.get("Odd_Under25_FT"), errors="coerce")
xg_col = pd.to_numeric(df_2026.get("total_xg", df_2026.get("Total_xG")), errors="coerce")
h_col = pd.to_numeric(df_2026.get("Odd_H_FT"), errors="coerce")
a_col = pd.to_numeric(df_2026.get("Odd_A_FT"), errors="coerce")
c_22 = (u25_col <= 2.00) | (xg_col <= 2.40) | (h_col <= 1.75) | (a_col <= 1.75)
calc_method("Lay 2x2 Quant", "Odd_CS_2x2_Lay", (df_2026["gh"] == 2) & (df_2026["ga"] == 2), 8.0, 14.0, c_22)

# 9. Lay 0x1 Agressivo
c_01_ag = h_col <= 1.85
calc_method("Lay 0x1 Agressivo", "Odd_CS_0x1_Lay", (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0, c_01_ag)

df_all = pd.concat(all_trades, ignore_index=True)
pivot_pnl = pd.pivot_table(df_all, values="PnL_R$", index="Mes", columns="Metodo", aggfunc="sum", fill_value=0.0)
pivot_count = pd.pivot_table(df_all, values="Resultado", index="Mes", columns="Metodo", aggfunc="count", fill_value=0)

summary = []
for met in sorted(df_all["Metodo"].unique()):
    sub = df_all[df_all["Metodo"] == met]
    tot = len(sub)
    grn = (sub["Resultado"] == "GREEN").sum()
    red = (sub["Resultado"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = sub["PnL_R$"].sum()
    summary.append({
        "Método": met,
        "Ops": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate": f"{wr:.2f}%",
        "P&L Total (R$)": f"R$ {pnl:,.2f}"
    })

res_txt = "=== TABELA CONSOLIDADA DE P&L POR MÊS E MÉTODO (2026) ===\n\n"
res_txt += pivot_pnl.to_string() + "\n\n"
res_txt += "=== TOTAL DE OPERAÇÕES POR MÊS E MÉTODO (2026) ===\n\n"
res_txt += pivot_count.to_string() + "\n\n"
res_txt += "=== RESUMO GERAL ACUMULADO POR MÉTODO EM 2026 ===\n\n"
res_txt += pd.DataFrame(summary).to_string(index=False)

with open("backtest_2026_final_report.txt", "w", encoding="utf-8") as f:
    f.write(res_txt)

print("DONE")
