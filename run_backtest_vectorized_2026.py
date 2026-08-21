import os, sys, pandas as pd, numpy as np

print("=== BACKTEST VETORIAL 2026 MÊS A MÊS - CHUNKED ===")

cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
        "Odd_CS_0x0_Lay", "Odd_CS_0x0", "Odd_CS_0x1_Lay", "Odd_CS_0x1",
        "Odd_CS_1x0_Lay", "Odd_CS_1x0", "Odd_CS_2x0_Lay", "Odd_CS_2x0",
        "Odd_CS_0x2_Lay", "Odd_CS_0x2", "Odd_CS_2x2_Lay", "Odd_CS_2x2",
        "Odd_Lay_Draw", "Odd_D_FT", "Odd_Lay_Under25", "Odd_Under25_FT",
        "Odd_H_FT", "Odd_A_FT", "total_xg", "Total_xG"]

chunks = []
for chunk in pd.read_csv("Resultados_2026_Full.csv", usecols=lambda c: c in cols, chunksize=30000, low_memory=False):
    chunk["Date"] = pd.to_datetime(chunk["Date"], errors="coerce")
    c_2026 = chunk[chunk["Date"].dt.year == 2026].dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"])
    if not c_2026.empty:
        chunks.append(c_2026)

df_2026 = pd.concat(chunks, ignore_index=True).sort_values("Date").reset_index(drop=True)
df_2026["Mes"] = df_2026["Date"].dt.strftime("%Y-%m")
df_2026["gh"] = df_2026["Goals_H_FT"].astype(int)
df_2026["ga"] = df_2026["Goals_A_FT"].astype(int)

all_dfs = []

def add_method_trades(df_sub, metodo, odd_col, is_red_series):
    if df_sub.empty: return
    sub = df_sub.copy()
    odd_series = sub[odd_col]
    sub["Metodo"] = metodo
    sub["is_red"] = is_red_series.loc[sub.index]
    sub["PnL_R$"] = np.where(~sub["is_red"], 95.0, -(odd_series - 1.0) * 100.0)
    sub["Resultado"] = np.where(~sub["is_red"], "GREEN", "RED")
    all_dfs.append(sub[["Metodo", "Mes", "Resultado", "PnL_R$"]])

# 1. Lay 0x0 RF v2
odd_00 = df_2026.get("Odd_CS_0x0_Lay", df_2026.get("Odd_CS_0x0", pd.Series(dtype=float)))
m_00 = (odd_00 >= 8.0) & (odd_00 <= 16.0) & (odd_00.notna())
add_method_trades(df_2026[m_00], "Lay 0x0 RF v2", "Odd_CS_0x0_Lay" if "Odd_CS_0x0_Lay" in df_2026.columns else "Odd_CS_0x0", (df_2026["gh"] == 0) & (df_2026["ga"] == 0))

# 2. Lay 0x1 RF v2
odd_01 = df_2026.get("Odd_CS_0x1_Lay", df_2026.get("Odd_CS_0x1", pd.Series(dtype=float)))
m_01 = (odd_01 >= 6.0) & (odd_01 <= 12.0) & (odd_01.notna())
add_method_trades(df_2026[m_01], "Lay 0x1 RF v2", "Odd_CS_0x1_Lay" if "Odd_CS_0x1_Lay" in df_2026.columns else "Odd_CS_0x1", (df_2026["gh"] == 0) & (df_2026["ga"] == 1))

# 3. Lay 1x0 RF v2
odd_10 = df_2026.get("Odd_CS_1x0_Lay", df_2026.get("Odd_CS_1x0", pd.Series(dtype=float)))
m_10 = (odd_10 >= 6.0) & (odd_10 <= 12.0) & (odd_10.notna())
add_method_trades(df_2026[m_10], "Lay 1x0 RF v2", "Odd_CS_1x0_Lay" if "Odd_CS_1x0_Lay" in df_2026.columns else "Odd_CS_1x0", (df_2026["gh"] == 1) & (df_2026["ga"] == 0))

# 4. Lay 2x0 RF v2
odd_20 = df_2026.get("Odd_CS_2x0_Lay", df_2026.get("Odd_CS_2x0", pd.Series(dtype=float)))
m_20 = (odd_20 >= 6.0) & (odd_20 <= 12.0) & (odd_20.notna())
add_method_trades(df_2026[m_20], "Lay 2x0 RF v2", "Odd_CS_2x0_Lay" if "Odd_CS_2x0_Lay" in df_2026.columns else "Odd_CS_2x0", (df_2026["gh"] == 2) & (df_2026["ga"] == 0))

# 5. Lay 0x2 RF v2
odd_02 = df_2026.get("Odd_CS_0x2_Lay", df_2026.get("Odd_CS_0x2", pd.Series(dtype=float)))
m_02 = (odd_02 >= 8.0) & (odd_02 <= 16.0) & (odd_02.notna())
add_method_trades(df_2026[m_02], "Lay 0x2 RF v2", "Odd_CS_0x2_Lay" if "Odd_CS_0x2_Lay" in df_2026.columns else "Odd_CS_0x2", (df_2026["gh"] == 0) & (df_2026["ga"] == 2))

# 6. Lay Draw v2
odd_draw = df_2026.get("Odd_Lay_Draw", df_2026.get("Odd_D_FT", pd.Series(dtype=float)))
m_draw = (odd_draw >= 2.80) & (odd_draw <= 4.20) & (odd_draw.notna())
add_method_trades(df_2026[m_draw], "Lay Draw v2", "Odd_D_FT", df_2026["gh"] == df_2026["ga"])

# 7. Lay Under 2.5 v2
odd_u25 = df_2026.get("Odd_Lay_Under25", df_2026.get("Odd_Under25_FT", pd.Series(dtype=float)))
m_u25 = (odd_u25 >= 1.70) & (odd_u25 <= 2.30) & (odd_u25.notna())
add_method_trades(df_2026[m_u25], "Lay Under 2.5 v2", "Odd_Under25_FT", (df_2026["gh"] + df_2026["ga"]) < 2.5)

# 8. Lay 2x2 Quant
odd_22 = df_2026.get("Odd_CS_2x2_Lay", df_2026.get("Odd_CS_2x2", pd.Series(dtype=float)))
u25_val = df_2026.get("Odd_Under25_FT", pd.Series(dtype=float))
xg_val = df_2026.get("total_xg", df_2026.get("Total_xG", pd.Series(dtype=float)))
m_22 = (odd_22 >= 8.0) & (odd_22 <= 14.0) & (odd_22.notna()) & ((u25_val <= 2.0) | (xg_val <= 2.40) | (df_2026.get("Odd_H_FT", pd.Series(dtype=float)) <= 1.75) | (df_2026.get("Odd_A_FT", pd.Series(dtype=float)) <= 1.75))
add_method_trades(df_2026[m_22], "Lay 2x2 Quant", "Odd_CS_2x2_Lay" if "Odd_CS_2x2_Lay" in df_2026.columns else "Odd_CS_2x2", (df_2026["gh"] == 2) & (df_2026["ga"] == 2))

# 9. Lay 0x1 Agressivo
m_01_ag = (odd_01 >= 6.0) & (odd_01 <= 12.0) & (odd_01.notna()) & (df_2026.get("Odd_H_FT", pd.Series(dtype=float)) <= 1.85)
add_method_trades(df_2026[m_01_ag], "Lay 0x1 Agressivo", "Odd_CS_0x1_Lay" if "Odd_CS_0x1_Lay" in df_2026.columns else "Odd_CS_0x1", (df_2026["gh"] == 0) & (df_2026["ga"] == 1))

df_all = pd.concat(all_dfs, ignore_index=True)
print(f"[+] Total de trades gerados no ano 2026: {len(df_all)}")

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

with open("backtest_2026_relatorio_final.txt", "w", encoding="utf-8") as f:
    f.write("=== TABELA CONSOLIDADA DE P&L POR MÊS E MÉTODO (2026) ===\n\n")
    f.write(pivot_pnl.to_string())
    f.write("\n\n=== TOTAL DE OPERAÇÕES POR MÊS E MÉTODO (2026) ===\n\n")
    f.write(pivot_count.to_string())
    f.write("\n\n=== RESUMO GERAL ACUMULADO POR MÉTODO EM 2026 ===\n\n")
    f.write(pd.DataFrame(summary).to_string(index=False))

print("✅ Relatório escrito com sucesso!")
