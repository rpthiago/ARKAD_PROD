import os, sys, pandas as pd, numpy as np

print("=== COMPUTANDO BACKTEST 2026 COMPLETO (TODOS OS MÉTODOS) ===", flush=True)

fpath = "b365_base_lean.csv"

target_cols = [
    "Date", "Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_A_FT",
    "Odd_CS_0x0_Lay", "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay", "Odd_CS_2x0_Lay", "Odd_CS_0x2_Lay", "Odd_CS_2x2_Lay",
    "Odd_CS_0x0", "Odd_CS_0x1", "Odd_CS_1x0", "Odd_CS_2x0", "Odd_CS_0x2", "Odd_CS_2x2",
    "Odd_Under25_FT", "Odd_Lay_Draw", "Odd_D_FT"
]

header = pd.read_csv(fpath, nrows=0).columns
avail_cols = [c for c in target_cols if c in header]

df = pd.read_csv(fpath, usecols=avail_cols, low_memory=False)

df["Date_str"] = df["Date"].astype(str)
df_2026 = df[df["Date_str"].str.startswith("2026-")].copy()
df_2026["Mes_Key"] = df_2026["Date_str"].str.slice(0, 7)

meses_nome = {
    "2026-01": "01/2026 (Jan)",
    "2026-02": "02/2026 (Fev)",
    "2026-03": "03/2026 (Mar)",
    "2026-04": "04/2026 (Abr)",
    "2026-05": "05/2026 (Mai)",
    "2026-06": "06/2026 (Jun)",
    "2026-07": "07/2026 (Jul)",
    "2026-08": "08/2026 (Ago)"
}
df_2026["Mes"] = df_2026["Mes_Key"].map(meses_nome).fillna(df_2026["Mes_Key"])

df_2026["gh"] = pd.to_numeric(df_2026["Goals_H_FT"], errors="coerce").fillna(-1).astype(int)
df_2026["ga"] = pd.to_numeric(df_2026["Goals_A_FT"], errors="coerce").fillna(-1).astype(int)
df_2026 = df_2026[(df_2026["gh"] >= 0) & (df_2026["ga"] >= 0)].reset_index(drop=True)

print(f"[+] Total de jogos 2026 processados: {len(df_2026)}", flush=True)

all_trades = []

def calc_method(metodo, odd_cols_possible, is_red_mask, min_odd, max_odd, cond_extra=None):
    odd_col = None
    for c in odd_cols_possible:
        if c in df_2026.columns:
            odd_col = c
            break
    if not odd_col: return

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
calc_method("Lay 0x0 RF v2", ["Odd_CS_0x0_Lay", "Odd_CS_0x0"], (df_2026["gh"] == 0) & (df_2026["ga"] == 0), 8.0, 16.0)

# 2. Lay 0x1 RF v2
calc_method("Lay 0x1 RF v2", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0)

# 3. Lay 1x0 RF v2
calc_method("Lay 1x0 RF v2", ["Odd_CS_1x0_Lay", "Odd_CS_1x0"], (df_2026["gh"] == 1) & (df_2026["ga"] == 0), 6.0, 12.0)

# 4. Lay 2x0 RF v2
calc_method("Lay 2x0 RF v2", ["Odd_CS_2x0_Lay", "Odd_CS_2x0"], (df_2026["gh"] == 2) & (df_2026["ga"] == 0), 6.0, 12.0)

# 5. Lay 0x2 RF v2
calc_method("Lay 0x2 RF v2", ["Odd_CS_0x2_Lay", "Odd_CS_0x2"], (df_2026["gh"] == 0) & (df_2026["ga"] == 2), 8.0, 16.0)

# 6. Lay Draw v2
calc_method("Lay Draw v2", ["Odd_Lay_Draw", "Odd_D_FT"], df_2026["gh"] == df_2026["ga"], 2.80, 4.20)

# 7. Lay Under 2.5 v2
calc_method("Lay Under 2.5 v2", ["Odd_Lay_Under25", "Odd_Under25_FT"], (df_2026["gh"] + df_2026["ga"]) < 2.5, 1.70, 2.30)

# 8. Lay 2x2 Quant
u25_col = pd.to_numeric(df_2026.get("Odd_Under25_FT"), errors="coerce")
xg_col = pd.to_numeric(df_2026.get("total_xg", df_2026.get("Total_xG")), errors="coerce")
h_col = pd.to_numeric(df_2026.get("Odd_H_FT"), errors="coerce")
a_col = pd.to_numeric(df_2026.get("Odd_A_FT"), errors="coerce")
c_22 = (u25_col <= 2.00) | (xg_col <= 2.40) | (h_col <= 1.75) | (a_col <= 1.75)
calc_method("Lay 2x2 Quant", ["Odd_CS_2x2_Lay", "Odd_CS_2x2"], (df_2026["gh"] == 2) & (df_2026["ga"] == 2), 8.0, 14.0, c_22)

# 9. Lay 0x1 Agressivo
c_01_ag = h_col <= 1.85
calc_method("Lay 0x1 Agressivo", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0, c_01_ag)

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

print("\n" + "="*80, flush=True)
print("=== TABELA CONSOLIDADA DE P&L POR MÊS E MÉTODO (2026) ===", flush=True)
print("="*80, flush=True)
print(pivot_pnl.to_string(), flush=True)

print("\n" + "="*80, flush=True)
print("=== TOTAL DE OPERAÇÕES POR MÊS E MÉTODO (2026) ===", flush=True)
print("="*80, flush=True)
print(pivot_count.to_string(), flush=True)

print("\n" + "="*80, flush=True)
print("=== RESUMO GERAL ACUMULADO POR MÉTODO EM 2026 ===", flush=True)
print("="*80, flush=True)
df_sum_show = pd.DataFrame(summary)
print(df_sum_show.to_string(index=False), flush=True)

art_dir = r"C:\Users\thiag\.gemini\antigravity\brain\95f807fc-aeff-419c-bec7-34d43b90cd11"
art_file = os.path.join(art_dir, "backtest_2026_todos_os_metodos.md")

def df_to_md_table(df_in, include_index=True):
    lines = []
    if include_index:
        cols = [df_in.index.name or "Mes"] + list(df_in.columns)
    else:
        cols = list(df_in.columns)
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    
    for idx, row in df_in.iterrows():
        if include_index:
            row_vals = [str(idx)] + [f"R$ {v:,.2f}" if isinstance(v, (int, float)) else str(v) for v in row.values]
        else:
            row_vals = [f"R$ {v:,.2f}" if isinstance(v, (int, float)) and "Rate" not in str(col) else str(v) for col, v in zip(df_in.columns, row.values)]
        lines.append("| " + " | ".join(row_vals) + " |")
    return "\n".join(lines)

md = "# 📊 Backtest Completo 2026 - Todos os Métodos Mês a Mês\n\n"
md += "Relatório oficial contendo a performance acumulada e mês a mês de **todos os métodos de 2026** no projeto **ARKAD_PROD**.\n\n"
md += "> ⚠️ **Gestão de Referência:** Stake Fixa de **R$ 100,00** por entrada (com comissão padrão Betfair de 5%).\n\n"
md += "### 💵 Tabela Consolidada de P&L Mensal (R$)\n\n"
md += df_to_md_table(pivot_pnl, include_index=True) + "\n\n"
md += "### 🔢 Volume de Operações Mensais (Entradas)\n\n"
md += df_to_md_table(pivot_count, include_index=True) + "\n\n"
md += "### 🏆 Resumo Geral Acumulado (Ano 2026)\n\n"
md += df_to_md_table(df_sum_show, include_index=False) + "\n"

with open(art_file, "w", encoding="utf-8") as f:
    f.write(md)

with open("backtest_2026_todos_os_metodos.md", "w", encoding="utf-8") as f:
    f.write(md)

print("\n[+] Relatório salvo no artefato:", art_file, flush=True)
