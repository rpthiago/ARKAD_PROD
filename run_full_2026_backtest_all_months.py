import os, sys, pandas as pd, numpy as np

print("=== INICIANDO PROCESSAMENTO ULTRA RÁPIDO DE BACKTEST 2026 ===", flush=True)

fpath = "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
if not os.path.exists(fpath):
    fpath = "b365_base_lean.csv"

target_cols = [
    "Date", "Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_A_FT", "Odd_H", "Odd_A", "Odd_H_FT_Back", "Odd_A_FT_Back",
    "Odd_CS_0x0_Lay", "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay", "Odd_CS_2x0_Lay", "Odd_CS_0x2_Lay", "Odd_CS_2x2_Lay",
    "Odd_CS_0x0", "Odd_CS_0x1", "Odd_CS_1x0", "Odd_CS_2x0", "Odd_CS_0x2", "Odd_CS_2x2",
    "Odd_Under25_FT", "Odd_Under25_FT_Back", "Odd_Under25", "Odd_Lay_Draw", "Odd_D_FT", "total_xg", "Total_xG"
]

header = pd.read_csv(fpath, nrows=0).columns
avail = [c for c in target_cols if c in header]

df = pd.read_csv(fpath, usecols=avail, low_memory=False)

df["Date_str"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df_2026 = df[df["Date_str"].str.startswith("2026-", na=False)].copy()
df_2026["Mes_Key"] = df_2026["Date_str"].str.slice(0, 7)

meses_map = {
    "2026-01": "01/2026 (Jan)",
    "2026-02": "02/2026 (Fev)",
    "2026-03": "03/2026 (Mar)",
    "2026-04": "04/2026 (Abr)",
    "2026-05": "05/2026 (Mai)",
    "2026-06": "06/2026 (Jun)",
    "2026-07": "07/2026 (Jul)",
    "2026-08": "08/2026 (Ago)"
}
df_2026["Mes"] = df_2026["Mes_Key"].map(meses_map).fillna(df_2026["Mes_Key"])

gh_col = [c for c in df_2026.columns if 'goals_h_ft' in c.lower() or 'gols mandante' in c.lower()][0]
ga_col = [c for c in df_2026.columns if 'goals_a_ft' in c.lower() or 'gols visitante' in c.lower()][0]

df_2026["gh"] = pd.to_numeric(df_2026[gh_col], errors="coerce").fillna(-1).astype(int)
df_2026["ga"] = pd.to_numeric(df_2026[ga_col], errors="coerce").fillna(-1).astype(int)
df_2026 = df_2026[(df_2026["gh"] >= 0) & (df_2026["ga"] >= 0)].reset_index(drop=True)

print(f"[+] Total de jogos 2026 em FRESH: {len(df_2026)}", flush=True)

all_trades = []

def calc_metodo(label, odd_col_candidates, is_red_series, min_odd, max_odd, extra_cond=None):
    odd_col = None
    for c in odd_col_candidates:
        if c in df_2026.columns:
            odd_col = c
            break
    if not odd_col: return
    
    odds = pd.to_numeric(df_2026[odd_col], errors="coerce")
    mask = (odds >= min_odd) & (odds <= max_odd) & odds.notna()
    if extra_cond is not None:
        mask = mask & extra_cond
        
    sub = df_2026[mask].copy()
    if sub.empty: return
    
    sub["Odd_Exec"] = odds.loc[sub.index]
    sub["is_red"] = is_red_series.loc[sub.index]
    sub["Resultado"] = np.where(~sub["is_red"], "GREEN", "RED")
    sub["PnL_R$"] = np.where(~sub["is_red"], 95.0, -(sub["Odd_Exec"] - 1.0) * 100.0)
    sub["Metodo"] = label
    all_trades.append(sub[["Metodo", "Mes", "Resultado", "PnL_R$"]])

o_u25 = pd.to_numeric(df_2026.get("Odd_Under25_FT_Back", df_2026.get("Odd_Under25_FT", df_2026.get("Odd_Under25"))), errors="coerce")
o_xg = pd.to_numeric(df_2026.get("total_xg", df_2026.get("Total_xG")), errors="coerce")
o_h = pd.to_numeric(df_2026.get("Odd_H_FT_Back", df_2026.get("Odd_H_FT", df_2026.get("Odd_H"))), errors="coerce")
o_a = pd.to_numeric(df_2026.get("Odd_A_FT_Back", df_2026.get("Odd_A_FT", df_2026.get("Odd_A"))), errors="coerce")

# 1. Lay 0x0 RF v2
calc_metodo("Lay 0x0 RF v2", ["Odd_CS_0x0_Lay", "Odd_CS_0x0"], (df_2026["gh"] == 0) & (df_2026["ga"] == 0), 8.0, 16.0)

# 2. Lay 0x1 RF v2
calc_metodo("Lay 0x1 RF v2", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0)

# 3. Lay 1x0 RF v2
calc_metodo("Lay 1x0 RF v2", ["Odd_CS_1x0_Lay", "Odd_CS_1x0"], (df_2026["gh"] == 1) & (df_2026["ga"] == 0), 6.0, 12.0)

# 4. Lay 2x0 RF v2
calc_metodo("Lay 2x0 RF v2", ["Odd_CS_2x0_Lay", "Odd_CS_2x0"], (df_2026["gh"] == 2) & (df_2026["ga"] == 0), 6.0, 12.0)

# 5. Lay 0x2 RF v2
calc_metodo("Lay 0x2 RF v2", ["Odd_CS_0x2_Lay", "Odd_CS_0x2"], (df_2026["gh"] == 0) & (df_2026["ga"] == 2), 8.0, 16.0)

# 6. Lay Draw v2
calc_metodo("Lay Draw v2", ["Odd_Lay_Draw", "Odd_D_FT"], df_2026["gh"] == df_2026["ga"], 2.80, 4.20)

# 7. Lay Under 2.5 v2
calc_metodo("Lay Under 2.5 v2", ["Odd_Lay_Under25", "Odd_Under25_FT", "Odd_Under25"], (df_2026["gh"] + df_2026["ga"]) < 2.5, 1.70, 2.30)

# 8. Lay 2x2 Quant
c_22 = (o_u25 <= 2.00) | (o_xg <= 2.40) | (o_h <= 1.75) | (o_a <= 1.75)
calc_metodo("Lay 2x2 Quant", ["Odd_CS_2x2_Lay", "Odd_CS_2x2"], (df_2026["gh"] == 2) & (df_2026["ga"] == 2), 8.0, 14.0, c_22)

# 9. Lay 0x1 Agressivo
c_01_ag = o_h <= 1.85
calc_metodo("Lay 0x1 Agressivo", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_2026["gh"] == 0) & (df_2026["ga"] == 1), 6.0, 12.0, c_01_ag)

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
        "Total Ops": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate": f"{wr:.2f}%",
        "P&L Total (R$)": f"R$ {pnl:,.2f}"
    })

df_sum = pd.DataFrame(summary)

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
print(df_sum.to_string(index=False), flush=True)

def df_to_markdown(df_in, include_index=True):
    lines = []
    cols = ([df_in.index.name or "Mes"] if include_index else []) + list(df_in.columns)
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for idx, r in df_in.iterrows():
        row_vals = ([str(idx)] if include_index else []) + [f"R$ {v:,.2f}" if isinstance(v, (int, float)) and "Rate" not in str(c) and "Ops" not in str(c) else str(v) for c, v in zip(df_in.columns, r.values)]
        lines.append("| " + " | ".join(row_vals) + " |")
    return "\n".join(lines)

art_path = r"C:\Users\thiag\.gemini\antigravity\brain\95f807fc-aeff-419c-bec7-34d43b90cd11\backtest_2026_relatorio_completo.md"

md = "# 📊 Backtest Completo 2026 - Mês a Mês (Todos os Métodos)\n\n"
md += "Relatório oficial consolidador do desempenho mensal de **todos os métodos ativos de 2026** no **ARKAD_PROD**.\n\n"
md += "> ⚠️ **Parâmetros de Gestão:** Stake Fixa de **R$ 100,00** por operação (com comissão padrão Betfair de 5%).\n\n"
md += "### 💵 Tabela Consolidada de P&L Mensal (R$)\n\n"
md += df_to_markdown(pivot_pnl, include_index=True) + "\n\n"
md += "### 🔢 Volume de Operações Mensais (Entradas)\n\n"
md += df_to_markdown(pivot_count, include_index=True) + "\n\n"
md += "### 🏆 Resumo Geral Acumulado no Ano de 2026\n\n"
md += df_to_markdown(df_sum, include_index=False) + "\n"

with open(art_path, "w", encoding="utf-8") as f:
    f.write(md)

with open("backtest_2026_relatorio_completo.md", "w", encoding="utf-8") as f:
    f.write(md)

print("\nSUCCESS", flush=True)
