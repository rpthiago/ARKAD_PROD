import os, sys, pandas as pd, numpy as np

print("=== GERANDO REPORT DE BACKTEST 2026 MÊS A MÊS ===")

# Base lean
df = pd.read_csv("b365_base_lean.csv", low_memory=False)

df["Date_str"] = df["Date"].astype(str)
df_2026 = df[df["Date_str"].str.startswith("2026-")].copy()
df_2026["Mes_Key"] = df_2026["Date_str"].str.slice(0, 7)

# Mapeamento de Meses Português
meses_nome = {
    "2026-01": "01/2026 (Janeiro)",
    "2026-02": "02/2026 (Fevereiro)",
    "2026-03": "03/2026 (Março)",
    "2026-04": "04/2026 (Abril)",
    "2026-05": "05/2026 (Maio)",
    "2026-06": "06/2026 (Junho)",
    "2026-07": "07/2026 (Julho)",
    "2026-08": "08/2026 (Agosto)"
}

df_2026["Mes"] = df_2026["Mes_Key"].map(meses_nome).fillna(df_2026["Mes_Key"])

df_2026["gh"] = pd.to_numeric(df_2026["Goals_H_FT"], errors="coerce").fillna(-1).astype(int)
df_2026["ga"] = pd.to_numeric(df_2026["Goals_A_FT"], errors="coerce").fillna(-1).astype(int)
df_2026 = df_2026[(df_2026["gh"] >= 0) & (df_2026["ga"] >= 0)].reset_index(drop=True)

print(f"[+] Total de jogos processados em 2026: {len(df_2026)}")

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

print("\n--- P&L CONSOLIDADO ---")
print(pivot_pnl)

# Escrever markdown artifact
art_path = r"C:\Users\thiag\.gemini\antigravity\brain\95f807fc-aeff-419c-bec7-34d43b90cd11\backtest_2026_mes_a_mes.md"

md_content = "# 📊 Backtest Completo 2026 - Mês a Mês (Todos os Métodos)\n\n"
md_content += "Relatório oficial contendo a performance acumulada e mês a mês de **todos os métodos de 2026** no projeto **ARKAD_PROD**.\n\n"
md_content += "> ⚠️ **Gestão de Referência:** Stake Fixa de **R$ 100,00** por entrada (com comissão padrão Betfair de 5%).\n\n"

md_content += "## 📈 Tabela Consolidada de P&L Mensal (R$)\n\n"
md_content += pivot_pnl.to_markdown() + "\n\n"

md_content += "## 🔢 Volume de Operações Mensais (Entradas)\n\n"
md_content += pivot_count.to_markdown() + "\n\n"

md_content += "## 🏆 Resumo Geral Acumulado (Ano 2026)\n\n"
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
        "Total Operações": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate (%)": f"{wr:.2f}%",
        "P&L Total (R$)": f"R$ {pnl:,.2f}"
    })

df_sum = pd.DataFrame(summary)
md_content += df_sum.to_markdown(index=False) + "\n"

with open(art_path, "w", encoding="utf-8") as f:
    f.write(md_content)

with open("backtest_2026_mes_a_mes.md", "w", encoding="utf-8") as f:
    f.write(md_content)

print("✅ ARTIFACT PUBLICADO COM SUCESSO EM:", art_path)
