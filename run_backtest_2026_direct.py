import os, sys, pandas as pd, numpy as np

print("=== BACKTEST DIRECT 2026 MÊS A MÊS - ULTRA RÁPIDO ===")

fpath = "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
if not os.path.exists(fpath):
    fpath = "b365_base_lean.csv"

def col_filter(c):
    c_low = c.lower()
    return any(k in c_low for k in ["date", "goals", "odd", "xg"])

df = pd.read_csv(fpath, usecols=col_filter, low_memory=False)
date_col = [c for c in df.columns if 'date' in c.lower() or 'data' in c.lower()][0]

df["Date_dt"] = pd.to_datetime(df[date_col], errors="coerce")
df_2026 = df[df["Date_dt"].dt.year == 2026].dropna(subset=["Date_dt"]).sort_values("Date_dt").reset_index(drop=True)
df_2026["Mes"] = df_2026["Date_dt"].dt.strftime("%Y-%m")

gh_col = [c for c in df_2026.columns if 'goals_h_ft' in c.lower() or 'gols mandante' in c.lower()][0]
ga_col = [c for c in df_2026.columns if 'goals_a_ft' in c.lower() or 'gols visitante' in c.lower()][0]
df_2026["gh"] = pd.to_numeric(df_2026[gh_col], errors="coerce").fillna(-1).astype(int)
df_2026["ga"] = pd.to_numeric(df_2026[ga_col], errors="coerce").fillna(-1).astype(int)
df_2026 = df_2026[(df_2026["gh"] >= 0) & (df_2026["ga"] >= 0)].copy()

print(f"[+] Partidas finalizadas em 2026: {len(df_2026)}")

all_dfs = []

def add_trades_vectorized(sub, metodo, odd_col, is_red_series):
    if sub.empty: return
    s = sub.copy()
    odd_series = pd.to_numeric(s[odd_col], errors="coerce")
    valid_mask = (odd_series > 1.0) & odd_series.notna()
    if not valid_mask.any(): return
    
    s = s[valid_mask].copy()
    odd_series = odd_series[valid_mask]
    s["Metodo"] = metodo
    s["is_red"] = is_red_series.loc[s.index]
    s["Resultado"] = np.where(~s["is_red"], "GREEN", "RED")
    s["PnL_R$"] = np.where(~s["is_red"], 95.0, -(odd_series - 1.0) * 100.0)
    all_dfs.append(s[["Metodo", "Mes", "Resultado", "PnL_R$"]])

# 1. Lay 0x0 RF v2
odd_00_col = [c for c in df_2026.columns if '0x0' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '0x0' in c.lower()]
if odd_00_col:
    odd_series = pd.to_numeric(df_2026[odd_00_col[0]], errors="coerce")
    m = (odd_series >= 8.0) & (odd_series <= 16.0)
    add_trades_vectorized(df_2026[m], "Lay 0x0 RF v2", odd_00_col[0], (df_2026["gh"] == 0) & (df_2026["ga"] == 0))

# 2. Lay 0x1 RF v2
odd_01_col = [c for c in df_2026.columns if '0x1' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '0x1' in c.lower()]
if odd_01_col:
    odd_series = pd.to_numeric(df_2026[odd_01_col[0]], errors="coerce")
    m = (odd_series >= 6.0) & (odd_series <= 12.0)
    add_trades_vectorized(df_2026[m], "Lay 0x1 RF v2", odd_01_col[0], (df_2026["gh"] == 0) & (df_2026["ga"] == 1))

# 3. Lay 1x0 RF v2
odd_10_col = [c for c in df_2026.columns if '1x0' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '1x0' in c.lower()]
if odd_10_col:
    odd_series = pd.to_numeric(df_2026[odd_10_col[0]], errors="coerce")
    m = (odd_series >= 6.0) & (odd_series <= 12.0)
    add_trades_vectorized(df_2026[m], "Lay 1x0 RF v2", odd_10_col[0], (df_2026["gh"] == 1) & (df_2026["ga"] == 0))

# 4. Lay 2x0 RF v2
odd_20_col = [c for c in df_2026.columns if '2x0' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '2x0' in c.lower()]
if odd_20_col:
    odd_series = pd.to_numeric(df_2026[odd_20_col[0]], errors="coerce")
    m = (odd_series >= 6.0) & (odd_series <= 12.0)
    add_trades_vectorized(df_2026[m], "Lay 2x0 RF v2", odd_20_col[0], (df_2026["gh"] == 2) & (df_2026["ga"] == 0))

# 5. Lay 0x2 RF v2
odd_02_col = [c for c in df_2026.columns if '0x2' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '0x2' in c.lower()]
if odd_02_col:
    odd_series = pd.to_numeric(df_2026[odd_02_col[0]], errors="coerce")
    m = (odd_series >= 8.0) & (odd_series <= 16.0)
    add_trades_vectorized(df_2026[m], "Lay 0x2 RF v2", odd_02_col[0], (df_2026["gh"] == 0) & (df_2026["ga"] == 2))

# 6. Lay Draw v2
odd_d_col = [c for c in df_2026.columns if ('draw' in c.lower() or 'd_ft' in c.lower()) and 'lay' in c.lower()] or [c for c in df_2026.columns if 'odd_d' in c.lower()]
if odd_d_col:
    odd_series = pd.to_numeric(df_2026[odd_d_col[0]], errors="coerce")
    m = (odd_series >= 2.80) & (odd_series <= 4.20)
    add_trades_vectorized(df_2026[m], "Lay Draw v2", odd_d_col[0], df_2026["gh"] == df_2026["ga"])

# 7. Lay Under 2.5 v2
odd_u25_col = [c for c in df_2026.columns if 'under25' in c.lower() or 'under 2.5' in c.lower()]
if odd_u25_col:
    odd_series = pd.to_numeric(df_2026[odd_u25_col[0]], errors="coerce")
    m = (odd_series >= 1.70) & (odd_series <= 2.30)
    add_trades_vectorized(df_2026[m], "Lay Under 2.5 v2", odd_u25_col[0], (df_2026["gh"] + df_2026["ga"]) < 2.5)

# 8. Lay 2x2 Quant
odd_22_col = [c for c in df_2026.columns if '2x2' in c.lower() and 'lay' in c.lower()] or [c for c in df_2026.columns if '2x2' in c.lower()]
if odd_22_col:
    odd_series = pd.to_numeric(df_2026[odd_22_col[0]], errors="coerce")
    u25_series = pd.to_numeric(df_2026[odd_u25_col[0]], errors="coerce") if odd_u25_col else pd.Series(index=df_2026.index, dtype=float)
    h_col = [c for c in df_2026.columns if c.lower() in ['odd_h', 'odd_h_ft', 'odd_home']][0]
    a_col = [c for c in df_2026.columns if c.lower() in ['odd_a', 'odd_a_ft', 'odd_away']][0]
    odd_h = pd.to_numeric(df_2026[h_col], errors="coerce")
    odd_a = pd.to_numeric(df_2026[a_col], errors="coerce")
    
    m = (odd_series >= 8.0) & (odd_series <= 14.0) & ((u25_series <= 2.00) | (odd_h <= 1.75) | (odd_a <= 1.75))
    add_trades_vectorized(df_2026[m], "Lay 2x2 Quant", odd_22_col[0], (df_2026["gh"] == 2) & (df_2026["ga"] == 2))

# 9. Lay 0x1 Agressivo
if odd_01_col:
    odd_series = pd.to_numeric(df_2026[odd_01_col[0]], errors="coerce")
    odd_h = pd.to_numeric(df_2026[[c for c in df_2026.columns if c.lower() in ['odd_h', 'odd_h_ft', 'odd_home']][0]], errors="coerce")
    m = (odd_series >= 6.0) & (odd_series <= 12.0) & (odd_h <= 1.85)
    add_trades_vectorized(df_2026[m], "Lay 0x1 Agressivo", odd_01_col[0], (df_2026["gh"] == 0) & (df_2026["ga"] == 1))

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
