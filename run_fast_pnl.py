import os, sys, pandas as pd, numpy as np

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

chunks = []
for chunk in pd.read_csv(fpath, usecols=avail, chunksize=30000, low_memory=False):
    chunk["d"] = pd.to_datetime(chunk["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    c_2026 = chunk[chunk["d"].str.startswith("2026-", na=False)]
    if not c_2026.empty:
        chunks.append(c_2026)

df = pd.concat(chunks, ignore_index=True)
df["Mes"] = df["d"].str.slice(0, 7)

df["gh"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce").fillna(-1).astype(int)
df["ga"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce").fillna(-1).astype(int)
df = df[(df["gh"] >= 0) & (df["ga"] >= 0)].reset_index(drop=True)

all_trades = []

def calc(metodo, odd_cols, is_red, min_o, max_o, extra=None):
    c_odd = None
    for c in odd_cols:
        if c in df.columns: c_odd = c; break
    if not c_odd: return
    odds = pd.to_numeric(df[c_odd], errors="coerce")
    mask = (odds >= min_o) & (odds <= max_o) & odds.notna()
    if extra is not None: mask = mask & extra
    sub = df[mask].copy()
    if sub.empty: return
    sub["odd"] = odds.loc[sub.index]
    sub["red"] = is_red.loc[sub.index]
    sub["pnl"] = np.where(~sub["red"], 95.0, -(sub["odd"] - 1.0) * 100.0)
    sub["res"] = np.where(~sub["red"], "GREEN", "RED")
    sub["met"] = metodo
    all_trades.append(sub[["met", "Mes", "res", "pnl"]])

ou25 = pd.to_numeric(df.get("Odd_Under25_FT_Back", df.get("Odd_Under25_FT", df.get("Odd_Under25"))), errors="coerce")
oxg = pd.to_numeric(df.get("total_xg", df.get("Total_xG")), errors="coerce")
oh = pd.to_numeric(df.get("Odd_H_FT_Back", df.get("Odd_H_FT", df.get("Odd_H"))), errors="coerce")
oa = pd.to_numeric(df.get("Odd_A_FT_Back", df.get("Odd_A_FT", df.get("Odd_A"))), errors="coerce")

calc("Lay 0x0 RF v2", ["Odd_CS_0x0_Lay", "Odd_CS_0x0"], (df["gh"]==0)&(df["ga"]==0), 8.0, 16.0)
calc("Lay 0x1 RF v2", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df["gh"]==0)&(df["ga"]==1), 6.0, 12.0)
calc("Lay 1x0 RF v2", ["Odd_CS_1x0_Lay", "Odd_CS_1x0"], (df["gh"]==1)&(df["ga"]==0), 6.0, 12.0)
calc("Lay 2x0 RF v2", ["Odd_CS_2x0_Lay", "Odd_CS_2x0"], (df["gh"]==2)&(df["ga"]==0), 6.0, 12.0)
calc("Lay 0x2 RF v2", ["Odd_CS_0x2_Lay", "Odd_CS_0x2"], (df["gh"]==0)&(df["ga"]==2), 8.0, 16.0)
calc("Lay Draw v2", ["Odd_Lay_Draw", "Odd_D_FT"], df["gh"]==df["ga"], 2.80, 4.20)
calc("Lay Under 2.5 v2", ["Odd_Lay_Under25", "Odd_Under25_FT", "Odd_Under25"], (df["gh"]+df["ga"])<2.5, 1.70, 2.30)
c22 = (ou25<=2.0)|(oxg<=2.4)|(oh<=1.75)|(oa<=1.75)
calc("Lay 2x2 Quant", ["Odd_CS_2x2_Lay", "Odd_CS_2x2"], (df["gh"]==2)&(df["ga"]==2), 8.0, 14.0, c22)
calc("Lay 0x1 Agressivo", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df["gh"]==0)&(df["ga"]==1), 6.0, 12.0, oh<=1.85)

df_all = pd.concat(all_trades, ignore_index=True)
p_pnl = pd.pivot_table(df_all, values="pnl", index="Mes", columns="met", aggfunc="sum", fill_value=0.0)
p_cnt = pd.pivot_table(df_all, values="res", index="Mes", columns="met", aggfunc="count", fill_value=0)

summary = []
for met in sorted(df_all["met"].unique()):
    sub = df_all[df_all["met"] == met]
    tot = len(sub)
    grn = (sub["res"] == "GREEN").sum()
    red = (sub["res"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = sub["pnl"].sum()
    summary.append({
        "Método": met,
        "Ops": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate": f"{wr:.2f}%",
        "P&L Total (R$)": f"R$ {pnl:,.2f}"
    })

print("=== PNL 2026 MENSAL COMPLETO ===", flush=True)
print(p_pnl.to_string(), flush=True)
print("\n=== VOLUME DE ENTRADAS ===", flush=True)
print(p_cnt.to_string(), flush=True)
print("\n=== RESUMO ANO 2026 ===", flush=True)
print(pd.DataFrame(summary).to_string(index=False), flush=True)

art_file = r"C:\Users\thiag\.gemini\antigravity\brain\95f807fc-aeff-419c-bec7-34d43b90cd11\backtest_2026_relatorio_completo.md"
with open(art_file, "w", encoding="utf-8") as f:
    f.write("# Backtest 2026\n\n## PnL Mensal\n\n" + p_pnl.to_string() + "\n\n## Entradas\n\n" + p_cnt.to_string())
with open("backtest_2026_relatorio_completo.md", "w", encoding="utf-8") as f:
    f.write("# Backtest 2026\n\n## PnL Mensal\n\n" + p_pnl.to_string() + "\n\n## Entradas\n\n" + p_cnt.to_string())
