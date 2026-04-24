import pandas as pd
import glob
import json
from pathlib import Path

hist = pd.read_csv("recalculo_sem_combos_usuario.csv")
ligas_historico = set(hist["Liga"].str.strip().str.upper().unique())

frames = []
for f in sorted(glob.glob("Apostas_Diarias/Apostas_*.xlsx")):
    frames.append(pd.read_excel(f))
df = pd.concat(frames, ignore_index=True)
df = df[[c for c in df.columns if not c.startswith("Unnamed")]].copy()
df["Resultado"] = df["Resultado"].astype(str).str.strip().str.upper()
df = df[df["Resultado"].isin(["GREEN", "RED"])].copy()
df["PnL"] = pd.to_numeric(df["Lucro_Prejuizo"], errors="coerce").fillna(0)
df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
df["Liga"] = df["Liga"].str.strip().str.upper()

filtros = json.loads(Path("config_prod_v1.json").read_text(encoding="utf-8"))["runtime_data"]["filtros_metodo"]

def passes_odd(row):
    m = str(row.get("Metodo", ""))
    flt = filtros.get(m)
    if not flt:
        return True
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False
    omn, omx = flt.get("odd_min"), flt.get("odd_max")
    if omn is not None and float(odd) < float(omn):
        return False
    if omx is not None and float(odd) > float(omx):
        return False
    return True

df = df[df.apply(passes_odd, axis=1)].copy()

jogos_com_0x1 = df[df["Metodo"] == "Lay_CS_0x1_B365"].groupby("Data")["Jogo"].apply(
    lambda x: set(x.str.strip())
)

def confirmado(row):
    if str(row["Metodo"]) != "Lay_CS_1x0_B365":
        return True
    return str(row["Jogo"]).strip() in jogos_com_0x1.get(row["Data"], set())

df = df[df.apply(confirmado, axis=1)].copy()
df["no_historico"] = df["Liga"].isin(ligas_historico)

SEP = "=" * 60

novas = df[~df["no_historico"]]
conhecidas = df[df["no_historico"]]

print(SEP)
print("LIGAS NOVAS (nao estavam no backtest historico)")
print(SEP)
if novas.empty:
    print("  Nenhuma apos filtros aplicados.")
else:
    tab = novas.groupby("Liga").agg(
        apostas=("PnL", "count"),
        greens=("Resultado", lambda x: (x == "GREEN").sum()),
        pnl=("PnL", "sum"),
    ).copy()
    tab["WR%"] = (tab["greens"] / tab["apostas"] * 100).round(1)
    tab["reds"] = tab["apostas"] - tab["greens"]
    tab = tab[["apostas", "greens", "reds", "WR%", "pnl"]].sort_values("pnl")
    print(tab.to_string())
    wr_n = (novas["Resultado"] == "GREEN").mean() * 100
    pnl_n = novas["PnL"].sum()
    print(f"\n  TOTAL: {len(novas)} ap | WR {wr_n:.1f}% | P&L R$ {pnl_n:,.2f}")

print()
print(SEP)
print("LIGAS DO BACKTEST HISTORICO (presentes agora)")
print(SEP)
if conhecidas.empty:
    print("  Nenhuma.")
else:
    tab2 = conhecidas.groupby("Liga").agg(
        apostas=("PnL", "count"),
        greens=("Resultado", lambda x: (x == "GREEN").sum()),
        pnl=("PnL", "sum"),
    ).copy()
    tab2["WR%"] = (tab2["greens"] / tab2["apostas"] * 100).round(1)
    tab2["reds"] = tab2["apostas"] - tab2["greens"]
    tab2 = tab2[["apostas", "greens", "reds", "WR%", "pnl"]].sort_values("pnl")
    print(tab2.to_string())
    wr_c = (conhecidas["Resultado"] == "GREEN").mean() * 100
    pnl_c = conhecidas["PnL"].sum()
    print(f"\n  TOTAL: {len(conhecidas)} ap | WR {wr_c:.1f}% | P&L R$ {pnl_c:,.2f}")

print()
print(SEP)
print("RESUMO")
print(SEP)
wr_tot = (df["Resultado"] == "GREEN").mean() * 100
pnl_tot = df["PnL"].sum()
print(f"  Total geral:       {len(df)} ap | WR {wr_tot:.1f}% | P&L R$ {pnl_tot:,.2f}")
if not novas.empty:
    wr_n2 = (novas["Resultado"] == "GREEN").mean() * 100
    pnl_n2 = novas["PnL"].sum()
    print(f"  Ligas novas:       {len(novas)} ap | WR {wr_n2:.1f}% | P&L R$ {pnl_n2:,.2f}")
if not conhecidas.empty:
    wr_c2 = (conhecidas["Resultado"] == "GREEN").mean() * 100
    pnl_c2 = conhecidas["PnL"].sum()
    print(f"  Ligas historico:   {len(conhecidas)} ap | WR {wr_c2:.1f}% | P&L R$ {pnl_c2:,.2f}")
