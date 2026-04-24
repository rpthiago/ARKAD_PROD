import pandas as pd
import glob
import json
from pathlib import Path

frames = []
for f in sorted(glob.glob("Apostas_Diarias/Apostas_*.xlsx")):
    frames.append(pd.read_excel(f))
df = pd.concat(frames, ignore_index=True)
df = df[[c for c in df.columns if not c.startswith("Unnamed")]].copy()
df["Resultado"] = df["Resultado"].astype(str).str.strip().str.upper()
df = df[df["Resultado"].isin(["GREEN", "RED"])].copy()
df["PnL"] = pd.to_numeric(df["Lucro_Prejuizo"], errors="coerce").fillna(0)
df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date

# Filtro odd_range
filtros = json.loads(Path("config_prod_v1.json").read_text(encoding="utf-8"))["runtime_data"]["filtros_metodo"]

def passes_odd(row):
    m = str(row.get("Metodo", ""))
    flt = filtros.get(m)
    if not flt:
        return True
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False
    omn = flt.get("odd_min")
    omx = flt.get("odd_max")
    if omn is not None and float(odd) < float(omn):
        return False
    if omx is not None and float(odd) > float(omx):
        return False
    return True

df = df[df.apply(passes_odd, axis=1)].copy()

# Identificar jogos com os dois metodos no mesmo dia
chave = ["Data", "Jogo"]
contagem = df.groupby(chave)["Metodo"].nunique().reset_index()
contagem.columns = ["Data", "Jogo", "num_metodos"]
duplos_set = set(zip(
    contagem.loc[contagem["num_metodos"] >= 2, "Data"],
    contagem.loc[contagem["num_metodos"] >= 2, "Jogo"]
))

df["duplo"] = df.apply(lambda r: (r["Data"], r["Jogo"]) in duplos_set, axis=1)

duplo = df[df["duplo"]]
simples = df[~df["duplo"]]

SEP = "=" * 60

print(SEP)
print("JOGOS COM OS 2 METODOS JUNTOS (0x1 + 1x0 mesmo jogo/dia)")
print(SEP)
wr_d = (duplo["Resultado"] == "GREEN").mean() * 100
pnl_d = duplo["PnL"].sum()
n_d = len(duplo)
print(f"  Apostas: {n_d} | WR: {wr_d:.1f}% | P&L: R$ {pnl_d:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = duplo[duplo["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR {w/len(sub)*100:.1f}% | P&L R$ {p:,.2f}")

print()
print(SEP)
print("JOGOS COM APENAS 1 METODO")
print(SEP)
wr_s = (simples["Resultado"] == "GREEN").mean() * 100
pnl_s = simples["PnL"].sum()
n_s = len(simples)
print(f"  Apostas: {n_s} | WR: {wr_s:.1f}% | P&L: R$ {pnl_s:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = simples[simples["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR {w/len(sub)*100:.1f}% | P&L R$ {p:,.2f}")

print()
print(SEP)
print("JOGOS DUPLOS - LISTAGEM (ordenado por P&L)")
print(SEP)
agrupado = duplo.groupby(["Data", "Jogo"]).agg(
    apostas=("Metodo", "count"),
    greens=("Resultado", lambda x: (x == "GREEN").sum()),
    reds=("Resultado", lambda x: (x == "RED").sum()),
    pnl=("PnL", "sum"),
    odds=("Odd_Base", lambda x: " / ".join(str(round(v, 1)) for v in sorted(x)))
).reset_index().sort_values("pnl")

print(agrupado.to_string(index=False))

print()
print(SEP)
print("SE BLOQUEASSEMOS JOGOS DUPLOS:")
print(SEP)
df_sem_duplos = simples.copy()
wr_nd = (df_sem_duplos["Resultado"] == "GREEN").mean() * 100
pnl_nd = df_sem_duplos["PnL"].sum()
n_nd = len(df_sem_duplos)
print(f"  Apostas: {n_nd} | WR: {wr_nd:.1f}% | P&L: R$ {pnl_nd:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = df_sem_duplos[df_sem_duplos["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR {w/len(sub)*100:.1f}% | P&L R$ {p:,.2f}")

print()
print(SEP)
print("DIA A DIA SEM JOGOS DUPLOS")
print(SEP)
acum = 0.0
for d, grp in df_sem_duplos.groupby("Data"):
    pnl_dia = grp["PnL"].sum()
    acum += pnl_dia
    a = len(grp)
    w = (grp["Resultado"] == "GREEN").sum()
    print(f"  {d}: {a} ap | {w}G {a-w}R | dia R$ {pnl_dia:,.2f} | acum R$ {acum:,.2f}")
print(f"  TOTAL: R$ {pnl_nd:,.2f}")
