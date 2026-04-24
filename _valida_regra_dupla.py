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

filtros = json.loads(Path("config_prod_v1.json").read_text(encoding="utf-8"))["runtime_data"]["filtros_metodo"]
rodo_cuts = json.loads(Path("config_rodos_master.json").read_text(encoding="utf-8")).get("filtros_rodo", [])

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

def is_rodo_blocked(row):
    for cut in rodo_cuts:
        lg = cut.get("league")
        if lg and str(row.get("Liga", "")).upper() != str(lg).upper():
            continue
        me = cut.get("method_equals")
        if me and str(row.get("Metodo", "")).upper() != str(me).upper():
            continue
        try:
            odd = float(row.get("Odd_Base") or 0)
        except Exception:
            continue
        omn, omx = cut.get("odd_min"), cut.get("odd_max")
        if omn is not None and odd < float(omn):
            continue
        if omx is not None and odd > float(omx):
            continue
        return True
    return False

df = df[df.apply(passes_odd, axis=1)].copy()
df = df[~df.apply(is_rodo_blocked, axis=1)].copy()

SEP = "=" * 60

print(SEP)
print("ANTES da regra dupla (odd_range + rodo ja aplicados)")
print(SEP)
n_antes = len(df)
wr_antes = (df["Resultado"] == "GREEN").mean() * 100
pnl_antes = df["PnL"].sum()
print(f"  Apostas: {n_antes} | WR: {wr_antes:.1f}% | P&L: R$ {pnl_antes:,.2f}")

# Regra de confirmacao dupla por (Data, Jogo)
jogos_com_0x1 = df[df["Metodo"] == "Lay_CS_0x1_B365"].groupby("Data")["Jogo"].apply(
    lambda x: set(x.str.strip())
)

def confirmado(row):
    if str(row["Metodo"]) != "Lay_CS_1x0_B365":
        return True
    jogos_ok = jogos_com_0x1.get(row["Data"], set())
    return str(row["Jogo"]).strip() in jogos_ok

mask_conf = df.apply(confirmado, axis=1)
df_conf = df[mask_conf].copy()
df_bloq = df[~mask_conf].copy()

print()
print(SEP)
print("APOS REGRA CONFIRMACAO DUPLA (configuracao final)")
print(SEP)
n2 = len(df_conf)
wr2 = (df_conf["Resultado"] == "GREEN").mean() * 100
pnl2 = df_conf["PnL"].sum()
removidos = n_antes - n2
print(f"  Apostas: {n2} | WR: {wr2:.1f}% | P&L: R$ {pnl2:,.2f}  (-{removidos} bloqueados)")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = df_conf[df_conf["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR {w/len(sub)*100:.1f}% | P&L R$ {p:,.2f}")

print()
print("--- DIA A DIA ---")
acum = 0.0
for d, grp in df_conf.groupby("Data"):
    pnl_d = grp["PnL"].sum()
    acum += pnl_d
    a = len(grp)
    w = (grp["Resultado"] == "GREEN").sum()
    print(f"  {d}: {a} ap | {w}G {a-w}R | dia R$ {pnl_d:,.2f} | acum R$ {acum:,.2f}")

print()
n_bloq = len(df_bloq)
wr_bloq = (df_bloq["Resultado"] == "GREEN").mean() * 100 if n_bloq else 0.0
pnl_bloq = df_bloq["PnL"].sum()
print(SEP)
print("1x0 BLOQUEADOS (sem confirmacao 0x1) — evitados:")
print(SEP)
print(f"  {n_bloq} apostas | WR {wr_bloq:.1f}% | P&L {pnl_bloq:,.2f}")
print()
print(SEP)
print("COMPARATIVO FINAL")
print(SEP)
print(f"  Sem regra dupla:   {n_antes} ap | WR {wr_antes:.1f}% | P&L R$ {pnl_antes:,.2f}")
print(f"  Com regra dupla:   {n2} ap | WR {wr2:.1f}% | P&L R$ {pnl2:,.2f}")
melhora = pnl2 - pnl_antes
print(f"  Melhora: R$ {melhora:+,.2f}")
