import pandas as pd
import glob
import json
from pathlib import Path

# --- Carregar todos os arquivos ---
frames = []
for f in sorted(glob.glob("Apostas_Diarias/Apostas_*.xlsx")):
    frames.append(pd.read_excel(f))
df = pd.concat(frames, ignore_index=True)
df = df[[c for c in df.columns if not c.startswith("Unnamed")]].copy()
df["Resultado"] = df["Resultado"].astype(str).str.strip().str.upper()
df = df[df["Resultado"].isin(["GREEN", "RED"])].copy()

# --- Configs ---
cfg = json.loads(Path("config_prod_v1.json").read_text(encoding="utf-8"))
filtros_metodo = cfg["runtime_data"]["filtros_metodo"]
rodo_master = json.loads(Path("config_rodos_master.json").read_text(encoding="utf-8"))
cuts = rodo_master.get("filtros_rodo", [])

# Usar Lucro_Prejuizo real (ja preenchido com stake e odd real)
df["PnL"] = pd.to_numeric(df["Lucro_Prejuizo"], errors="coerce").fillna(0)

# --- UNIVERSO BRUTO ---
print("=" * 60)
print("BACKTEST COM LUCRO_PREJUIZO REAL (Stake R$ 500 flat)")
print("=" * 60)
print()
print("--- BRUTO (sem filtros) ---")
total_bruto = df["PnL"].sum()
w_bruto = (df["Resultado"] == "GREEN").sum()
wr_bruto = w_bruto / len(df) * 100
print(f"  Apostas: {len(df)} | WR: {wr_bruto:.1f}% | P&L REAL: R$ {total_bruto:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = df[df["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR {w/len(sub)*100:.1f}% | P&L R$ {p:,.2f}")
print()

# --- FILTRO ODD_RANGE ---
def passes_odd(row):
    m = str(row.get("Metodo", ""))
    flt = filtros_metodo.get(m)
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

mask_odd = df.apply(passes_odd, axis=1)
df_odd = df[mask_odd].copy()

print("--- APOS FILTRO ODD_RANGE (0x1: 8.0-11.5  |  1x0: 4.5-11.5) ---")
wins2 = (df_odd["Resultado"] == "GREEN").sum()
total2 = len(df_odd)
wr2 = wins2 / total2 * 100
pnl2 = df_odd["PnL"].sum()
print(f"  Apostas: {total2} | WR: {wr2:.1f}% | P&L REAL: R$ {pnl2:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = df_odd[df_odd["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR: {w/len(sub)*100:.1f}% | P&L: R$ {p:,.2f}")
print()

# --- FILTRO ODD_RANGE + RODO ---
def is_rodo_blocked(row):
    league_col, method_col, odd_col = "Liga", "Metodo", "Odd_Base"
    for cut in cuts:
        lg = cut.get("league")
        if lg and str(row.get(league_col, "")).strip().upper() != str(lg).strip().upper():
            continue
        me = cut.get("method_equals")
        if me and str(row.get(method_col, "")).strip().upper() != str(me).strip().upper():
            continue
        try:
            odd = float(row.get(odd_col) or 0)
        except Exception:
            continue
        omn = cut.get("odd_min")
        omx = cut.get("odd_max")
        if omn is not None and odd < float(omn):
            continue
        if omx is not None and odd > float(omx):
            continue
        return True
    return False

mask_rodo = ~df_odd.apply(is_rodo_blocked, axis=1)
df_final = df_odd[mask_rodo].copy()

print("--- APOS FILTRO ODD_RANGE + RODO BLACKLIST (CONFIGURACAO FINAL) ---")
wins3 = (df_final["Resultado"] == "GREEN").sum()
total3 = len(df_final)
wr3 = wins3 / total3 * 100
pnl3 = df_final["PnL"].sum()
print(f"  Apostas: {total3} | WR: {wr3:.1f}% | P&L REAL: R$ {pnl3:,.2f}")
for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    sub = df_final[df_final["Metodo"] == m]
    if not sub.empty:
        w = (sub["Resultado"] == "GREEN").sum()
        p = sub["PnL"].sum()
        print(f"  {m}: {len(sub)} ap | WR: {w/len(sub)*100:.1f}% | P&L: R$ {p:,.2f}")
print()

print("--- DIA A DIA (configuracao final) ---")
df_final = df_final.copy()
df_final["Data"] = pd.to_datetime(df_final["Data"], errors="coerce").dt.date
acum = 0.0
for d, grp in df_final.groupby("Data"):
    pnl_d = grp["PnL"].sum()
    acum += pnl_d
    a = len(grp)
    w = (grp["Resultado"] == "GREEN").sum()
    print(f"  {d}: {a} ap | {w}G {a-w}R | dia R$ {pnl_d:,.2f} | acum R$ {acum:,.2f}")
print(f"  TOTAL: R$ {pnl3:,.2f}")
print()

print("=" * 60)
print("RESUMO COMPARATIVO")
print("=" * 60)
print(f"  Bruto (713 ap):      WR {wr_bruto:.1f}%  P&L R$ {total_bruto:,.2f}")
print(f"  + Filtro odd_range:  WR {wr2:.1f}%  P&L R$ {pnl2:,.2f}  ({total2} ap, -{len(df)-total2} removidas)")
print(f"  + Rodo blacklist:    WR {wr3:.1f}%  P&L R$ {pnl3:,.2f}  ({total3} ap, -{total2-total3} removidas)")
print()

removidas = df[~mask_odd]
print("TOP LIGAS REMOVIDAS PELO FILTRO ODD_RANGE (por P&L):")
tab = removidas.groupby(["Liga", "Metodo"]).agg(
    apostas=("PnL", "count"), pnl=("PnL", "sum")
).sort_values("pnl")
print(tab.head(15).to_string())
