"""
Comparacao: Scanner Juros Compostos (DASHBOARD_ARKAD-1) vs Metodo Atual (ARKAD_PROD blacklist)
Dataset: pacote_reproducao_todos_jogos_20250801_20260414.csv
"""
import pandas as pd
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from engine_ciclo_producao import prepare_dataframe, _run_cycle_no_monitor, load_config

# Replicar _matches_cut inline (evita importar streamlit do main.py)
def _matches_cut(row: pd.Series, cut: dict, league_col: str, method_col: str, odd_col: str) -> bool:
    league = str(row.get(league_col, ""))
    method = str(row.get(method_col, ""))
    odd = pd.to_numeric(row.get(odd_col), errors="coerce")

    if cut.get("league") and str(cut["league"]).upper() != league.upper():
        return False
    if cut.get("method_equals"):
        meth_cut = str(cut["method_equals"])
        aliases = {
            "Lay_CS_0x1_B365": {"Lay_CS_0x1_B365", "Lay_0x1", "L0x1"},
            "Lay_CS_1x0_B365": {"Lay_CS_1x0_B365", "Lay_1x0", "L1x0"},
        }
        allowed = aliases.get(meth_cut, {meth_cut})
        if method not in allowed:
            return False
    if not pd.isna(odd):
        odd_min = cut.get("odd_min")
        odd_max = cut.get("odd_max")
        if odd_min is not None and float(odd) < float(odd_min):
            return False
        if odd_max is not None and float(odd) > float(odd_max):
            return False
    return True

cfg = load_config(Path("config_backtest_exec.json"))
rodo = json.loads(Path("config_rodos_master.json").read_text(encoding="utf-8"))
cuts = rodo.get("filtros_rodo", [])

CSV = Path("Arquivados_Apostas_Diarias/Relatorios/WalkForward/pacote_reproducao_todos_jogos_20250801_20260414.csv")
df_raw = pd.read_csv(CSV)
df_raw["Data_Arquivo"] = pd.to_datetime(df_raw["Data_Arquivo"], errors="coerce")
df_raw = df_raw[df_raw["1/0"].notna()].copy()

result_col = cfg["input"]["columns"]["result_col"]
if result_col not in df_raw.columns:
    df_raw[result_col] = df_raw["1/0"]

# ============================================================
# FILTRO A: Scanner Juros Compostos (DASHBOARD_ARKAD-1)
# Regras extraidas de streamlit_scanner_juros_compostos.py
# ============================================================
BLACKLIST_LIGAS_JC = {
    "NETHERLANDS 1", "UKRAINE 1", "PORTUGAL 2", "ITALY 3",
    "TURKEY 1", "SAUDI ARABIA 1", "ARGENTINA 1", "ROMANIA 1",
}
ALLOWED_1X0_JC = {"ITALY 1", "SPAIN 1", "SPAIN 2"}

L0_ODD_MIN = 8.0
L0_ODD_MAX = 11.0
L1_ODD_MAX = 11.0

def filtro_juros_compostos(row):
    liga = str(row.get("Liga", ""))
    metodo = str(row.get("Metodo", ""))
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False
    odd = float(odd)

    if metodo == "Lay_CS_0x1_B365":
        if liga in BLACKLIST_LIGAS_JC:
            return False
        return L0_ODD_MIN <= odd <= L0_ODD_MAX

    if metodo == "Lay_CS_1x0_B365":
        if liga not in ALLOWED_1X0_JC:
            return False
        return odd <= L1_ODD_MAX

    return False

# ============================================================
# FILTRO B: ARKAD_PROD Blacklist (metodo atual corrigido)
# ============================================================
def filtro_blacklist(row):
    return not any(_matches_cut(row, c, "Liga", "Metodo", "Odd_Base") for c in cuts)

print("Aplicando filtros... aguarde.")
df_raw["_jc"] = df_raw.apply(filtro_juros_compostos, axis=1)
df_raw["_bl"] = df_raw.apply(filtro_blacklist, axis=1)

df_jc = df_raw[df_raw["_jc"]].copy()
df_bl = df_raw[df_raw["_bl"]].copy()
df_sem = df_raw.copy()

print(f"Dataset total com resultado: {len(df_raw)}")
print(f"  Juros Compostos (JC)   : {len(df_jc)}")
print(f"  Blacklist atual (BL)   : {len(df_bl)}")
print(f"  Sem filtro             : {len(df_sem)}")
print()

# ============================================================
# Simulacao
# ============================================================
def simular(df_group, label):
    df_p = prepare_dataframe(df_group.copy(), cfg, environment="historico")
    if df_p.empty:
        print(f"[{label}] Vazio.")
        return None
    df_out, summary = _run_cycle_no_monitor(df_p, cfg, environment="historico")
    lucro = summary["Lucro_Final"]
    wr = summary["Win_Rate_Executadas_%"]
    dd_abs = summary["Max_Drawdown_Abs"]
    dd_pct = summary["Max_Drawdown_%"]
    entradas = summary["Entradas_Executadas"]
    step_ups = summary["Step_Ups"]
    score = abs(lucro/dd_abs) if dd_abs > 0 else float("inf")
    print(f"  Entradas   : {entradas}")
    print(f"  Win Rate   : {wr:.1f}%")
    print(f"  Lucro Final: R$ {lucro:+,.2f}")
    print(f"  DD Max     : R$ {dd_abs:,.2f}  ({dd_pct:.2f}%)")
    print(f"  Score L/DD : {score:.2f}")
    print(f"  Step-ups   : {step_ups}")
    return df_out, summary

pd.set_option("display.width", 130)

print("=" * 65)
print("CENARIO 1 — SEM FILTRO")
print("=" * 65)
res_sem = simular(df_sem, "Sem Filtro")

print()
print("=" * 65)
print("CENARIO 2 — SCANNER JUROS COMPOSTOS (DASHBOARD_ARKAD-1)")
print(f"  Lay 0x1: odd {L0_ODD_MIN}-{L0_ODD_MAX}, excl ligas: {len(BLACKLIST_LIGAS_JC)}")
print(f"  Lay 1x0: odd <={L1_ODD_MAX}, ligas: {ALLOWED_1X0_JC}")
print("=" * 65)
res_jc = simular(df_jc, "Juros Compostos")

print()
print("=" * 65)
print("CENARIO 3 — BLACKLIST ATUAL (ARKAD_PROD)")
print(f"  Bloqueia {len(cuts)} combos toxicos (Liga+Metodo+Odd)")
print("=" * 65)
res_bl = simular(df_bl, "Blacklist atual")

# ============================================================
# Mensal
# ============================================================
print()
print("=" * 65)
print("LUCRO MENSAL — COMPARATIVO")
print("=" * 65)

def lucro_mensal(res, label):
    if res is None:
        return pd.Series(dtype=float)
    df_out = res[0]
    d = df_out.copy()
    date_col = "__date" if "__date" in d.columns else "Data_Arquivo"
    d["__mes"] = pd.to_datetime(d[date_col], errors="coerce").dt.to_period("M")
    return d.groupby("__mes")["PnL_Linha"].sum().rename(label)

ls = lucro_mensal(res_sem, "Sem_Filtro")
lj = lucro_mensal(res_jc, "Juros_Compostos")
lb = lucro_mensal(res_bl, "Blacklist_Atual")

comp = pd.concat([ls, lj, lb], axis=1).fillna(0).round(2)
comp["Melhor"] = comp[["Juros_Compostos", "Blacklist_Atual"]].idxmax(axis=1)
print(comp.to_string())

# ============================================================
# Entradas em comum e exclusivas
# ============================================================
print()
print("=" * 65)
print("OVERLAP: entradas que AMBOS aprovam vs exclusivas")
print("=" * 65)
ambos = df_raw[df_raw["_jc"] & df_raw["_bl"]]
so_jc = df_raw[df_raw["_jc"] & ~df_raw["_bl"]]
so_bl = df_raw[~df_raw["_jc"] & df_raw["_bl"]]

def wr_grupo(sub):
    if sub.empty: return 0, 0, 0
    w = (sub["1/0"]==1).sum()
    t = len(sub)
    return w, t-w, w/t*100

w,l,wr = wr_grupo(ambos)
print(f"  Aprovados por AMBOS  : {len(ambos)} entradas | {w}W/{l}L | WR {wr:.1f}%")
w,l,wr = wr_grupo(so_jc)
print(f"  Apenas Juros Comp.   : {len(so_jc)} entradas | {w}W/{l}L | WR {wr:.1f}%")
w,l,wr = wr_grupo(so_bl)
print(f"  Apenas Blacklist     : {len(so_bl)} entradas | {w}W/{l}L | WR {wr:.1f}%")

# ============================================================
# JC rejeita mas BL aprova — o que JC esta deixando passar
# ============================================================
print()
print("Entradas que Juros Compostos BLOQUEIA mas Blacklist APROVA (perdas nelas):")
perdas_jc_bl = so_bl[so_bl["1/0"]==0]
if perdas_jc_bl.empty:
    print("  Nenhuma perda nessas entradas.")
else:
    for _, r in perdas_jc_bl.iterrows():
        print(f"  {r['Data_Arquivo'].strftime('%d/%m/%y')} | {r['Liga']} | {r['Jogo'][:40]} | {r['Metodo']} | Odd {r['Odd_Base']:.2f}")

print()
print("Entradas que Juros Compostos APROVA mas Blacklist BLOQUEIA (perdas nelas):")
perdas_bl_jc = so_jc[so_jc["1/0"]==0]
if perdas_bl_jc.empty:
    print("  Nenhuma perda nessas entradas.")
else:
    for _, r in perdas_bl_jc.iterrows():
        print(f"  {r['Data_Arquivo'].strftime('%d/%m/%y')} | {r['Liga']} | {r['Jogo'][:40]} | {r['Metodo']} | Odd {r['Odd_Base']:.2f}")

# ============================================================
# Resumo
# ============================================================
print()
print("=" * 65)
print("RESUMO FINAL")
print("=" * 65)
for label, res in [("Sem Filtro", res_sem), ("Juros Compostos (JC)", res_jc), ("Blacklist Atual (BL)", res_bl)]:
    if res is None: continue
    s = res[1]
    lucro = s["Lucro_Final"]
    dd = s["Max_Drawdown_Abs"]
    score = abs(lucro/dd) if dd > 0 else float("inf")
    print(f"  {label:<24}: Lucro R$ {lucro:+8,.0f} | WR {s['Win_Rate_Executadas_%']:5.1f}% | DD R$ {dd:>6,.0f} | Score {score:.2f} | Entradas {s['Entradas_Executadas']}")
