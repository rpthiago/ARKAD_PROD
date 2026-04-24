"""
Backtest: Blacklist Atual vs Blacklist + Prioridade (cap 4/dia, max 1 Lay1x0)
"""
import pandas as pd
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from engine_ciclo_producao import prepare_dataframe, _run_cycle_no_monitor, load_config

cfg = load_config(Path("config_backtest_exec.json"))
rodo = json.loads(Path("config_rodos_master.json").read_text(encoding="utf-8"))
cuts = rodo.get("filtros_rodo", [])

CSV = Path("Arquivados_Apostas_Diarias/Relatorios/WalkForward/pacote_reproducao_todos_jogos_20250801_20260414.csv")
df_raw = pd.read_csv(CSV)
df_raw["Data_Arquivo"] = pd.to_datetime(df_raw["Data_Arquivo"])
df_raw = df_raw[df_raw["1/0"].notna()].copy()
df_raw = df_raw.sort_values(["Data_Arquivo", "Horario_Entrada"]).reset_index(drop=True)

# ── Filtro blacklist base ─────────────────────────────────────────────────
def matches_cut(row, cut):
    liga = str(row.get("Liga", ""))
    metodo = str(row.get("Metodo", ""))
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if cut.get("league") and str(cut["league"]).upper() != liga.upper():
        return False
    if cut.get("method_equals") and str(cut["method_equals"]) != metodo:
        return False
    if not pd.isna(odd):
        if cut.get("odd_min") is not None and float(odd) < float(cut["odd_min"]):
            return False
        if cut.get("odd_max") is not None and float(odd) > float(cut["odd_max"]):
            return False
    return True

df_raw["_bl"] = ~df_raw.apply(lambda r: any(matches_cut(r, c) for c in cuts), axis=1)
df_bl = df_raw[df_raw["_bl"]].copy().reset_index(drop=True)

# ── Função de prioridade ──────────────────────────────────────────────────
def prioridade(row):
    m = str(row["Metodo"])
    o = float(row["Odd_Base"]) if pd.notna(row["Odd_Base"]) else 0.0
    if m == "Lay_CS_0x1_B365":
        return 1 if o >= 9 else 2
    if m == "Lay_CS_1x0_B365":
        return 3 if o < 9 else 4
    return 9

df_bl["prio"] = df_bl.apply(prioridade, axis=1)

# ── Aplicar cap por dia: max 4 entradas, max 1 Lay1x0 ───────────────────
MAX_DIA = 4
MAX_L1_DIA = 1

selecionados = []
for dia, grupo in df_bl.groupby("Data_Arquivo"):
    g = grupo.sort_values(["prio", "Odd_Base"], ascending=[True, False])
    l1_count = 0
    count = 0
    for _, row in g.iterrows():
        if count >= MAX_DIA:
            break
        if row["Metodo"] == "Lay_CS_1x0_B365":
            if l1_count >= MAX_L1_DIA:
                continue
            l1_count += 1
        selecionados.append(row.name)
        count += 1

df_prio = df_bl.loc[selecionados].copy().reset_index(drop=True)

# ── Simulação ─────────────────────────────────────────────────────────────
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
    score = abs(lucro / dd_abs) if dd_abs > 0 else float("inf")
    print(f"  Entradas    : {entradas}")
    print(f"  Win Rate    : {wr:.1f}%")
    print(f"  Lucro Final : R$ {lucro:+,.2f}")
    print(f"  DD Max      : R$ {dd_abs:,.2f}  ({dd_pct:.2f}%)")
    print(f"  Score L/DD  : {score:.2f}")
    print(f"  Step-ups    : {step_ups}")
    return df_out, summary

print("=" * 65)
print("CENARIO A — BLACKLIST PURO (sem cap de volume)")
print("=" * 65)
res_bl = simular(df_bl, "Blacklist")

print()
print("=" * 65)
print(f"CENARIO B — BLACKLIST + PRIORIDADE (max {MAX_DIA}/dia, max {MAX_L1_DIA} Lay1x0)")
print("  P1: Lay0x1 odd>=9  |  P2: Lay0x1 odd 7-9")
print("  P3: Lay1x0 odd<9   |  P4: Lay1x0 odd>=9")
print("=" * 65)
res_prio = simular(df_prio, "Prioridade")

# ── Comparativo mensal ────────────────────────────────────────────────────
print()
print("=" * 65)
print("LUCRO MENSAL — COMPARATIVO")
print("=" * 65)

def lucro_mensal(res, label):
    if res is None:
        return pd.Series(dtype=float)
    df_out = res[0].copy()
    date_col = "__date" if "__date" in df_out.columns else "Data_Arquivo"
    df_out["__mes"] = pd.to_datetime(df_out[date_col], errors="coerce").dt.to_period("M")
    return df_out.groupby("__mes")["PnL_Linha"].sum().rename(label)

lb = lucro_mensal(res_bl,   "Blacklist_Puro")
lp = lucro_mensal(res_prio, "BL+Prioridade")
comp = pd.concat([lb, lp], axis=1).fillna(0).round(2)
comp["Melhor"] = comp[["Blacklist_Puro", "BL+Prioridade"]].idxmax(axis=1)
print(comp.to_string())

# ── Entradas descartadas pelo cap ─────────────────────────────────────────
descartados = df_bl[~df_bl.index.isin(selecionados)].copy()
print()
print("=" * 65)
print(f"ENTRADAS DESCARTADAS PELO CAP ({len(descartados)} total)")
print("=" * 65)
if not descartados.empty:
    w = (descartados["1/0"] == 1).sum()
    l = (descartados["1/0"] == 0).sum()
    print(f"  Resultado: {w}W / {l}L | WR {w/(w+l)*100:.1f}%")
    print()
    for dia, g in descartados.groupby("Data_Arquivo"):
        for _, r in g.iterrows():
            res = "GREEN" if r["1/0"] == 1 else "RED"
            print(f"  {str(dia.date())} | P{r['prio']} | {r['Metodo']} | {r['Liga']} | Odd {r['Odd_Base']:.2f} | {res}")

# ── Resumo final ──────────────────────────────────────────────────────────
print()
print("=" * 65)
print("RESUMO FINAL")
print("=" * 65)
for label, res in [("Blacklist Puro", res_bl), (f"BL+Prioridade (max {MAX_DIA}/dia)", res_prio)]:
    if res is None:
        continue
    s = res[1]
    lucro = s["Lucro_Final"]
    dd = s["Max_Drawdown_Abs"]
    score = abs(lucro / dd) if dd > 0 else float("inf")
    print(
        f"  {label:<30}: Lucro R$ {lucro:+8,.0f} | "
        f"WR {s['Win_Rate_Executadas_%']:5.1f}% | "
        f"DD R$ {dd:>6,.0f} | "
        f"Score {score:.2f} | "
        f"Entradas {s['Entradas_Executadas']}"
    )

# ── Análise dias de alto volume: full vs top4 ─────────────────────────────
print()
print("=" * 65)
print("DIAS COM 4+ ENTRADAS — Resultado Full vs Selecionado")
print("=" * 65)
por_dia = df_bl.groupby("Data_Arquivo").size().reset_index(name="n")
dias_altos = por_dia[por_dia["n"] >= 4]["Data_Arquivo"]

for d in dias_altos:
    full = df_bl[df_bl["Data_Arquivo"] == d]
    sel  = df_prio[df_prio["Data_Arquivo"] == d] if "Data_Arquivo" in df_prio.columns else pd.DataFrame()
    wr_full = (full["1/0"] == 1).mean() * 100
    wr_sel  = (sel["1/0"] == 1).mean() * 100 if not sel.empty else 0
    n_sel = len(sel)
    descartados_dia = full[~full.index.isin(sel.index)] if not sel.empty else full
    reds_desc = (descartados_dia["1/0"] == 0).sum() if not descartados_dia.empty else 0
    tag = "EVITOU RED" if reds_desc > 0 else "ok"
    print(
        f"{d.date()} | Full {len(full)}x WR={wr_full:.0f}% "
        f"| Sel {n_sel}x WR={wr_sel:.0f}% | {tag}"
    )
