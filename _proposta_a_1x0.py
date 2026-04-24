"""
Proposta A: restringir Lay_CS_1x0_B365 apenas às ligas com histórico validado.

Histórico mostra 1x0 válido em:
  - SPAIN 1:  52 entradas, WR 84%
  - ITALY 1:  21 entradas, WR 90%

Compara 3 configurações no dataset histórico completo:
  baseline   — filtros atuais (odd range + blacklist), 1x0 livre
  proposta_a — filtros atuais + 1x0 apenas SPAIN1 e ITALY1
  proposta_a2 — filtros atuais + 1x0 apenas ITALY1 (SPAIN1 com WR 84% é questionável)
"""
import subprocess, sys, json, pandas as pd
from pathlib import Path

ROOT     = Path(".")
CSV_HIST = ROOT / "recalculo_sem_combos_usuario.csv"
OUT      = Path("Arquivados_Apostas_Diarias/Relatorios/Proposta_A_1x0")
OUT.mkdir(parents=True, exist_ok=True)

hist = pd.read_csv(CSV_HIST)
hist["Data_Arquivo"]   = pd.to_datetime(hist["Data_Arquivo"])
hist["Metodo"]         = hist["Metodo"].str.strip()
hist["Liga"]           = hist["Liga"].str.strip().str.upper()

# Filtros base (mesmos do sistema atual)
mask_0x1 = (hist["Metodo"] == "Lay_CS_0x1_B365") & (hist["Odd_Base"] >= 8.0) & (hist["Odd_Base"] <= 11.5)
mask_1x0 = (hist["Metodo"] == "Lay_CS_1x0_B365") & (hist["Odd_Base"] >= 4.5) & (hist["Odd_Base"] <= 11.5)
base = hist[mask_0x1 | mask_1x0].copy()

# Blacklist
with open("config_rodos_master.json") as f:
    regras_bl = json.load(f)["filtros_rodo"]

def is_bloqueado(row):
    for r in regras_bl:
        liga_r  = r.get("league", "").strip().upper()
        met_r   = r.get("method_equals", "").strip()
        odd_min = r.get("odd_min") or -99
        odd_max = r.get("odd_max") or 999
        if (row["Liga"] == liga_r and row["Metodo"] == met_r
                and odd_min <= row["Odd_Base"] <= odd_max):
            return True
    return False

base["bloq"] = base.apply(is_bloqueado, axis=1)
base = base[~base["bloq"]].copy()

print(f"Base histórica com filtros atuais: {len(base)} entradas")

# ── Constrói os 3 CSVs ───────────────────────────────────────────────────────
colunas = ["Data_Arquivo", "Horario_Entrada", "Liga", "Jogo",
           "Metodo", "Odd_Base", "1/0"]

# Baseline (1x0 livre)
csv_baseline = OUT / "hist_baseline.csv"
base[colunas].to_csv(csv_baseline, index=False)

# Proposta A: 1x0 só SPAIN 1 + ITALY 1
LIGAS_A = {"SPAIN 1", "ITALY 1"}
mask_a = (base["Metodo"] == "Lay_CS_0x1_B365") | (
    (base["Metodo"] == "Lay_CS_1x0_B365") & base["Liga"].isin(LIGAS_A)
)
df_a = base[mask_a].copy()
csv_a = OUT / "hist_proposta_a.csv"
df_a[colunas].to_csv(csv_a, index=False)

# Proposta A2: 1x0 só ITALY 1
LIGAS_A2 = {"ITALY 1"}
mask_a2 = (base["Metodo"] == "Lay_CS_0x1_B365") | (
    (base["Metodo"] == "Lay_CS_1x0_B365") & base["Liga"].isin(LIGAS_A2)
)
df_a2 = base[mask_a2].copy()
csv_a2 = OUT / "hist_proposta_a2.csv"
df_a2[colunas].to_csv(csv_a2, index=False)

print(f"  baseline   : {len(base)} ent ({base['Metodo'].value_counts().get('Lay_CS_1x0_B365',0)} 1x0)")
print(f"  proposta_a : {len(df_a)} ent ({df_a['Metodo'].value_counts().get('Lay_CS_1x0_B365',0)} 1x0 — SPAIN1+ITALY1)")
print(f"  proposta_a2: {len(df_a2)} ent ({df_a2['Metodo'].value_counts().get('Lay_CS_1x0_B365',0)} 1x0 — só ITALY1)")

# ── Roda engine ──────────────────────────────────────────────────────────────
CONFIG = "config_backtest_exec.json"

cenarios = [
    ("baseline",    csv_baseline, "Baseline (1x0 livre)"),
    ("proposta_a",  csv_a,        "Proposta A (1x0: SPAIN1+ITALY1)"),
    ("proposta_a2", csv_a2,       "Proposta A2 (1x0: só ITALY1)"),
]

print()
print("="*65)
print("RODANDO ENGINE (histórico completo ago/2025–abr/2026)")
print("="*65)

for run_id, csv_path, desc in cenarios:
    cmd = [
        sys.executable, "engine_ciclo_producao.py",
        "--input", str(csv_path),
        "--config", CONFIG,
        "--environment", "historico",
        "--output-dir", str(OUT),
        "--run-id", f"hist_{run_id}",
        "--skip-mini-report",
    ]
    print(f"\n[{run_id}] {desc}...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  ERRO: {r.stderr[-800:]}")
    else:
        for line in r.stdout.strip().split("\n"):
            if any(k.lower() in line.lower() for k in
                   ["lucro","win_rate","drawdown","executadas","skipadas","step_up","step_down","fase2"]):
                print(f"  {line.strip()}")

# ── Tabela comparativa ───────────────────────────────────────────────────────
print()
print("="*65)
print("TABELA COMPARATIVA — HISTÓRICO COMPLETO")
print("="*65)
print()

linhas = []
for run_id, _, desc in cenarios:
    sp = OUT / f"hist_{run_id}_summary.json"
    if sp.exists():
        with open(sp) as f:
            s = json.load(f)
        ent = s.get("Entradas_Executadas", 0)
        wr  = s.get("Win_Rate_Executadas_%", 0)
        pnl = s.get("Lucro_Final", 0)
        dd  = s.get("Max_Drawdown_Abs", 0)
        ddp = s.get("Max_Drawdown_%", 0)
        su  = s.get("Step_Ups", 0)
        linhas.append((desc, ent, wr, pnl, dd, ddp, su))

header = "{:<36} {:>5} {:>7} {:>10} {:>9} {:>7} {:>5}".format(
    "Cenário", "Ent", "WR%", "P&L", "DD abs", "DD%", "StepUps"
)
print(header)
print("-"*80)
for desc, ent, wr, pnl, dd, ddp, su in linhas:
    print("{:<36} {:>5} {:>6.1f}% {:>+9.0f} {:>9.0f} {:>6.1f}% {:>5}".format(
        desc, ent, wr, pnl, dd, ddp, su
    ))

# ── Por mês — desempenho mensal comparado ────────────────────────────────────
print()
print("="*65)
print("RESULTADO MENSAL — BASELINE vs PROPOSTA A")
print("="*65)
print()

for run_id, desc in [("baseline","Baseline"),("proposta_a","Proposta A")]:
    ops_p = OUT / f"hist_{run_id}_ops.csv"
    if not ops_p.exists():
        continue
    ops = pd.read_csv(ops_p)
    if "data" not in ops.columns and "Data_Arquivo" not in ops.columns:
        print(f"  {desc}: colunas disponíveis: {ops.columns.tolist()[:10]}")
        continue
    date_col = "Data_Arquivo" if "Data_Arquivo" in ops.columns else "data"
    ops[date_col] = pd.to_datetime(ops[date_col], errors="coerce")
    ops["mes"] = ops[date_col].dt.to_period("M")
    exec_ops = ops[ops["Status_Execucao"] == "EXECUTED"] if "Status_Execucao" in ops.columns else ops
    by_mes = exec_ops.groupby("mes").agg(
        ent=("PnL_Linha", "count"),
        pnl=("PnL_Linha", "sum"),
        greens=("__result", "sum"),
    ).reset_index()
    by_mes["reds"] = by_mes["ent"] - by_mes["greens"].astype(int)
    by_mes["wr%"] = (by_mes["greens"] / by_mes["ent"] * 100).round(1)
    by_mes["pnl"] = by_mes["pnl"].round(0)
    print(f"\n{desc}:")
    print(by_mes[["mes","ent","greens","reds","wr%","pnl"]].to_string(index=False))

# ── 1x0 detail: quais ligas contribuem e como ────────────────────────────────
print()
print("="*65)
print("DETALHE 1x0 NO HISTÓRICO: WR POR LIGA")
print("="*65)

h1x0 = base[base["Metodo"] == "Lay_CS_1x0_B365"]
print(f"\nTotal 1x0 no baseline: {len(h1x0)}")
print()
print("{:<25} {:>6} {:>6} {:>6} {:>7}".format("Liga","Ent","G","R","WR%"))
print("-"*55)
for liga, g in h1x0.groupby("Liga"):
    greens = (g["1/0"] == 1).sum()
    reds   = (g["1/0"] == 0).sum()
    wr = greens/(greens+reds)*100 if (greens+reds) > 0 else 0
    flag = " ⚠️" if wr < 85 else ""
    print("{:<25} {:>6} {:>6} {:>6} {:>6.0f}%{}".format(liga, len(g), greens, reds, wr, flag))

print()
print("FIM.")
