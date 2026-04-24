"""
Backtest de auditoria do Rodo (blacklist).
Ago/2025 → hoje.

Passo 1: filtros básicos apenas (odd range + ligas_permitidas)  → sem rodo
Passo 2: com rodo atual
Passo 3: auditoria regra-a-regra (quanto cada regra impacta o P&L)
"""
import subprocess, sys, json, pandas as pd, numpy as np
from pathlib import Path

ROOT     = Path(".")
CSV_HIST = ROOT / "recalculo_sem_combos_usuario.csv"
OUT      = Path("Arquivados_Apostas_Diarias/Relatorios/Auditoria_Rodo_2025")
OUT.mkdir(parents=True, exist_ok=True)

# ── 1. Carrega histórico ─────────────────────────────────────────────────────
hist = pd.read_csv(CSV_HIST)
hist["Data_Arquivo"] = pd.to_datetime(hist["Data_Arquivo"])
hist["Metodo"]       = hist["Metodo"].str.strip()
hist["Liga"]         = hist["Liga"].str.strip().str.upper()

# ── 2. Filtros básicos ───────────────────────────────────────────────────────
LIGAS_1x0 = {"SPAIN 1", "ITALY 1", "SPAIN 2"}

mask_0x1 = (hist["Metodo"] == "Lay_CS_0x1_B365") & (hist["Odd_Base"] >= 8.0) & (hist["Odd_Base"] <= 11.5)
mask_1x0 = (
    (hist["Metodo"] == "Lay_CS_1x0_B365") &
    (hist["Odd_Base"] >= 4.5) & (hist["Odd_Base"] <= 11.5) &
    hist["Liga"].isin(LIGAS_1x0)
)
base = hist[mask_0x1 | mask_1x0].copy()

print(f"Base histórica (filtros básicos, SEM rodo): {len(base)} entradas")
print(f"  0x1: {(base['Metodo']=='Lay_CS_0x1_B365').sum()}")
print(f"  1x0: {(base['Metodo']=='Lay_CS_1x0_B365').sum()} (apenas SPAIN1/ITALY1/SPAIN2)")

colunas = ["Data_Arquivo", "Horario_Entrada", "Liga", "Jogo", "Metodo", "Odd_Base", "1/0"]

# CSV sem rodo
csv_sem_rodo = OUT / "hist_sem_rodo.csv"
base[colunas].to_csv(csv_sem_rodo, index=False)

# ── 3. Aplica blacklist ──────────────────────────────────────────────────────
with open("config_rodos_master.json") as f:
    cfg_rodo = json.load(f)
regras = cfg_rodo["filtros_rodo"]

def is_bloqueado(row, regras):
    for r in regras:
        liga_r  = r.get("league", "").strip().upper()
        met_r   = r.get("method_equals", "").strip()
        odd_min = r.get("odd_min") if r.get("odd_min") is not None else -99
        odd_max = r.get("odd_max") if r.get("odd_max") is not None else 999
        if (row["Liga"] == liga_r and row["Metodo"] == met_r
                and odd_min <= row["Odd_Base"] <= odd_max):
            return True
    return False

base["bloq"]    = base.apply(lambda r: is_bloqueado(r, regras), axis=1)
base_com_rodo   = base[~base["bloq"]].copy()
bloqueadas      = base[base["bloq"]].copy()

print(f"\nCom rodo aplicado: {len(base_com_rodo)} entradas ({len(bloqueadas)} bloqueadas pelo rodo)")

csv_com_rodo = OUT / "hist_com_rodo.csv"
base_com_rodo[colunas].to_csv(csv_com_rodo, index=False)

# ── 4. Roda engine nos dois cenários ─────────────────────────────────────────
CONFIG = "config_backtest_exec.json"

cenarios = [
    ("sem_rodo", csv_sem_rodo, "Filtros básicos (SEM rodo)"),
    ("com_rodo", csv_com_rodo, "Filtros básicos + rodo atual"),
]

print()
print("="*65)
print("RODANDO ENGINE")
print("="*65)

summaries = {}
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
        print(f"  ERRO: {r.stderr[-600:]}")
        continue
    sp = OUT / f"hist_{run_id}_summary.json"
    if sp.exists():
        with open(sp) as f:
            summaries[run_id] = json.load(f)
    for line in r.stdout.strip().split("\n"):
        if any(k.lower() in line.lower() for k in
               ["lucro","win_rate","drawdown","executadas","step_up","fase2"]):
            print(f"  {line.strip()}")

# ── 5. Tabela comparativa ─────────────────────────────────────────────────────
print()
print("="*65)
print("COMPARATIVO: SEM RODO vs COM RODO ATUAL")
print("="*65)
print()
hdr = "{:<35} {:>5} {:>7} {:>10} {:>9} {:>7}".format(
    "Cenário","Ent","WR%","P&L","DD abs","DD%")
print(hdr)
print("-"*75)
for run_id, desc in [("sem_rodo","Sem rodo"), ("com_rodo","Com rodo atual")]:
    s = summaries.get(run_id, {})
    print("{:<35} {:>5} {:>6.1f}% {:>+9.0f} {:>9.0f} {:>6.1f}%".format(
        desc,
        s.get("Entradas_Executadas",0),
        s.get("Win_Rate_Executadas_%",0),
        s.get("Lucro_Final",0),
        s.get("Max_Drawdown_Abs",0),
        s.get("Max_Drawdown_%",0),
    ))

# ── 6. Auditoria regra-a-regra ───────────────────────────────────────────────
print()
print("="*65)
print("AUDITORIA RODO — IMPACTO DE CADA REGRA")
print("="*65)
print()

# Para cada regra, calcula stats das entradas bloqueadas
print("{:<50} {:>5} {:>5} {:>5} {:>7} {:>10}".format(
    "Regra","Ent","G","R","WR%","PnL_flat"))
print("-"*90)

total_bloq_pnl = 0
for reg in regras:
    liga_r  = reg.get("league", "").strip().upper()
    met_r   = reg.get("method_equals", "").strip()
    odd_min = reg.get("odd_min") if reg.get("odd_min") is not None else -99
    odd_max = reg.get("odd_max") if reg.get("odd_max") is not None else 999
    nome    = reg.get("name", reg.get("id", "?"))

    sub = base[
        (base["Liga"] == liga_r) &
        (base["Metodo"] == met_r) &
        (base["Odd_Base"] >= odd_min) &
        (base["Odd_Base"] <= odd_max)
    ]

    if len(sub) == 0:
        # regra sem impacto no universo atual
        print("{:<50} {:>5}  (sem entradas no dataset atual)".format(str(nome)[:50], 0))
        continue

    greens  = (sub["1/0"] == 1).sum()
    reds    = (sub["1/0"] == 0).sum()
    wr      = greens / len(sub) * 100 if len(sub) > 0 else 0
    # P&L flat R$500 aproximado (lay: green = +stake/odd, red = -stake)
    pnl_est = sub.apply(
        lambda rw: 500 / (rw["Odd_Base"] - 1) if rw["1/0"] == 1 else -500,
        axis=1
    ).sum()
    total_bloq_pnl += pnl_est

    flag = ""
    if wr >= 90 and len(sub) >= 5:
        flag = " <- REVISAR (WR ok, sendo bloqueado)"
    elif wr < 80:
        flag = " TOXICO"

    print("{:<50} {:>5} {:>5} {:>5} {:>6.0f}% {:>+9.0f}{}".format(
        str(nome)[:50], len(sub), int(greens), int(reds), wr, pnl_est, flag
    ))

print()
print(f"PnL estimado total das entradas bloqueadas: R${total_bloq_pnl:+.0f}")
print("(negativo = rodo está retirando entradas ruins → bom)")
print("(positivo = rodo está retirando entradas boas → revisar)")

# ── 7. Top combos ruins sem rodo (candidatos a novas regras) ─────────────────
print()
print("="*65)
print("COMBOS COM MENOR WR NO UNIVERSO SEM RODO (candidatos à blacklist)")
print("="*65)
print()

# Apenas 0x1 (1x0 já está restrito a 3 ligas com WR ≥ 90%)
sub_0x1 = base[base["Metodo"] == "Lay_CS_0x1_B365"].copy()

combos = []
for (liga, met), g in sub_0x1.groupby(["Liga", "Metodo"]):
    if len(g) < 5:
        continue
    greens = (g["1/0"] == 1).sum()
    reds   = (g["1/0"] == 0).sum()
    wr     = greens / len(g) * 100
    odd_med = g["Odd_Base"].mean()
    breakeven = (1 - 1/(odd_med)) * 100
    pnl_est = g.apply(
        lambda rw: 500/(rw["Odd_Base"]-1) if rw["1/0"]==1 else -500, axis=1
    ).sum()
    combos.append(dict(Liga=liga, Met=met, Ent=len(g), G=int(greens),
                       R=int(reds), WR=wr, OddMed=odd_med,
                       BE=breakeven, PnL=pnl_est))

df_combos = pd.DataFrame(combos).sort_values("PnL")
print("Piores P&L (flat R$500), min 5 entradas:")
print()
print("{:<25} {:>5} {:>5} {:>5} {:>7} {:>7} {:>7} {:>10}".format(
    "Liga","Ent","G","R","WR%","OddMed","BE%","PnL"))
print("-"*80)
for _, row in df_combos.head(15).iterrows():
    na_bl = any(
        row["Liga"] == r.get("league","").upper() and
        row["Met"]  == r.get("method_equals","") and
        (r.get("odd_min") or -99) <= row["OddMed"] <= (r.get("odd_max") or 999)
        for r in regras
    )
    tag = " [ja no rodo]" if na_bl else " <- NOVO CANDIDATO"
    print("{:<25} {:>5} {:>5} {:>5} {:>6.0f}% {:>7.2f} {:>6.0f}% {:>+9.0f}{}".format(
        row["Liga"][:25], int(row["Ent"]), row["G"], row["R"],
        row["WR"], row["OddMed"], row["BE"], row["PnL"], tag
    ))

print()
print("FIM.")
