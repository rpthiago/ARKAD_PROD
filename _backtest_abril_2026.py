"""
Backtest de abril/2026 usando resultados reais das planilhas.

Cenários:
  A) Sem filtros, engine progressivo (tudo que foi apostado)
  B) Filtros corretos (odd range + blacklist), engine progressivo
  C) Filtros corretos + 1x0 restrito às ligas históricas (SPAIN 1 + ITALY 1), engine progressivo

Usa --environment historico no engine (sem slippage/liquidez - resultados já são reais).
"""
import glob, subprocess, sys, json
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(".")
DIR  = Path("Apostas_Diarias")
OUT  = Path("Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026")
OUT.mkdir(parents=True, exist_ok=True)

# ── Carrega todas as planilhas de abril ─────────────────────────────────────
frames = []
for arq in sorted(glob.glob(str(DIR / "Apostas_*.xlsx"))):
    df2 = pd.read_excel(arq)
    df2["__arquivo"] = Path(arq).name
    frames.append(df2)
raw = pd.concat(frames, ignore_index=True)
raw["Resultado"] = raw["Resultado"].fillna("").astype(str).str.strip().str.upper()

def resultado_para_binario(r: str) -> float:
    if any(k in r for k in ("GREEN", "VITORIA", "VIT")):
        return 1.0
    if any(k in r for k in ("RED", "DERROTA", "DER")):
        return 0.0
    return np.nan

raw["1/0"] = raw["Resultado"].apply(resultado_para_binario)

# Extrai data do nome do arquivo
raw["Data_Arquivo"] = raw["__arquivo"].str.extract(r"Apostas_(\d{4})(\d{2})(\d{2})").apply(
    lambda r: f"{r[0]}-{r[1]}-{r[2]}", axis=1
)
raw["Horario_Entrada"] = raw["Hora"].astype(str)
raw["Jogo"] = raw["Jogo"]

# Remove pendentes/sem resultado
raw = raw[raw["1/0"].notna()].copy()

print(f"Total de linhas com resultado válido: {len(raw)}")
print(f"Distribuição: {raw['1/0'].value_counts().to_dict()}  (1=green, 0=red)")

# Carrega blacklist
with open("config_rodos_master.json") as f:
    regras_bl = json.load(f)["filtros_rodo"]

def is_bloqueado(row):
    for r in regras_bl:
        liga_r  = r.get("league", "").strip().upper()
        met_r   = r.get("method_equals", "").strip()
        odd_min = r.get("odd_min") or -99
        odd_max = r.get("odd_max") or 999
        if (str(row["Liga"]).strip().upper() == liga_r and
            str(row["Metodo"]).strip() == met_r and
            odd_min <= row["Odd_Base"] <= odd_max):
            return True
    return False

# ── Monta coluna padrão engine ───────────────────────────────────────────────
colunas_engine = ["Data_Arquivo","Horario_Entrada","Liga","Jogo","Metodo",
                  "Odd_Base","Odd_Real_Pega","1/0"]

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO A — sem filtros (tudo apostado)
# ────────────────────────────────────────────────────────────────────────────
csv_a = OUT / "abril_cenario_A_sem_filtro.csv"
raw[colunas_engine].to_csv(csv_a, index=False)
print(f"\nCenário A: {len(raw)} entradas → {csv_a.name}")

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO B — filtro odd + blacklist
# ────────────────────────────────────────────────────────────────────────────
mask_0x1 = (raw["Metodo"]=="Lay_CS_0x1_B365") & (raw["Odd_Base"] >= 8.0) & (raw["Odd_Base"] <= 11.5)
mask_1x0 = (raw["Metodo"]=="Lay_CS_1x0_B365") & (raw["Odd_Base"] >= 4.5) & (raw["Odd_Base"] <= 11.5)
filt_b = raw[mask_0x1 | mask_1x0].copy()
filt_b["bloq"] = filt_b.apply(is_bloqueado, axis=1)
filt_b = filt_b[~filt_b["bloq"]].copy()

csv_b = OUT / "abril_cenario_B_filtros.csv"
filt_b[colunas_engine].to_csv(csv_b, index=False)
print(f"Cenário B: {len(filt_b)} entradas → {csv_b.name}")

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO C — filtros + 1x0 restrito às ligas históricas válidas
#   Histórico validou 1x0 apenas em SPAIN 1 (84% WR, 52 ent) e ITALY 1 (90% WR, 21 ent)
# ────────────────────────────────────────────────────────────────────────────
LIGAS_1x0_VALIDADAS = {"SPAIN 1", "ITALY 1"}
mask_0x1_c = (filt_b["Metodo"] == "Lay_CS_0x1_B365")
mask_1x0_c = (filt_b["Metodo"] == "Lay_CS_1x0_B365") & filt_b["Liga"].str.strip().str.upper().isin(LIGAS_1x0_VALIDADAS)
filt_c = filt_b[mask_0x1_c | mask_1x0_c].copy()

csv_c = OUT / "abril_cenario_C_1x0_apenas_hist.csv"
filt_c[colunas_engine].to_csv(csv_c, index=False)
print(f"Cenário C: {len(filt_c)} entradas → {csv_c.name}")
print(f"  (0x1: {mask_0x1_c.sum()} | 1x0 históricas: {mask_1x0_c[mask_1x0_c].count()})")

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO D — somente 0x1 (sem nenhum 1x0)
# ────────────────────────────────────────────────────────────────────────────
filt_d = filt_b[filt_b["Metodo"] == "Lay_CS_0x1_B365"].copy()
csv_d = OUT / "abril_cenario_D_somente_0x1.csv"
filt_d[colunas_engine].to_csv(csv_d, index=False)
print(f"Cenário D: {len(filt_d)} entradas → {csv_d.name}")

# ── Roda o engine para cada cenário ─────────────────────────────────────────
config = "config_backtest_exec.json"  # rampa on, slippage off, liquidity off
resultados_dir = str(OUT)

cenarios = [
    ("A_sem_filtro",       csv_a, "Sem filtros (tudo apostado)"),
    ("B_filtros",          csv_b, "Filtros corretos (odd + blacklist)"),
    ("C_1x0_hist",         csv_c, "Filtros + 1x0 só SPAIN1/ITALY1"),
    ("D_somente_0x1",      csv_d, "Somente 0x1"),
]

print("\n" + "="*65)
print("RODANDO ENGINE PARA CADA CENÁRIO")
print("="*65)

for run_id, csv_path, descricao in cenarios:
    cmd = [
        sys.executable, "engine_ciclo_producao.py",
        "--input", str(csv_path),
        "--config", config,
        "--environment", "historico",
        "--output-dir", resultados_dir,
        "--run-id", f"abril_2026_{run_id}",
        "--skip-mini-report",
    ]
    print(f"\n[{run_id}] {descricao}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERRO:\n{result.stderr[-1500:]}")
    else:
        # Extrai resumo do stdout
        lines = result.stdout.strip().split("\n")
        for line in lines:
            if any(k in line.lower() for k in ["lucro", "profit", "win_rate", "win rate", "drawdown", "entradas", "greens", "reds"]):
                print(f"  {line.strip()}")

# ── Lê os summaries gerados e faz tabela comparativa ───────────────────────
print("\n" + "="*65)
print("TABELA COMPARATIVA DOS 4 CENÁRIOS")
print("="*65)

rows = []
for run_id, csv_path, descricao in cenarios:
    summary_path = OUT / f"abril_2026_{run_id}_summary.json"
    if summary_path.exists():
        with open(summary_path) as f:
            s = json.load(f)
        rows.append({
            "Cenário": descricao,
            "Entradas": s.get("executed_rows", s.get("total_rows", "?")),
            "Greens": s.get("wins", "?"),
            "Reds": s.get("losses", "?"),
            "WR%": round(s.get("win_rate", 0) * 100, 1),
            "P&L (R$)": round(s.get("lucro_acumulado", 0), 2),
            "DD abs (R$)": round(s.get("drawdown_abs", 0), 2),
        })
    else:
        # Fallback: lê ops CSV e calcula na mão
        ops_path = OUT / f"abril_2026_{run_id}_ops.csv"
        if ops_path.exists():
            ops = pd.read_csv(ops_path)
            exec_ops = ops[ops.get("status", pd.Series()).eq("EXECUTED")] if "status" in ops.columns else ops
            rows.append({"Cenário": descricao, "Nota": f"ops encontrado: {len(ops)} linhas"})
        else:
            rows.append({"Cenário": descricao, "Nota": "sem summary gerado"})

if rows and "P&L (R$)" in rows[0]:
    df_res = pd.DataFrame(rows)
    print(df_res.to_string(index=False))
else:
    for r in rows:
        print(r)

print("\nFIM.")
