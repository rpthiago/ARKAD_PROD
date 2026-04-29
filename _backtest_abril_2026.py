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

# ── Carrega base consolidada fiel (92 jogos) ──────────────────────────────────
consolidado_path = DIR / "apostas_reais_consolidado.xlsx"
if consolidado_path.exists():
    print(f"Carregando base consolidada: {consolidado_path.name}")
    raw = pd.read_excel(consolidado_path)
    raw["__arquivo"] = consolidado_path.name
else:
    print("Base consolidada não encontrada, carregando arquivos individuais...")
    frames = []
    for arq in sorted(glob.glob(str(DIR / "Apostas_*.xlsx"))):
        if "reais" in arq.lower(): continue
        df2 = pd.read_excel(arq)
        df2["__arquivo"] = Path(arq).name
        frames.append(df2)
    raw = pd.concat(frames, ignore_index=True)

# Normalização de colunas
if "Resultado" not in raw.columns and "RESULTADO" in raw.columns:
    raw = raw.rename(columns={"RESULTADO": "Resultado"})
if "Metodo" not in raw.columns and "METODO" in raw.columns:
    raw = raw.rename(columns={"METODO": "Metodo"})
if "Odd_Base" not in raw.columns and "ODD" in raw.columns:
    raw = raw.rename(columns={"ODD": "Odd_Base"})

raw["Resultado"] = raw["Resultado"].fillna("").astype(str).str.strip().str.upper()

def resultado_para_binario(r: str) -> float:
    r_clean = str(r).upper()
    if any(k in r_clean for k in ("GREEN", "VITORIA", "VIT", "WIN")):
        return 1.0
    if any(k in r_clean for k in ("RED", "DERROTA", "DER", "LOSS")):
        return 0.0
    return np.nan

raw["1/0"] = raw["Resultado"].apply(resultado_para_binario)
raw = raw[raw["1/0"].notna()].copy()

print(f"Total de linhas com resultado válido: {len(raw)}")
print(f"Distribuição: {raw['1/0'].value_counts().to_dict()}  (1=green, 0=red)")

# Normalização final para o Engine
if "Data" in raw.columns:
    raw["Data_Arquivo"] = pd.to_datetime(raw["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
else:
    raw["Data_Arquivo"] = raw["__arquivo"].str.extract(r"Apostas_(\d{4})(\d{2})(\d{2})").apply(
        lambda r: f"{r[0]}-{r[1]}-{r[2]}" if pd.notna(r).all() else "2026-04-01", axis=1
    )

raw["Horario_Entrada"] = raw["Hora"] if "Hora" in raw.columns else "00:00"
raw["Horario_Entrada"] = raw["Horario_Entrada"].fillna("00:00").astype(str)
raw["Jogo"] = raw["Jogo"]

# Carrega blacklist
with open("config_rodos_master.json") as f:
    regras_bl = json.load(f)["filtros_rodo"]

def is_bloqueado(row):
    for r in regras_bl:
        liga_r  = r.get("league", "").strip().upper()
        met_r   = r.get("method_equals", "").strip()
        odd_min = r.get("odd_min") or -99
        odd_max = r.get("odd_max") or 999
        
        # Filtra pela Odd_Real_Pega se disponível, senão Odd_Base
        val_odd = row["Odd_Real_Pega"] if pd.notna(row["Odd_Real_Pega"]) else row["Odd_Base"]
        
        if (str(row["Liga"]).strip().upper() == liga_r and
            str(row["Metodo"]).strip() == met_r and
            odd_min <= val_odd <= odd_max):
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
print(f"\nCenário A: {len(raw)} entradas - {csv_a.name}")

# Carrega Whitelist do config_prod_v1.json
with open("config_prod_v1.json") as f:
    cfg_prod = json.load(f)
    whitelist_0x1 = set(cfg_prod["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]["ligas_permitidas"])
    whitelist_1x0 = set(cfg_prod["runtime_data"]["filtros_metodo"]["Lay_CS_1x0_B365"]["ligas_permitidas"])

# ── Coluna auxiliar para filtro de Odd (Real Pega > Base) ───────────────────
raw["val_odd"] = raw["Odd_Real_Pega"].fillna(raw["Odd_Base"])

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO B — filtro odd + blacklist + whitelist
# ────────────────────────────────────────────────────────────────────────────
def passa_whitelist(row):
    liga = str(row["Liga"]).strip().upper()
    metodo = str(row["Metodo"]).strip()
    if metodo == "Lay_CS_0x1_B365":
        return liga in whitelist_0x1
    if metodo == "Lay_CS_1x0_B365":
        return liga in whitelist_1x0
    return False

mask_0x1 = (raw["Metodo"]=="Lay_CS_0x1_B365") & (raw["val_odd"] >= 8.0) & (raw["val_odd"] <= 11.5)
mask_1x0 = (raw["Metodo"]=="Lay_CS_1x0_B365") & (raw["val_odd"] >= 4.5) & (raw["val_odd"] <= 11.5)
filt_b = raw[(mask_0x1 | mask_1x0)].copy()

# Aplica Whitelist
filt_b = filt_b[filt_b.apply(passa_whitelist, axis=1)].copy()

# Aplica Blacklist (Rodo)
filt_b["bloq"] = filt_b.apply(is_bloqueado, axis=1)
filt_b = filt_b[~filt_b["bloq"]].copy()

csv_b = OUT / "abril_cenario_B_filtros.csv"
filt_b[colunas_engine].to_csv(csv_b, index=False)
print(f"Cenário B: {len(filt_b)} entradas - {csv_b.name}")

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO C — filtros + 1x0 restrito às ligas históricas válidas
# ────────────────────────────────────────────────────────────────────────────
LIGAS_1x0_VALIDADAS = {"SPAIN 1", "ITALY 1", "SPAIN 2"}
mask_0x1_c = (filt_b["Metodo"] == "Lay_CS_0x1_B365")
mask_1x0_c = (filt_b["Metodo"] == "Lay_CS_1x0_B365") & filt_b["Liga"].str.strip().str.upper().isin(LIGAS_1x0_VALIDADAS)
filt_c = filt_b[mask_0x1_c | mask_1x0_c].copy()

csv_c = OUT / "abril_cenario_C_1x0_apenas_hist.csv"
filt_c[colunas_engine].to_csv(csv_c, index=False)
print(f"Cenário C: {len(filt_c)} entradas - {csv_c.name}")

# ────────────────────────────────────────────────────────────────────────────
# CENÁRIO D — somente 0x1 (sem nenhum 1x0)
# ────────────────────────────────────────────────────────────────────────────
filt_d = filt_b[filt_b["Metodo"] == "Lay_CS_0x1_B365"].copy()
csv_d = OUT / "abril_cenario_D_somente_0x1.csv"
filt_d[colunas_engine].to_csv(csv_d, index=False)
print(f"Cenário D: {len(filt_d)} entradas - {csv_d.name}")

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
