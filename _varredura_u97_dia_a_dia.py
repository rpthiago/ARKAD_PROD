"""
Varredura dia a dia: base CSV × filtros Universo 97
Período: 2025-08-01 a 2026-03-31
Saída: Arquivados_Apostas_Diarias/Relatorios/Comparativo_Automatizado/API_vs_Base_u97_202508_202603/
"""

import json, pathlib, pandas as pd

ROOT = pathlib.Path(__file__).parent
OUT_DIR = ROOT / "Arquivados_Apostas_Diarias/Relatorios/Comparativo_Automatizado/API_vs_Base_u97_202508_202603"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 1. Carregar config ────────────────────────────────────────────────────────
cfg = json.loads((ROOT / "config_universo_97.json").read_text(encoding="utf-8"))

filtros_metodo = cfg["runtime_data"]["filtros_metodo"]

# Coletar todos os rodos (3 fontes, deduplica por id)
_rodo_map: dict[int, dict] = {}
for r in cfg.get("filtros_rodo", []):
    _rodo_map[r["id"]] = r
for r in cfg.get("filters", {}).get("filtros_rodo", []):
    _rodo_map[r["id"]] = r
for r in cfg.get("filters", {}).get("toxic_cuts", []):
    _rodo_map[r["id"]] = r
RODOS = list(_rodo_map.values())
print(f"Total rodos carregados: {len(RODOS)}")

# ── 2. Carregar base CSV ───────────────────────────────────────────────────────
df = pd.read_csv(ROOT / "recalculo_sem_combos_usuario.csv")
df["_data"] = pd.to_datetime(df["Data_Arquivo"], errors="coerce").dt.date
df["Odd_Base"] = pd.to_numeric(df["Odd_Base"], errors="coerce")
df["1/0"] = pd.to_numeric(df["1/0"], errors="coerce")
df["PnL_Linha"] = pd.to_numeric(df["PnL_Linha"], errors="coerce")

# Filtrar período
DATA_INI = pd.Timestamp("2025-08-01").date()
DATA_FIM = pd.Timestamp("2026-03-31").date()
df = df[(df["_data"] >= DATA_INI) & (df["_data"] <= DATA_FIM)].copy()
print(f"Linhas no período: {len(df)}")

# ── 3. Funções de filtro ──────────────────────────────────────────────────────

def passa_metodo(row: pd.Series) -> bool:
    """Verifica se a linha passa nos filtros de método (liga + odd range)."""
    metodo = str(row.get("Metodo", ""))
    liga   = str(row.get("Liga", ""))
    odd    = float(row.get("Odd_Base", 0) or 0)
    if metodo not in filtros_metodo:
        return False
    fm = filtros_metodo[metodo]
    ligas = fm.get("ligas_permitidas", [])
    if liga not in ligas:
        return False
    odd_min = fm.get("odd_min") or 0.0
    odd_max = fm.get("odd_max") or 999.0
    return odd_min <= odd <= odd_max


def bloqueado_por_rodo(row: pd.Series) -> tuple[bool, str]:
    """Retorna (bloqueado, nome_do_rodo) se algum rodo bate."""
    liga   = str(row.get("Liga", ""))
    metodo = str(row.get("Metodo", ""))
    odd    = float(row.get("Odd_Base", 0) or 0)
    for r in RODOS:
        if r.get("league") and r["league"] != liga:
            continue
        if r.get("method_equals") and r["method_equals"] != metodo:
            continue
        rmin = r.get("odd_min") or 0.0
        rmax = r.get("odd_max") or 999.0
        if odd >= rmin and odd <= rmax:
            return True, r.get("name", f"Rodo_{r['id']}")
    return False, ""

# ── 4. Aplicar filtros ────────────────────────────────────────────────────────
df["_passa_metodo"] = df.apply(passa_metodo, axis=1)
df_cand = df[df["_passa_metodo"]].copy()
print(f"Candidatos após filtro de método: {len(df_cand)}")

rodo_results = df_cand.apply(bloqueado_por_rodo, axis=1)
df_cand["_bloqueado"] = rodo_results.apply(lambda x: x[0])
df_cand["_rodo_nome"] = rodo_results.apply(lambda x: x[1])

df_exec = df_cand[~df_cand["_bloqueado"]].copy()
df_skip = df_cand[df_cand["_bloqueado"]].copy()

print(f"EXECUTADOS pelo u97: {len(df_exec)}")
print(f"BLOQUEADOS por rodo: {len(df_skip)}")

# ── 5. Sumário diário ─────────────────────────────────────────────────────────
def daily_summary(gdf: pd.DataFrame) -> pd.Series:
    n = len(gdf)
    wins = (gdf["1/0"] == 1.0).sum()
    pnl  = gdf["PnL_Linha"].sum()
    wr   = round(wins / n * 100, 2) if n > 0 else 0.0
    return pd.Series({"n_executados": n, "wins": wins, "WR%": wr, "PnL": round(pnl, 2)})

daily = df_exec.groupby("_data").apply(daily_summary).reset_index()
daily.rename(columns={"_data": "Data"}, inplace=True)

# Dias com candidatos mas todos bloqueados
cand_daily = df_cand.groupby("_data").size().reset_index(name="n_candidatos")
daily = cand_daily.merge(daily, left_on="_data", right_on="Data", how="left")
daily["n_executados"] = daily["n_executados"].fillna(0).astype(int)
daily["wins"]         = daily["wins"].fillna(0).astype(int)
daily["WR%"]          = daily["WR%"].fillna(0.0)
daily["PnL"]          = daily["PnL"].fillna(0.0)
daily["n_bloqueados"] = daily["n_candidatos"] - daily["n_executados"]
daily.drop(columns=["Data"], errors="ignore", inplace=True)
daily.rename(columns={"_data": "Data"}, inplace=True)
daily = daily[["Data", "n_candidatos", "n_executados", "n_bloqueados", "wins", "WR%", "PnL"]]
daily = daily.sort_values("Data")

# ── 6. Detalhes por jogo executado ───────────────────────────────────────────
detail_cols = ["Data_Arquivo", "Horario_Entrada", "Liga", "Jogo", "Metodo", "Odd_Base", "1/0", "PnL_Linha", "Placar_FT"]
avail_cols  = [c for c in detail_cols if c in df_exec.columns]
df_detail   = df_exec[avail_cols].copy().rename(columns={"Data_Arquivo": "Data"})

# ── 7. Rodos que mais bloquearam ─────────────────────────────────────────────
rodo_counts = (
    df_skip["_rodo_nome"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "Rodo", "_rodo_nome": "n_bloqueados", "count": "n_bloqueados"})
)

# ── 8. Sumário global ─────────────────────────────────────────────────────────
total_exec = len(df_exec)
total_wins = (df_exec["1/0"] == 1.0).sum()
wr_global  = round(total_wins / total_exec * 100, 2) if total_exec > 0 else 0.0
pnl_global = round(df_exec["PnL_Linha"].sum(), 2)

summary = {
    "periodo": f"{DATA_INI} a {DATA_FIM}",
    "total_linhas_base": len(df),
    "total_candidatos": len(df_cand),
    "total_executados_u97": total_exec,
    "total_bloqueados_rodo": len(df_skip),
    "wins": int(total_wins),
    "WR_global_pct": wr_global,
    "PnL_total": pnl_global,
    "dias_com_apostas": int((daily["n_executados"] > 0).sum()),
    "rodos_ativos": len(RODOS),
}

# ── 9. Salvar ─────────────────────────────────────────────────────────────────
daily.to_csv(OUT_DIR / "daily_summary.csv", index=False)
df_detail.to_csv(OUT_DIR / "detail_executados.csv", index=False)
rodo_counts.to_csv(OUT_DIR / "rodo_bloqueios.csv", index=False)
(OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

# ── 10. Imprimir resultados ───────────────────────────────────────────────────
print("\n" + "="*60)
print("  VARREDURA UNIVERSO 97 — AGO/2025 a MAR/2026")
print("="*60)
print(f"  Candidatos totais  : {len(df_cand):>6}")
print(f"  Executados u97     : {total_exec:>6}")
print(f"  Bloqueados (rodo)  : {len(df_skip):>6}")
print(f"  WR global          : {wr_global:>6.2f}%")
print(f"  P&L total          : R$ {pnl_global:>10,.2f}")
print(f"  Dias com apostas   : {summary['dias_com_apostas']:>6}")
print("="*60)

print("\n--- SUMÁRIO MENSAL ---")
df_exec["Mes"] = pd.to_datetime(df_exec["Data_Arquivo"], errors="coerce").dt.to_period("M").astype(str)
mensal = df_exec.groupby("Mes").apply(lambda g: pd.Series({
    "n": len(g),
    "wins": int((g["1/0"] == 1.0).sum()),
    "WR%": round((g["1/0"] == 1.0).sum() / len(g) * 100, 2) if len(g) > 0 else 0,
    "PnL": round(g["PnL_Linha"].sum(), 2)
})).reset_index()
print(mensal.to_string(index=False))

print(f"\nTop 10 rodos que mais bloquearam:")
print(rodo_counts.head(10).to_string(index=False))

print(f"\nArquivos salvos em: {OUT_DIR}")
