"""
Auditoria diaria de aderencia: jogos do dia (Apostas_YYYYMMDD.xlsx)
vs referencia de backtest (recalculo_sem_combos_usuario.csv + filtros da config).

Gera automaticamente:
1) tabela de match por dia;
2) lista de divergencias com motivo;
3) semaforo (verde/amarelo/vermelho) diario e geral.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
CFG_PATH = ROOT / "config_universo_97.json"
BASE_PATH = ROOT / "recalculo_sem_combos_usuario.csv"
DIARIO_DIR = ROOT / "Apostas_Diarias"
OUT_DIR = ROOT / "Arquivados_Apostas_Diarias" / "Relatorios" / "Comparativo_Automatizado" / "Auditoria_Aderencia_Diaria"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_text(text: object) -> str:
    if not isinstance(text, str):
        return ""
    out = text.lower().strip()
    out = re.sub(r"\s+", " ", out)
    out = out.replace("fc ", "")
    out = out.replace(" cf", "")
    out = out.replace(" (per)", "")
    out = out.replace(" (chi)", "")
    out = out.replace(" (col)", "")
    out = out.replace(" (uru)", "")
    return out.strip()


def _split_match(jogo: object) -> tuple[str, str]:
    if not isinstance(jogo, str):
        return "", ""
    norm = jogo.replace(" VS ", " x ").replace(" vs ", " x ").replace(" Vs ", " x ")
    parts = [p.strip() for p in norm.split(" x ")]
    if len(parts) >= 2:
        return _normalize_text(parts[0]), _normalize_text(parts[1])
    return _normalize_text(norm), ""


def _build_rodo_list(cfg: dict) -> list[dict]:
    merged: dict[int, dict] = {}
    for r in cfg.get("filtros_rodo", []):
        if "id" in r:
            merged[int(r["id"])] = r
    for r in cfg.get("filters", {}).get("filtros_rodo", []):
        if "id" in r:
            merged[int(r["id"])] = r
    for r in cfg.get("filters", {}).get("toxic_cuts", []):
        if "id" in r:
            merged[int(r["id"])] = r
    return list(merged.values())


def _passes_method_filters(row: pd.Series, filtros_metodo: dict) -> tuple[bool, str]:
    metodo = str(row.get("Metodo", "")).strip()
    liga = str(row.get("Liga", "")).strip().upper()
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")

    if metodo not in filtros_metodo:
        return False, "metodo_nao_mapeado"

    fm = filtros_metodo.get(metodo, {})
    if pd.isna(odd):
        return False, "odd_ausente"

    odd_min = fm.get("odd_min")
    odd_max = fm.get("odd_max")
    if odd_min is not None and float(odd) < float(odd_min):
        return False, "fora_faixa_odd"
    if odd_max is not None and float(odd) > float(odd_max):
        return False, "fora_faixa_odd"

    ligas_ok = {str(x).strip().upper() for x in fm.get("ligas_permitidas", [])}
    if ligas_ok and liga not in ligas_ok:
        return False, "fora_da_liga"

    return True, "ok"


def _match_rodo(row: pd.Series, rodos: list[dict]) -> tuple[bool, str]:
    liga = str(row.get("Liga", "")).strip()
    metodo = str(row.get("Metodo", "")).strip()
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False, ""

    for r in rodos:
        if r.get("league") and str(r.get("league")) != liga:
            continue
        if r.get("method_equals") and str(r.get("method_equals")) != metodo:
            continue
        rmin = r.get("odd_min")
        rmax = r.get("odd_max")
        if rmin is not None and float(odd) < float(rmin):
            continue
        if rmax is not None and float(odd) > float(rmax):
            continue
        return True, str(r.get("name", f"Rodo_{r.get('id', 'NA')}"))

    return False, ""


def _semaforo(match_rate: float) -> str:
    if match_rate >= 90:
        return "verde"
    if match_rate >= 70:
        return "amarelo"
    return "vermelho"


def _load_base() -> pd.DataFrame:
    base = pd.read_csv(BASE_PATH)
    base["Data"] = pd.to_datetime(base["Data_Arquivo"], errors="coerce").dt.date
    base["Liga"] = base["Liga"].astype(str).str.strip()
    base["Metodo"] = base["Metodo"].astype(str).str.strip()
    base["Odd_Base"] = pd.to_numeric(base["Odd_Base"], errors="coerce")
    base[["home_norm", "away_norm"]] = base["Jogo"].apply(lambda s: pd.Series(_split_match(s)))
    base["key"] = base.apply(
        lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_norm']}|{r['away_norm']}",
        axis=1,
    )
    base["key_no_method"] = base.apply(
        lambda r: f"{r['Data']}|{r['Liga']}|{r['home_norm']}|{r['away_norm']}",
        axis=1,
    )
    return base


def _load_live_from_xlsx() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in sorted(DIARIO_DIR.glob("Apostas_*.xlsx")):
        stamp = p.stem.replace("Apostas_", "")
        if len(stamp) != 8 or not stamp.isdigit():
            continue
        dt = pd.to_datetime(stamp, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            continue

        x = pd.read_excel(p)
        if x.empty:
            continue

        x["Data"] = dt.date()
        x["Liga"] = x.get("Liga", "").astype(str).str.strip()
        x["Metodo"] = x.get("Metodo", "").astype(str).str.strip()
        if "Odd_Base" in x.columns:
            x["Odd_Base"] = pd.to_numeric(x["Odd_Base"], errors="coerce")
        else:
            x["Odd_Base"] = pd.NA
        x[["home_norm", "away_norm"]] = x["Jogo"].apply(lambda s: pd.Series(_split_match(s)))
        x["key"] = x.apply(
            lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_norm']}|{r['away_norm']}",
            axis=1,
        )
        x["key_no_method"] = x.apply(
            lambda r: f"{r['Data']}|{r['Liga']}|{r['home_norm']}|{r['away_norm']}",
            axis=1,
        )
        x["arquivo_origem"] = p.name
        frames.append(x)

    if not frames:
        return pd.DataFrame(columns=["Data", "Liga", "Metodo", "Jogo", "Odd_Base", "key", "key_no_method", "arquivo_origem"])

    return pd.concat(frames, ignore_index=True)


def main() -> None:
    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    filtros_metodo = cfg.get("runtime_data", {}).get("filtros_metodo", {})
    rodos = _build_rodo_list(cfg)

    base = _load_base()
    live = _load_live_from_xlsx()

    if live.empty:
        print("Nenhuma planilha Apostas_*.xlsx encontrada em Apostas_Diarias.")
        return

    common_dates = sorted(set(base["Data"].dropna()) & set(live["Data"].dropna()))
    if not common_dates:
        print("Nao existem datas em comum entre base e planilhas diarias.")
        return

    base_c = base[base["Data"].isin(common_dates)].copy()
    live_c = live[live["Data"].isin(common_dates)].copy()

    base_keys = set(base_c["key"])
    live_keys = set(live_c["key"])
    inter_keys = base_keys & live_keys

    rows_daily = []
    for d in common_dates:
        b = set(base_c.loc[base_c["Data"] == d, "key"])
        l = set(live_c.loc[live_c["Data"] == d, "key"])
        i = b & l
        match_rate = (len(i) / len(l) * 100.0) if l else 0.0
        coverage_rate = (len(i) / len(b) * 100.0) if b else 0.0
        rows_daily.append(
            {
                "Data": d,
                "live_total": len(l),
                "base_total": len(b),
                "match_exato": len(i),
                "precision_live_to_base_pct": round(match_rate, 2),
                "coverage_base_to_live_pct": round(coverage_rate, 2),
                "semaforo": _semaforo(match_rate),
            }
        )

    daily_df = pd.DataFrame(rows_daily).sort_values("Data")

    # Divergencias do lado live
    only_live_df = live_c[~live_c["key"].isin(inter_keys)].copy()
    motivo_live = []
    rodo_live = []
    for _, r in only_live_df.iterrows():
        has_same_game = bool(
            ((base_c["Data"] == r["Data"]) & (base_c["key_no_method"] == r["key_no_method"]))
            .any()
        )
        if not has_same_game:
            motivo_live.append("nao_veio_na_base")
            rodo_live.append("")
            continue

        ok_metodo, mot_metodo = _passes_method_filters(r, filtros_metodo)
        if not ok_metodo:
            motivo_live.append(mot_metodo)
            rodo_live.append("")
            continue

        blocked, rodo_name = _match_rodo(r, rodos)
        if blocked:
            motivo_live.append("bloqueado_rodo_no_backtest")
            rodo_live.append(rodo_name)
        else:
            motivo_live.append("divergencia_sem_regra_clara")
            rodo_live.append("")

    only_live_df["lado"] = "somente_live"
    only_live_df["motivo"] = motivo_live
    only_live_df["rodo"] = rodo_live

    # Divergencias do lado base
    only_base_df = base_c[~base_c["key"].isin(inter_keys)].copy()
    motivo_base = []
    for _, r in only_base_df.iterrows():
        has_same_game = bool(
            ((live_c["Data"] == r["Data"]) & (live_c["key_no_method"] == r["key_no_method"]))
            .any()
        )
        if has_same_game:
            motivo_base.append("metodo_ou_liga_divergente_no_live")
        else:
            motivo_base.append("nao_veio_no_live")

    only_base_df["lado"] = "somente_base"
    only_base_df["motivo"] = motivo_base
    only_base_df["rodo"] = ""

    div_cols = [
        "Data",
        "lado",
        "motivo",
        "rodo",
        "Liga",
        "Metodo",
        "Jogo",
        "Odd_Base",
        "arquivo_origem",
        "key",
    ]
    diverg_df = pd.concat(
        [
            only_live_df.reindex(columns=div_cols),
            only_base_df.reindex(columns=div_cols),
        ],
        ignore_index=True,
    )

    precision_global = (len(inter_keys) / len(live_keys) * 100.0) if live_keys else 0.0
    recall_global = (len(inter_keys) / len(base_keys) * 100.0) if base_keys else 0.0
    semaforo_global = _semaforo(precision_global)

    motivo_counts = (
        diverg_df["motivo"].value_counts(dropna=False).rename_axis("motivo").reset_index(name="qtd")
    )

    summary = {
        "periodo_comum_inicio": str(common_dates[0]),
        "periodo_comum_fim": str(common_dates[-1]),
        "dias_comuns": len(common_dates),
        "rodos_ativos": len(rodos),
        "total_live": int(len(live_keys)),
        "total_base": int(len(base_keys)),
        "intersecao_exata": int(len(inter_keys)),
        "precision_live_to_base_pct": round(precision_global, 2),
        "recall_base_to_live_pct": round(recall_global, 2),
        "semaforo_global": semaforo_global,
        "regra_semaforo": {
            "verde": ">= 90%",
            "amarelo": ">= 70% e < 90%",
            "vermelho": "< 70%",
        },
    }

    daily_path = OUT_DIR / "match_por_dia.csv"
    diverg_path = OUT_DIR / "divergencias_com_motivo.csv"
    motivos_path = OUT_DIR / "resumo_motivos.csv"
    summary_path = OUT_DIR / "semaforo_aderencia.json"

    daily_df.to_csv(daily_path, index=False)
    diverg_df.to_csv(diverg_path, index=False)
    motivo_counts.to_csv(motivos_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 68)
    print("AUDITORIA DE ADERENCIA DIARIA | LIVE (XLSX) x BASE (BACKTEST)")
    print("=" * 68)
    print(f"Periodo comum: {common_dates[0]} a {common_dates[-1]} ({len(common_dates)} dias)")
    print(f"Live total: {len(live_keys)} | Base total: {len(base_keys)} | Intersecao: {len(inter_keys)}")
    print(f"Precision live->base: {precision_global:.2f}%")
    print(f"Recall base->live: {recall_global:.2f}%")
    print(f"Semaforo global: {semaforo_global.upper()}")

    print("\nTop motivos de divergencia:")
    print(motivo_counts.head(10).to_string(index=False))

    print("\nArquivos gerados:")
    print(f"- {daily_path}")
    print(f"- {diverg_path}")
    print(f"- {motivos_path}")
    print(f"- {summary_path}")


if __name__ == "__main__":
    main()
