"""
Auditor diario de aderencia: jogos do dia (Apostas_YYYYMMDD.xlsx) vs base de referencia
(backtest em recalculo_sem_combos_usuario.csv).

Gera automaticamente:
1) tabela de match por dia
2) lista de divergencias com motivo
3) semaforo (verde/amarelo/vermelho) de aderencia

Uso:
  python _auditor_aderencia_diaria.py
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
        merged[int(r.get("id", -1))] = r
    for r in cfg.get("filters", {}).get("filtros_rodo", []):
        merged[int(r.get("id", -1))] = r
    for r in cfg.get("filters", {}).get("toxic_cuts", []):
        merged[int(r.get("id", -1))] = r
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

    live = pd.concat(frames, ignore_index=True)
    if "Odd_Base" in live.columns:
        live["Odd_Base"] = pd.to_numeric(live["Odd_Base"], errors="coerce")
    return live


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

    # Tabela de match por dia
    rows_daily = []
    for d in common_dates:
        b = set(base_c.loc[base_c["Data"] == d, "key"])
        l = set(live_c.loc[live_c["Data"] == d, "key"])
        i = b & l
        match_rate = (len(i) / len(l) * 100.0) if l else 0.0
        rows_daily.append(
            {
                "Data": d,
                "live_total": len(l),
                "base_total": len(b),
                "match_exato": len(i),
                "precision_live_to_base_pct": round(match_rate, 2),
                "semaforo": _semaforo(match_rate),
            }
        )

    daily_df = pd.DataFrame(rows_daily).sort_values("Data")

    # Divergencias com motivo - lado live (esta no dia, nao bate no backtest)
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

    # Divergencias com motivo - lado base (esta no backtest, nao apareceu no dia)
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
    diverg_live = only_live_df.reindex(columns=div_cols)
    diverg_base = only_base_df.reindex(columns=div_cols)
    diverg_df = pd.concat([diverg_live, diverg_base], ignore_index=True)

    # Sumario geral
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

    # Persistencia
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
"""
Auditor diário de aderência: jogos do dia (Apostas_*.xlsx) vs referência do backtest (base CSV + filtros da config).

Gera automaticamente:
1) tabela de match por dia;
2) lista de divergências com motivo;
3) semáforo diário e geral de aderência.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_BASE = ROOT / "recalculo_sem_combos_usuario.csv"
DEFAULT_CFG = ROOT / "config_universo_97.json"
DEFAULT_APOSTAS_DIR = ROOT / "Apostas_Diarias"
DEFAULT_OUT_DIR = (
    ROOT
    / "Arquivados_Apostas_Diarias"
    / "Relatorios"
    / "Comparativo_Automatizado"
    / "Auditoria_Aderencia_Diaria"
)


@dataclass(frozen=True)
class FiltroMetodo:
    odd_min: float
    odd_max: float
    ligas_permitidas: set[str]


def _norm_text(value: object) -> str:
    if value is None:
        return ""
    txt = str(value).strip().upper()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _norm_team(team: object) -> str:
    txt = _norm_text(team)
    replacements = [" FC ", " CF ", "(PER)", "(CHI)", "(COL)", "(URU)"]
    txt = f" {txt} "
    for rep in replacements:
        txt = txt.replace(rep, " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _split_jogo(jogo: object) -> tuple[str, str]:
    txt = str(jogo or "")
    txt = txt.replace(" vs ", " x ").replace(" VS ", " x ").replace(" Vs ", " x ")
    parts = [p.strip() for p in txt.split(" x ") if p.strip()]
    if len(parts) >= 2:
        return _norm_team(parts[0]), _norm_team(parts[1])
    single = _norm_team(txt)
    return single, ""


def _extract_rodos(cfg: dict) -> list[dict]:
    rodo_map: dict[int, dict] = {}
    for rodo in cfg.get("filtros_rodo", []) or []:
        if "id" in rodo:
            rodo_map[int(rodo["id"])] = rodo
    for rodo in (cfg.get("filters", {}) or {}).get("filtros_rodo", []) or []:
        if "id" in rodo:
            rodo_map[int(rodo["id"])] = rodo
    for rodo in (cfg.get("filters", {}) or {}).get("toxic_cuts", []) or []:
        if "id" in rodo:
            rodo_map[int(rodo["id"])] = rodo
    return list(rodo_map.values())


def _parse_filtros_metodo(cfg: dict) -> dict[str, FiltroMetodo]:
    filtros = (cfg.get("runtime_data", {}) or {}).get("filtros_metodo", {}) or {}
    out: dict[str, FiltroMetodo] = {}
    for metodo, bloco in filtros.items():
        ligas = {_norm_text(x) for x in bloco.get("ligas_permitidas", []) or []}
        odd_min = float(bloco.get("odd_min") or 0.0)
        odd_max = float(bloco.get("odd_max") or 999.0)
        out[_norm_text(metodo)] = FiltroMetodo(
            odd_min=odd_min,
            odd_max=odd_max,
            ligas_permitidas=ligas,
        )
    return out


def _passes_method_filter(row: pd.Series, filtros: dict[str, FiltroMetodo]) -> bool:
    metodo = _norm_text(row.get("Metodo"))
    liga = _norm_text(row.get("Liga"))
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")

    flt = filtros.get(metodo)
    if flt is None:
        return False
    if pd.isna(odd):
        return False
    oddv = float(odd)
    if oddv < flt.odd_min or oddv > flt.odd_max:
        return False
    if flt.ligas_permitidas and liga not in flt.ligas_permitidas:
        return False
    return True


def _blocked_by_rodo(row: pd.Series, rodos: list[dict]) -> tuple[bool, str]:
    liga = _norm_text(row.get("Liga"))
    metodo = _norm_text(row.get("Metodo"))
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    oddv = float(odd) if not pd.isna(odd) else float("nan")

    for rodo in rodos:
        r_league = _norm_text(rodo.get("league"))
        if r_league and r_league != liga:
            continue
        r_method = _norm_text(rodo.get("method_equals"))
        if r_method and r_method != metodo:
            continue
        rmin = rodo.get("odd_min")
        rmax = rodo.get("odd_max")
        if rmin is not None and (pd.isna(oddv) or oddv < float(rmin)):
            continue
        if rmax is not None and (pd.isna(oddv) or oddv > float(rmax)):
            continue
        return True, str(rodo.get("name") or f"Rodo_{rodo.get('id', '?')}")
    return False, ""


def _read_live_xlsx(apostas_dir: Path, start_date: pd.Timestamp | None, end_date: pd.Timestamp | None) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for file in sorted(apostas_dir.glob("Apostas_*.xlsx")):
        stem = file.stem.replace("Apostas_", "")
        if len(stem) != 8 or not stem.isdigit():
            continue
        dt = pd.to_datetime(stem, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            continue
        if start_date is not None and dt < start_date:
            continue
        if end_date is not None and dt > end_date:
            continue
        frame = pd.read_excel(file)
        if frame.empty:
            continue
        frame["Data_Arquivo"] = dt.date()
        frame["_arquivo_xlsx"] = file.name
        rows.append(frame)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _build_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Liga"] = out["Liga"].astype(str)
    out["Metodo"] = out["Metodo"].astype(str)
    out["Jogo"] = out["Jogo"].astype(str)
    out["_liga_n"] = out["Liga"].map(_norm_text)
    out["_metodo_n"] = out["Metodo"].map(_norm_text)
    homes_aways = out["Jogo"].map(_split_jogo)
    out["_home_n"] = [x[0] for x in homes_aways]
    out["_away_n"] = [x[1] for x in homes_aways]
    out["_date_n"] = pd.to_datetime(out["Data_Arquivo"], errors="coerce").dt.date
    out["_key_full"] = out.apply(
        lambda r: f"{r['_date_n']}|{r['_liga_n']}|{r['_metodo_n']}|{r['_home_n']}|{r['_away_n']}",
        axis=1,
    )
    out["_key_game"] = out.apply(
        lambda r: f"{r['_date_n']}|{r['_home_n']}|{r['_away_n']}",
        axis=1,
    )
    return out


def _classify_live_divergence(
    live_row: pd.Series,
    game_to_base: dict[str, pd.Series],
    filtros_metodo: dict[str, FiltroMetodo],
    rodos: list[dict],
) -> str:
    key_game = live_row["_key_game"]
    base_candidate = game_to_base.get(key_game)
    if base_candidate is None:
        return "nao_veio_na_base"

    metodo = _norm_text(base_candidate.get("Metodo"))
    liga = _norm_text(base_candidate.get("Liga"))
    odd = pd.to_numeric(base_candidate.get("Odd_Base"), errors="coerce")

    flt = filtros_metodo.get(metodo)
    if flt is None:
        return "metodo_fora_config"
    if liga not in flt.ligas_permitidas:
        return "fora_liga"
    if pd.isna(odd):
        return "odd_invalida"
    oddv = float(odd)
    if oddv < flt.odd_min or oddv > flt.odd_max:
        return "fora_faixa_odd"

    blocked, rodo_name = _blocked_by_rodo(base_candidate, rodos)
    if blocked:
        return f"bloqueado_rodo:{rodo_name}"
    return "divergencia_de_chave"


def _semaforo(match_pct: float, coverage_pct: float) -> str:
    if match_pct >= 90.0 and coverage_pct >= 90.0:
        return "verde"
    if match_pct >= 70.0 and coverage_pct >= 70.0:
        return "amarelo"
    return "vermelho"


def main() -> None:
    parser = argparse.ArgumentParser(description="Auditoria diária de aderência jogos do dia x backtest.")
    parser.add_argument("--base", default=str(DEFAULT_BASE), help="CSV base do backtest")
    parser.add_argument("--config", default=str(DEFAULT_CFG), help="Config com filtros")
    parser.add_argument("--apostas-dir", default=str(DEFAULT_APOSTAS_DIR), help="Pasta com Apostas_*.xlsx")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Pasta de saída")
    parser.add_argument("--start", default=None, help="Data inicial YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Data final YYYY-MM-DD")
    args = parser.parse_args()

    base_path = Path(args.base)
    cfg_path = Path(args.config)
    apostas_dir = Path(args.apostas_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start_date = pd.to_datetime(args.start, errors="coerce") if args.start else None
    end_date = pd.to_datetime(args.end, errors="coerce") if args.end else None

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    filtros_metodo = _parse_filtros_metodo(cfg)
    rodos = _extract_rodos(cfg)

    base = pd.read_csv(base_path)
    base = base.rename(columns={"Data": "Data_Arquivo"})
    base["Data_Arquivo"] = pd.to_datetime(base["Data_Arquivo"], errors="coerce")
    base = base.dropna(subset=["Data_Arquivo"]).copy()
    if start_date is not None:
        base = base[base["Data_Arquivo"] >= start_date]
    if end_date is not None:
        base = base[base["Data_Arquivo"] <= end_date]
    base["Data_Arquivo"] = base["Data_Arquivo"].dt.date

    live = _read_live_xlsx(apostas_dir, start_date, end_date)
    if live.empty:
        raise SystemExit("Nenhuma planilha Apostas_*.xlsx encontrada no período informado.")

    base_k = _build_keys(base)
    live_k = _build_keys(live)

    base_k["_passa_metodo"] = base_k.apply(_passes_method_filter, axis=1, filtros=filtros_metodo)
    cand = base_k[base_k["_passa_metodo"]].copy()
    rodo_ret = cand.apply(_blocked_by_rodo, axis=1, rodos=rodos)
    cand["_bloq"] = [x[0] for x in rodo_ret]
    cand["_rodo"] = [x[1] for x in rodo_ret]
    ref_exec = cand[~cand["_bloq"]].copy()

    common_dates = sorted(set(live_k["_date_n"].dropna()) & set(ref_exec["_date_n"].dropna()))
    live_c = live_k[live_k["_date_n"].isin(common_dates)].copy()
    ref_c = ref_exec[ref_exec["_date_n"].isin(common_dates)].copy()
    base_c = base_k[base_k["_date_n"].isin(common_dates)].copy()

    live_keys = set(live_c["_key_full"])
    ref_keys = set(ref_c["_key_full"])
    inter_keys = live_keys & ref_keys

    # Tabela diária com match e semáforo
    daily_rows: list[dict] = []
    for d in common_dates:
        lset = set(live_c.loc[live_c["_date_n"] == d, "_key_full"])
        rset = set(ref_c.loc[ref_c["_date_n"] == d, "_key_full"])
        inter = len(lset & rset)
        live_n = len(lset)
        ref_n = len(rset)
        match_pct = (inter / live_n * 100.0) if live_n else 0.0
        coverage_pct = (inter / ref_n * 100.0) if ref_n else 0.0
        daily_rows.append(
            {
                "Data": d,
                "live_exec": live_n,
                "backtest_exec_ref": ref_n,
                "matches": inter,
                "match_pct": round(match_pct, 2),
                "coverage_pct": round(coverage_pct, 2),
                "semaforo": _semaforo(match_pct, coverage_pct),
            }
        )
    daily_df = pd.DataFrame(daily_rows).sort_values("Data") if daily_rows else pd.DataFrame()

    # Divergências live -> referência
    game_to_base = {
        key: row
        for key, row in zip(base_c["_key_game"], base_c.to_dict(orient="records"), strict=False)
    }
    div_rows: list[dict] = []

    for _, row in live_c.iterrows():
        if row["_key_full"] in ref_keys:
            continue
        motivo = _classify_live_divergence(row, game_to_base, filtros_metodo, rodos)
        div_rows.append(
            {
                "lado": "live_sem_ref",
                "Data": row["_date_n"],
                "Liga": row.get("Liga", ""),
                "Jogo": row.get("Jogo", ""),
                "Metodo": row.get("Metodo", ""),
                "motivo": motivo,
                "arquivo": row.get("_arquivo_xlsx", ""),
            }
        )

    # Divergências referência -> live
    live_key_set = set(live_c["_key_full"])
    for _, row in ref_c.iterrows():
        if row["_key_full"] in live_key_set:
            continue
        div_rows.append(
            {
                "lado": "ref_sem_live",
                "Data": row["_date_n"],
                "Liga": row.get("Liga", ""),
                "Jogo": row.get("Jogo", ""),
                "Metodo": row.get("Metodo", ""),
                "motivo": "faltou_no_dia",
                "arquivo": "",
            }
        )

    div_df = pd.DataFrame(div_rows)
    if not div_df.empty:
        div_df = div_df.sort_values(["Data", "lado", "motivo", "Liga", "Jogo"]).reset_index(drop=True)

    total_live = len(live_keys)
    total_ref = len(ref_keys)
    total_matches = len(inter_keys)
    match_pct_total = (total_matches / total_live * 100.0) if total_live else 0.0
    coverage_pct_total = (total_matches / total_ref * 100.0) if total_ref else 0.0
    semaforo_geral = _semaforo(match_pct_total, coverage_pct_total)

    summary = {
        "periodo": {
            "start": str(min(common_dates)) if common_dates else None,
            "end": str(max(common_dates)) if common_dates else None,
        },
        "datas_em_comum": len(common_dates),
        "live_exec_total": total_live,
        "backtest_exec_ref_total": total_ref,
        "matches_total": total_matches,
        "match_pct_total": round(match_pct_total, 2),
        "coverage_pct_total": round(coverage_pct_total, 2),
        "semaforo_geral": semaforo_geral,
        "rodos_ativos": len(rodos),
    }

    daily_path = out_dir / "daily_match_table.csv"
    div_path = out_dir / "divergencias_com_motivo.csv"
    summary_path = out_dir / "semaforo_resumo.json"

    daily_df.to_csv(daily_path, index=False)
    div_df.to_csv(div_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 68)
    print("AUDITOR DIARIO DE ADERENCIA")
    print("=" * 68)
    print(f"Datas em comum               : {len(common_dates)}")
    print(f"Live executado (total)       : {total_live}")
    print(f"Backtest ref executado (tot) : {total_ref}")
    print(f"Matches totais               : {total_matches}")
    print(f"Match % total                : {match_pct_total:.2f}%")
    print(f"Coverage % total             : {coverage_pct_total:.2f}%")
    print(f"Semaforo geral               : {semaforo_geral.upper()}")
    print("-" * 68)
    if not div_df.empty:
        print("Top motivos de divergencia:")
        print(div_df["motivo"].value_counts().head(10).to_string())
        print("-" * 68)
    print(f"Arquivo daily                : {daily_path}")
    print(f"Arquivo divergencias         : {div_path}")
    print(f"Arquivo semaforo             : {summary_path}")
    print("=" * 68)


if __name__ == "__main__":
    main()
