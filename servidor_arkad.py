from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from ingestao_tempo_real import load_live_dataframe

ROOT_DIR = Path(__file__).resolve().parent
CSV_PATH = ROOT_DIR / "recalculo_sem_combos_usuario.csv"
PROD_CFG_PATH = ROOT_DIR / "config_prod_v1.json"
RODO_MASTER_PATH = ROOT_DIR / "config_rodos_master.json"

app = FastAPI(title="Servidor Arkad", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _parse_hhmm_to_minutes(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or ":" not in s:
        return None
    parts = s.split(":")
    if len(parts) < 2:
        return None
    try:
        hh = int(float(parts[0]))
        mm = int(float(parts[1]))
    except Exception:
        return None
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh * 60 + mm


def _matches_cut(row: pd.Series, cut: dict[str, Any], league_col: str, method_col: str, odd_col: str) -> bool:
    league = str(row.get(league_col, ""))
    method = str(row.get(method_col, ""))
    odd = pd.to_numeric(row.get(odd_col), errors="coerce")

    def _expand_method_aliases(value: str) -> set[str]:
        s = str(value or "")
        variants = {s}
        if "Lay_CS_0x1_" in s:
            variants.add(s.replace("Lay_CS_0x1_", "Lay_CS_1x0_"))
        if "Lay_CS_1x0_" in s:
            variants.add(s.replace("Lay_CS_1x0_", "Lay_CS_0x1_"))
        return variants

    cut_leagues = set(cut.get("leagues", []))
    if cut.get("league"):
        cut_leagues.add(str(cut["league"]))
    if cut_leagues and league not in cut_leagues:
        return False

    method_equals = cut.get("method_equals")
    method_contains = cut.get("method_contains")
    if method_equals and method not in _expand_method_aliases(str(method_equals)):
        return False
    if method_contains and str(method_contains) not in method:
        return False

    if pd.isna(odd):
        return False
    odd_min = cut.get("odd_min")
    odd_max = cut.get("odd_max")
    if odd_min is not None and float(odd) < float(odd_min):
        return False
    if odd_max is not None and float(odd) > float(odd_max):
        return False

    return True


def _extract_rodo_cuts(data: dict[str, Any]) -> list[dict[str, Any]]:
    cuts = data.get("filtros_rodo")
    if isinstance(cuts, list):
        return [c for c in cuts if isinstance(c, dict)]

    filters = data.get("filters", {})
    if isinstance(filters, dict):
        nested = filters.get("filtros_rodo") or filters.get("toxic_cuts")
        if isinstance(nested, list):
            return [c for c in nested if isinstance(c, dict)]
    return []


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"Arquivo nao encontrado: {path.name}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"JSON invalido em {path.name}: {exc}") from exc


def _load_approved_signals(target_date_iso: str) -> tuple[list[dict[str, Any]], str]:
    cfg = _load_json(PROD_CFG_PATH)
    rodo_master = _load_json(RODO_MASTER_PATH)

    cuts = _extract_rodo_cuts(rodo_master)
    if not cuts:
        raise HTTPException(status_code=500, detail="config_rodos_master.json sem filtros_rodo validos")

    live_df, live_source = load_live_dataframe(target_date_iso, cfg)
    if not live_df.empty:
        df = live_df.copy()
        source_used = live_source
    else:
        if not CSV_PATH.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Ingestao falhou ({live_source}) e arquivo local ausente: {CSV_PATH.name}",
            )
        try:
            df = pd.read_csv(CSV_PATH)
            source_used = f"Fallback CSV local ({CSV_PATH.name}) | motivo: {live_source}"
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Falha ao ler CSV de sinais: {exc}") from exc

    dt_cfg = cfg.get("input", {}).get("datetime", {})
    col_cfg = cfg.get("input", {}).get("columns", {})
    date_col = dt_cfg.get("date_col", "Data_Arquivo")
    time_col = dt_cfg.get("time_col", "Horario_Entrada")
    league_col = col_cfg.get("league_col", "Liga")
    method_col = col_cfg.get("method_col", "Metodo")
    odd_col = col_cfg.get("odd_signal_col", "Odd_Base")

    required_cols = [date_col, time_col, league_col, method_col, odd_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"Colunas ausentes no CSV: {', '.join(missing)}")

    target_date = pd.Timestamp(pd.to_datetime(target_date_iso, errors="coerce").date())
    df = df.copy()
    df["__date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    df = df[df["__date"] == target_date].copy()
    if df.empty:
        return [], source_used

    blocked = df.apply(lambda r: any(_matches_cut(r, cut, league_col, method_col, odd_col) for cut in cuts), axis=1)

    df["__mins"] = df[time_col].apply(_parse_hhmm_to_minutes)
    df = df[df["__mins"].notna()].copy()
    if df.empty:
        return [], source_used

    df["Status"] = blocked.reindex(df.index).map(lambda x: "SKIP" if bool(x) else "EXECUTED")
    approved = df[df["Status"] == "EXECUTED"].copy()
    if approved.empty:
        return [], source_used

    approved = approved.sort_values("__mins").reset_index(drop=True)

    response_cols = [
        date_col,
        time_col,
        league_col,
        "Jogo" if "Jogo" in approved.columns else None,
        method_col,
        odd_col,
        "PnL_Linha" if "PnL_Linha" in approved.columns else None,
        "Status",
    ]
    response_cols = [c for c in response_cols if c is not None]

    records = approved[response_cols].to_dict(orient="records")
    return records, source_used


@app.get("/arkad/sinais")
def get_sinais(date: str = Query(default_factory=lambda: date.today().isoformat())) -> dict[str, Any]:
    print("REQ RECEBIDA")
    records, source_used = _load_approved_signals(date)
    return {
        "date": date,
        "count": len(records),
        "source": source_used,
        "items": records,
    }


if __name__ == "__main__":
    import uvicorn

    print("🚀 Servidor Arkad Online na porta 8080. Dashboard pronto para receber sinais!")
    uvicorn.run("servidor_arkad:app", host="0.0.0.0", port=8080, reload=False)
