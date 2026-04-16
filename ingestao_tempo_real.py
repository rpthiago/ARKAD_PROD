from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd
import requests


DEFAULT_TIMEOUT_SEC = 15.0


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        total = payload.get("total")
        dados = payload.get("dados")
        if isinstance(total, int) and total >= 0 and isinstance(dados, list):
            return [x for x in dados if isinstance(x, dict)]
        for key in ("data", "items", "results", "rows", "matches"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return [payload]
    return []


def _ensure_required_columns(df: pd.DataFrame, cfg: dict[str, Any], target_date_iso: str) -> pd.DataFrame:
    dt_cfg = cfg.get("input", {}).get("datetime", {})
    col_cfg = cfg.get("input", {}).get("columns", {})

    date_col = dt_cfg.get("date_col", "Data_Arquivo")
    time_col = dt_cfg.get("time_col", "Horario_Entrada")
    league_col = col_cfg.get("league_col", "Liga")
    method_col = col_cfg.get("method_col", "Metodo")
    odd_col = col_cfg.get("odd_signal_col", "Odd_Base")

    out = df.copy()
    if date_col not in out.columns:
        out[date_col] = target_date_iso
    if time_col not in out.columns:
        out[time_col] = "00:00"
    if league_col not in out.columns:
        out[league_col] = "UNKNOWN"
    if method_col not in out.columns:
        out[method_col] = "Lay_CS_0x1_B365"
    if odd_col not in out.columns:
        out[odd_col] = pd.NA
    if "Jogo" not in out.columns:
        out["Jogo"] = "Jogo sem nome"

    out[odd_col] = pd.to_numeric(out[odd_col], errors="coerce")
    out = out[out[odd_col].notna()].copy()
    return out


def _normalize_provider_frame(
    provider_name: str,
    raw: pd.DataFrame,
    cfg: dict[str, Any],
    target_date_iso: str,
    default_method: str,
) -> pd.DataFrame:
    if raw.empty:
        return raw

    dt_cfg = cfg.get("input", {}).get("datetime", {})
    col_cfg = cfg.get("input", {}).get("columns", {})
    date_col = dt_cfg.get("date_col", "Data_Arquivo")
    time_col = dt_cfg.get("time_col", "Horario_Entrada")
    league_col = col_cfg.get("league_col", "Liga")
    method_col = col_cfg.get("method_col", "Metodo")
    odd_col = col_cfg.get("odd_signal_col", "Odd_Base")

    alias_map = {
        "date": date_col,
        "data": date_col,
        "data_arquivo": date_col,
        "time": time_col,
        "hora": time_col,
        "horario": time_col,
        "horario_entrada": time_col,
        "league": league_col,
        "liga": league_col,
        "method": method_col,
        "metodo": method_col,
        "odd": odd_col,
        "odd_base": odd_col,
        "jogo": "Jogo",
        "match": "Jogo",
        "evento": "Jogo",
    }

    rename_map: dict[str, str] = {}
    for col in raw.columns:
        key = str(col).strip().lower()
        if key in alias_map:
            rename_map[col] = alias_map[key]

    out = raw.rename(columns=rename_map)

    # FutPythonTrader envia Home/Away separados; compoe o campo Jogo quando ausente.
    if "Jogo" not in out.columns:
        home_col = next((c for c in out.columns if str(c).strip().lower() == "home"), None)
        away_col = next((c for c in out.columns if str(c).strip().lower() == "away"), None)
        if home_col and away_col:
            out["Jogo"] = out[home_col].astype(str).str.strip() + " x " + out[away_col].astype(str).str.strip()

    # Scanner master FutPython: expande em duas linhas por jogo (Lay 0x1 e Lay 1x0)
    # usando as colunas corretas de correct score antes do filtro de Rodo.
    cs_01_col = next((c for c in out.columns if str(c).strip().lower() == "odd_cs_0x1"), None)
    cs_10_col = next((c for c in out.columns if str(c).strip().lower() == "odd_cs_1x0"), None)

    provider_suffix = "BF" if "fair" in str(provider_name).lower() else "B365"
    if cs_01_col or cs_10_col:
        expanded_frames: list[pd.DataFrame] = []
        if cs_01_col:
            df_01 = out.copy()
            df_01[method_col] = f"Lay_CS_0x1_{provider_suffix}"
            df_01[odd_col] = pd.to_numeric(df_01[cs_01_col], errors="coerce")
            expanded_frames.append(df_01)
        if cs_10_col:
            df_10 = out.copy()
            df_10[method_col] = f"Lay_CS_1x0_{provider_suffix}"
            df_10[odd_col] = pd.to_numeric(df_10[cs_10_col], errors="coerce")
            expanded_frames.append(df_10)
        if expanded_frames:
            out = pd.concat(expanded_frames, ignore_index=True, sort=False)
    elif odd_col not in out.columns or pd.to_numeric(out.get(odd_col), errors="coerce").isna().all():
        # Fallback generico para provedores sem colunas de scanner CS.
        odd_candidates = [
            c
            for c in out.columns
            if str(c).strip().lower().startswith("odd_") and pd.to_numeric(out[c], errors="coerce").notna().any()
        ]
        if odd_candidates:
            out[odd_col] = pd.to_numeric(out[odd_candidates[0]], errors="coerce")

    out = _ensure_required_columns(out, cfg, target_date_iso)

    if method_col in out.columns:
        out[method_col] = out[method_col].fillna(default_method)
    else:
        out[method_col] = default_method

    if "Fonte" not in out.columns:
        out["Fonte"] = provider_name
    return out


def _load_from_custom_provider(
    provider_name: str,
    provider_cfg: dict[str, Any],
    cfg: dict[str, Any],
    target_date_iso: str,
    timeout_sec: float,
) -> tuple[pd.DataFrame, str | None]:
    endpoint_url = str(provider_cfg.get("endpoint_url", "")).strip()
    if not endpoint_url:
        return pd.DataFrame(), None

    url_template = str(provider_cfg.get("url_template", "")).strip()
    if url_template:
        endpoint_url = url_template.format(date=target_date_iso, data=target_date_iso)
    elif bool(provider_cfg.get("append_date_path", False)):
        endpoint_url = endpoint_url.rstrip("/") + f"/{target_date_iso}/"

    headers = dict(provider_cfg.get("headers", {}) or {})
    params = dict(provider_cfg.get("query_params", {}) or {})
    date_param = str(provider_cfg.get("date_param", "date")).strip() or "date"
    if not bool(provider_cfg.get("append_date_path", False)) and not url_template:
        params[date_param] = target_date_iso

    token_env = str(provider_cfg.get("token_env", "")).strip()
    auth_header = str(provider_cfg.get("auth_header", "Authorization")).strip() or "Authorization"
    auth_scheme = str(provider_cfg.get("auth_scheme", "Bearer")).strip()
    if token_env and auth_header not in headers:
        token = os.getenv(token_env, "").strip()
        if token:
            headers[auth_header] = f"{auth_scheme} {token}".strip() if auth_scheme else token

    try:
        response = requests.get(endpoint_url, params=params, headers=headers, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.json()
        records = _extract_records(payload)
        raw = pd.DataFrame(records)
        default_method = str(provider_cfg.get("default_method", "Lay_CS_0x1_B365")).strip() or "Lay_CS_0x1_B365"
        out = _normalize_provider_frame(provider_name, raw, cfg, target_date_iso, default_method)
        return out, None
    except Exception as exc:
        return pd.DataFrame(), f"{provider_name}: {exc}"


def _to_hhmm(iso_datetime: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_datetime.replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return "00:00"


def _load_from_odds_api(
    ingest_cfg: dict[str, Any],
    cfg: dict[str, Any],
    target_date_iso: str,
    timeout_sec: float,
) -> tuple[pd.DataFrame, str | None]:
    odds_cfg = dict(ingest_cfg.get("odds_api", {}) or {})
    if not bool(odds_cfg.get("enabled", False)):
        return pd.DataFrame(), None

    api_key_env = str(odds_cfg.get("api_key_env", "ODDS_API_KEY")).strip() or "ODDS_API_KEY"
    api_key = os.getenv(api_key_env, "").strip()
    if not api_key:
        return pd.DataFrame(), f"odds_api: variavel {api_key_env} nao definida"

    base_url = str(odds_cfg.get("base_url", "https://api.the-odds-api.com/v4/sports/upcoming/odds")).strip()
    sport_key = str(odds_cfg.get("sport_key", "soccer")).strip() or "soccer"

    try:
        providers = dict(ingest_cfg.get("providers", {}) or {})
        bookmaker_keys = []
        for prov_name in ("bet365", "betfair"):
            key = str(providers.get(prov_name, {}).get("bookmaker_key", prov_name)).strip()
            if key:
                bookmaker_keys.append(key)
        bookmakers = ",".join(bookmaker_keys)

        params = {
            "apiKey": api_key,
            "bookmakers": bookmakers,
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        if sport_key != "soccer":
            params["sport"] = sport_key

        response = requests.get(base_url, params=params, timeout=timeout_sec)
        response.raise_for_status()
        events = response.json()

        rows: list[dict[str, Any]] = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            commence = str(ev.get("commence_time", ""))
            if not commence.startswith(target_date_iso):
                continue

            home = str(ev.get("home_team", "")).strip()
            away = str(ev.get("away_team", "")).strip()
            jogo = f"{home} x {away}".strip(" x") or "Jogo sem nome"
            liga = str(ev.get("sport_title", "UNKNOWN")).strip() or "UNKNOWN"
            hora = _to_hhmm(commence)

            bookmakers_data = ev.get("bookmakers", [])
            if not isinstance(bookmakers_data, list):
                continue

            for bm in bookmakers_data:
                if not isinstance(bm, dict):
                    continue
                bm_key = str(bm.get("key", "")).lower()
                method = "Lay_CS_0x1_B365" if "365" in bm_key else "Lay_CS_0x1_BF"

                markets = bm.get("markets", [])
                if not isinstance(markets, list):
                    continue
                odd_value = None
                for market in markets:
                    outcomes = market.get("outcomes", []) if isinstance(market, dict) else []
                    if not isinstance(outcomes, list):
                        continue
                    prices = [x.get("price") for x in outcomes if isinstance(x, dict)]
                    prices = [p for p in prices if isinstance(p, (int, float))]
                    if prices:
                        odd_value = max(prices)
                        break

                if odd_value is None:
                    continue

                rows.append(
                    {
                        "Data_Arquivo": target_date_iso,
                        "Horario_Entrada": hora,
                        "Liga": liga,
                        "Jogo": jogo,
                        "Metodo": method,
                        "Odd_Base": float(odd_value),
                        "Fonte": bm_key,
                    }
                )

        out = pd.DataFrame(rows)
        out = _ensure_required_columns(out, cfg, target_date_iso)
        return out, None
    except Exception as exc:
        return pd.DataFrame(), f"odds_api: {exc}"


def load_live_dataframe(target_date_iso: str, cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    runtime = cfg.get("runtime_data", {})
    ingest_cfg = dict(runtime.get("live_ingestion", {}) or {})
    if not bool(ingest_cfg.get("enabled", False)):
        return pd.DataFrame(), "Ingestao em tempo real desabilitada"

    timeout_sec = max(float(ingest_cfg.get("timeout_sec", DEFAULT_TIMEOUT_SEC)), 5.0)
    providers = dict(ingest_cfg.get("providers", {}) or {})
    active_sources_raw = ingest_cfg.get("active_sources", ["bet365", "betfair"])
    if isinstance(active_sources_raw, list):
        active_sources = [str(x).strip().lower() for x in active_sources_raw if str(x).strip()]
    else:
        active_sources = ["bet365", "betfair"]

    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    used_sources: list[str] = []

    for provider_name in active_sources:
        provider_cfg = dict(providers.get(provider_name, {}) or {})
        df_prov, err = _load_from_custom_provider(provider_name, provider_cfg, cfg, target_date_iso, timeout_sec)
        if err:
            errors.append(err)
        if not df_prov.empty:
            frames.append(df_prov)
            used_sources.append(provider_name)

    # Opcional: fallback de ingestao via Odds API quando endpoints custom nao responderem.
    if not frames:
        df_odds, err = _load_from_odds_api(ingest_cfg, cfg, target_date_iso, timeout_sec)
        if err:
            errors.append(err)
        if not df_odds.empty:
            frames.append(df_odds)
            used_sources.append("odds_api")

    if not frames:
        err_txt = " | ".join(errors) if errors else "sem dados nas fontes em tempo real"
        return pd.DataFrame(), f"Ingestao em tempo real sem dados: {err_txt}"

    df = pd.concat(frames, ignore_index=True, sort=False)
    df = df.drop_duplicates(subset=["Jogo", "Horario_Entrada", "Metodo"], keep="first")
    src_txt = ", ".join(used_sources)
    return df, f"Ingestao em tempo real ativa ({src_txt})"
