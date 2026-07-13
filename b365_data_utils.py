import io
import difflib
import re
import unicodedata
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_DIA_URL, API_HEADERS, API_TOKEN
from futpythontrader_client import get_dataframe_safe


API_B365_DAILY = "https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/bet365/"
API_B365_HIST = "https://apicomunidade.futpythontrader.com/api/dados/bet365/download/"
BETFAIR_MATCH_THRESHOLD = 0.82
CS_SCORELINES = [
    "0x0", "0x1", "0x2", "0x3",
    "1x0", "1x1", "1x2", "1x3",
    "2x0", "2x1", "2x2", "2x3",
    "3x0", "3x1", "3x2", "3x3",
]

CONNECT_TIMEOUT_SEC = 12
READ_TIMEOUT_SEC = 25


@lru_cache(maxsize=1)
def _get_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    # Evita herdar proxies do ambiente (comum em hosts que causam ConnectTimeout na API).
    session.trust_env = False
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _normalize_b365(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    normalized = df.copy()
    if "Date" in normalized.columns:
        normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce")
    for column_name in [name for name in normalized.columns if "Odd_" in name]:
        normalized[column_name] = pd.to_numeric(normalized[column_name], errors="coerce")
    return normalized


def extract_api_records(payload) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        total = payload.get("total")
        dados = payload.get("dados")
        if isinstance(total, int) and total >= 0 and isinstance(dados, list):
            return [item for item in dados if isinstance(item, dict)]
        for key in ("data", "items", "results", "rows", "matches"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []


def _extract_records(payload) -> list[dict]:
    # Compatibilidade interna com chamadas antigas.
    return extract_api_records(payload)


def _fallback_day_from_historical(source: str, date_str: str) -> pd.DataFrame:
    hist_df = get_dataframe_safe(source=source, timeout=90)
    if hist_df.empty or "Date" not in hist_df.columns:
        return pd.DataFrame()
    day_series = pd.to_datetime(hist_df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    day_df = hist_df[day_series == date_str].copy()
    return _normalize_b365(day_df)


def fetch_b365_daily(date_str: str) -> pd.DataFrame:
    try:
        response = _get_http_session().get(
            f"{API_B365_DAILY}{date_str}/",
            headers=API_HEADERS,
            timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
        )
        if response.status_code == 200:
            records = extract_api_records(response.json())
            if records:
                return _normalize_b365(pd.DataFrame(records))
    except Exception:
        pass
    return _fallback_day_from_historical("bet365", date_str)


def fetch_betfair_daily(date_str: str) -> pd.DataFrame:
    try:
        response = _get_http_session().get(
            API_DIA_URL.format(data=date_str),
            headers=API_HEADERS,
            timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
        )
        if response.status_code == 200:
            records = extract_api_records(response.json())
            if records:
                return _normalize_b365(pd.DataFrame(records))
    except Exception:
        pass
    return _fallback_day_from_historical("betfair", date_str)


@lru_cache(maxsize=1)
def load_b365_historical() -> pd.DataFrame:
    from pathlib import Path
    local_path = Path(__file__).resolve().parent / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
    if local_path.exists():
        try:
            return _normalize_b365(pd.read_csv(local_path, low_memory=False))
        except Exception:
            pass

    # Base ENXUTA committada (para a nuvem, que nao tem a base full de 114MB e nao
    # consegue baixa-la sem timeout/OOM). Colunas suficientes p/ os metodos ativos.
    lean_path = Path(__file__).resolve().parent / "b365_base_lean.csv"
    if lean_path.exists():
        try:
            return _normalize_b365(pd.read_csv(lean_path, low_memory=False))
        except Exception:
            pass

    headers = {"User-Agent": "Mozilla/5.0"}
    if API_TOKEN:
        headers["Authorization"] = f"Token {API_TOKEN}"

    last_error = None
    for timeout_sec in (60, 120):
        try:
            response = _get_http_session().get(
                API_B365_HIST,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SEC, timeout_sec),
            )
            response.raise_for_status()
            return _normalize_b365(pd.read_csv(io.StringIO(response.text)))
        except Exception as exc:
            last_error = exc

    if last_error is not None:
        raise last_error

    return pd.DataFrame()


def resolve_odd_column(df: pd.DataFrame, config: dict) -> Optional[str]:
    for column_name in config.get("odd_aliases") or [config["odd_col"]]:
        if column_name in df.columns:
            return column_name
    return None


def _canon_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", normalized.lower())


def enrich_b365_with_betfair_cs(b365_df: pd.DataFrame, betfair_df: pd.DataFrame) -> pd.DataFrame:
    if b365_df.empty or betfair_df.empty:
        return b365_df

    base = b365_df.copy().reset_index(drop=True)
    ref = betfair_df.copy().reset_index(drop=True)

    for df in (base, ref):
        df["_home_key"] = df.get("Home", pd.Series(index=df.index, dtype=str)).map(_canon_text)
        df["_away_key"] = df.get("Away", pd.Series(index=df.index, dtype=str)).map(_canon_text)
        df["_fixture_key"] = df["_home_key"] + "|" + df["_away_key"]
        df["_time_key"] = df.get("Time", pd.Series(index=df.index, dtype=str)).astype(str).str[:5]

    used_betfair: set[int] = set()
    match_map: dict[int, int] = {}
    for b365_idx, b365_row in base.iterrows():
        best_idx = None
        best_score = 0.0
        for betfair_idx, betfair_row in ref.iterrows():
            if betfair_idx in used_betfair:
                continue
            score = difflib.SequenceMatcher(None, b365_row["_fixture_key"], betfair_row["_fixture_key"]).ratio()
            if b365_row.get("League", "") == betfair_row.get("League", ""):
                score += 0.08
            if b365_row["_time_key"] == betfair_row["_time_key"]:
                score += 0.05
            if score > best_score:
                best_score = score
                best_idx = betfair_idx
        if best_idx is not None and best_score >= BETFAIR_MATCH_THRESHOLD:
            used_betfair.add(best_idx)
            match_map[b365_idx] = best_idx

    if not match_map:
        return base.drop(columns=["_home_key", "_away_key", "_fixture_key", "_time_key"])

    enriched = base.copy()
    for scoreline in CS_SCORELINES:
        back_col = f"Odd_CS_{scoreline}_Back"
        lay_col = f"Odd_CS_{scoreline}_Lay"
        target_back_col = f"Odd_CS_{scoreline}"
        if target_back_col not in enriched.columns:
            enriched[target_back_col] = pd.NA
        if lay_col not in enriched.columns:
            enriched[lay_col] = pd.NA

        for b365_idx, betfair_idx in match_map.items():
            if back_col in ref.columns:
                enriched.at[b365_idx, target_back_col] = ref.at[betfair_idx, back_col]
            if lay_col in ref.columns:
                enriched.at[b365_idx, lay_col] = ref.at[betfair_idx, lay_col]

    enriched["Match_Source_CS"] = "betfair"
    enriched.loc[~enriched.index.isin(match_map.keys()), "Match_Source_CS"] = "sem_match"
    return enriched.drop(columns=["_home_key", "_away_key", "_fixture_key", "_time_key"])


def get_b365_method_data(date_str: str, config: dict) -> tuple[pd.DataFrame, str]:
    daily_df = fetch_b365_daily(date_str)
    daily_odd_col = resolve_odd_column(daily_df, config)
    requires_lay_odd = bool(config.get("is_lay"))
    if daily_odd_col is not None:
        # Para métodos Lay, evita travar em odd não-Lay da API diária e tenta cruzar com Betfair.
        if not (requires_lay_odd and not str(daily_odd_col).endswith("_Lay")):
            return daily_df, "api_diaria"

    betfair_df = fetch_betfair_daily(date_str)
    enriched_daily_df = enrich_b365_with_betfair_cs(daily_df, betfair_df)
    if resolve_odd_column(enriched_daily_df, config) is not None:
        return enriched_daily_df, "api_diaria_cruzada_betfair"

    try:
        historical_df = load_b365_historical()
    except Exception:
        # Nao interrompe o scanner quando o historico estiver indisponivel;
        # mantem o melhor dataset diario ja obtido.
        return enriched_daily_df, "api_diaria_sem_coluna_historico_indisponivel"

    if historical_df.empty or "Date" not in historical_df.columns:
        return enriched_daily_df, "api_diaria_sem_coluna"

    day_df = historical_df[historical_df["Date"].dt.strftime("%Y-%m-%d") == date_str].copy()
    if not day_df.empty and resolve_odd_column(day_df, config) is not None:
        return day_df, "historico"

    return enriched_daily_df, "api_diaria_sem_coluna"