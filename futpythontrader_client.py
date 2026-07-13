import io
import os
import time
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_TOKEN

_ALLOWED_SOURCES = {"bet365", "betfair", "footystats", "footstats"}
_SOURCE_ALIASES = {"footstats": "footystats"}


def _env_int(name: str, default: int) -> int:
    try:
        value = int(str(os.getenv(name, "")).strip())
        return value if value > 0 else int(default)
    except Exception:
        return int(default)


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    params: Optional[dict] = None,
    timeout: int = 60,
) -> requests.Response:
    """Executa request com retries e backoff para reduzir falhas transitórias."""
    retries = _env_int("FUTPYTHON_RETRIES", 4)
    backoff_base = float(os.getenv("FUTPYTHON_RETRY_BACKOFF", "1.5") or 1.5)

    # Usa timeout em tupla (connect, read) para evitar queda por read timeout curto.
    connect_timeout = float(_env_int("FUTPYTHON_CONNECT_TIMEOUT", 20))
    read_timeout = float(_env_int("FUTPYTHON_READ_TIMEOUT", timeout))
    timeout_tuple = (connect_timeout, read_timeout)

    session = _get_http_session()
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=timeout_tuple,
                proxies={"http": None, "https": None},
            )
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = backoff_base * attempt
            time.sleep(min(sleep_s, 12.0))

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Falha de request sem excecao explicita.")


def _get_api_token() -> str:
    names = ("FUTPYTHON_TOKEN", "FUTPYTHON_API_TOKEN", "API_TOKEN")

    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value

    # Streamlit Cloud costuma usar secrets em vez de variaveis de ambiente.
    try:
        import streamlit as st  # type: ignore

        for name in names:
            value = str(st.secrets.get(name, "")).strip()
            if value:
                return value
    except Exception:
        pass

    return str(API_TOKEN).strip()


def _extract_records(payload) -> list[dict]:
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


def _day_from_historical(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if df.empty or "Date" not in df.columns:
        return pd.DataFrame()
    date_series = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df[date_series == date_str].copy()


@lru_cache(maxsize=1)
def _get_http_session() -> requests.Session:
    retry = Retry(
        total=_env_int("FUTPYTHON_HTTP_RETRIES", 4),
        connect=_env_int("FUTPYTHON_HTTP_RETRIES", 4),
        read=_env_int("FUTPYTHON_HTTP_RETRIES", 4),
        backoff_factor=float(os.getenv("FUTPYTHON_HTTP_BACKOFF", "1.2") or 1.2),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.trust_env = False
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _build_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Token {token}", "User-Agent": "Mozilla/5.0"}


def _ensure_token() -> str:
    token = _get_api_token()
    if not token:
        raise RuntimeError(
            "FUTPYTHON_TOKEN nao configurado. Defina a variavel de ambiente e reinicie o terminal/app."
        )
    return token


def _raise_for_status_with_context(response: requests.Response) -> None:
    if response.status_code == 401:
        raise RuntimeError(
            "Erro 401 da API FutPythonTrader: token expirado/invalido. "
            "Atualize FUTPYTHON_TOKEN e reinicie o terminal/app."
        )
    response.raise_for_status()


def _normalize_source(source: str) -> str:
    src = str(source).strip().lower()
    src = _SOURCE_ALIASES.get(src, src)
    if src not in _ALLOWED_SOURCES:
        valid = ", ".join(sorted(_ALLOWED_SOURCES))
        raise ValueError(f"Fonte invalida: {source}. Use uma de: {valid}")
    return src


def _download_url(source: str) -> str:
    normalized = _normalize_source(source)
    return f"https://apicomunidade.futpythontrader.com/api/dados/{normalized}/download/"


def _daily_url(source: str, date_str: str) -> str:
    normalized = _normalize_source(source)
    return f"https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/{normalized}/{date_str}/"


def get_dataframe(source: str, params: Optional[dict] = None, timeout: int = 60) -> pd.DataFrame:
    """Baixa o CSV historico da fonte e devolve DataFrame."""
    # Fallback local para bet365 para evitar download lento de 54MB
    if str(source).strip().lower() == "bet365":
        local_file = "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
        if os.path.exists(local_file):
            try:
                return pd.read_csv(local_file, low_memory=False)
            except Exception:
                pass

    token = _ensure_token()
    url = _download_url(source)
    response = _request_with_retry(
        "GET",
        url,
        headers=_build_headers(token),
        params=params,
        timeout=timeout,
    )
    _raise_for_status_with_context(response)
    return pd.read_csv(io.BytesIO(response.content), low_memory=False)


def get_daily_dataframe(source: str, date_str: str, timeout: int = 20) -> pd.DataFrame:
    """Baixa os jogos do dia (JSON) da fonte e devolve DataFrame."""
    token = _ensure_token()
    url = _daily_url(source, date_str)
    try:
        response = _request_with_retry(
            "GET",
            url,
            headers=_build_headers(token),
            timeout=timeout,
        )
        _raise_for_status_with_context(response)
        payload = response.json()
        records = _extract_records(payload)
        df = pd.DataFrame(records)
    except Exception:
        # Fallback resiliente: quando jogos-do-dia estiver instavel, tenta historico filtrado por data.
        df_hist = get_dataframe_safe(source=source, timeout=max(timeout, 60))
        df = _day_from_historical(df_hist, date_str)

    if df.empty:
        return pd.DataFrame()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for col in [c for c in df.columns if "Odd_" in c]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_dataframe_safe(source: str, params: Optional[dict] = None, timeout: int = 60) -> pd.DataFrame:
    """Versao resiliente: retorna DataFrame vazio em caso de erro."""
    try:
        return get_dataframe(source=source, params=params, timeout=timeout)
    except Exception:
        return pd.DataFrame()
