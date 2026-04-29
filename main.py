from __future__ import annotations

import json
import os
import time
import zoneinfo
from datetime import date, datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

_TZ_BR = zoneinfo.ZoneInfo("America/Sao_Paulo")


def _now_br() -> datetime:
    """Retorna o datetime atual no fuso de Brasilia (UTC-3 / UTC-2 no verao)."""
    return datetime.now(tz=_TZ_BR)

import pandas as pd
import requests
import streamlit as st

from ingestao_tempo_real import load_live_dataframe

ROOT_DIR = Path(__file__).resolve().parent
APOSTAS_DIR = ROOT_DIR / "Apostas_Diarias"
PROD_CFG_PATH = ROOT_DIR / "config_universo_97.json"
RODOS_MASTER_PATH = ROOT_DIR / "config_rodos_master.json"
FIXED_ENDPOINT_URL = "http://127.0.0.1:8080/arkad/sinais"
LOCAL_FALLBACK_PATH = ROOT_DIR / "recalculo_sem_combos_usuario.csv"
OPERATIONAL_GLOBS = ["Apostas_*.xlsx", "Apostas_*.xls"]


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


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return [payload]
    return []


def _resolve_endpoint_url(cfg: dict[str, Any] | None = None) -> str:
    def _normalize_api_url(url: str) -> str:
        u = str(url or "").strip()
        if not u:
            return ""
        low = u.lower()
        if low.endswith("/sinais") or low.endswith("/arkad/sinais"):
            return u
        if low.startswith("http://") or low.startswith("https://"):
            return u.rstrip("/") + "/sinais"
        return u

    # Prioridade: variavel de ambiente > config raiz > runtime_data.endpoint_url > fallback local.
    env_url = str(os.getenv("ARKAD_API_URL", "")).strip()
    if env_url:
        return _normalize_api_url(env_url)

    cfg = cfg or {}
    root_url = str(cfg.get("arkad_api_url", "")).strip()
    if root_url:
        return _normalize_api_url(root_url)

    runtime = cfg.get("runtime_data", {}) if isinstance(cfg, dict) else {}
    runtime_url = str(runtime.get("endpoint_url", "")).strip()
    if runtime_url:
        return _normalize_api_url(runtime_url)

    # Fallback local padrao.
    return _normalize_api_url(FIXED_ENDPOINT_URL)


def _probe_api_url(api_url: str) -> tuple[bool, str]:
    if not api_url:
        return False, "URL vazia"
    headers: dict[str, str] = {}
    proxies = {"http": None, "https": None}
    test_date = _now_br().date().isoformat()
    try:
        resp = requests.get(api_url, params={"date": test_date}, headers=headers, timeout=10.0, proxies=proxies)
        if 200 <= resp.status_code < 300:
            return True, f"HTTP {resp.status_code} OK"
        body = (resp.text or "").strip().replace("\n", " ")
        return False, f"HTTP {resp.status_code} | body: {body[:220]}"
    except Exception as exc:
        return False, str(exc)


def _is_local_endpoint(url: str) -> bool:
    s = str(url or "").strip().lower()
    return "127.0.0.1" in s or "localhost" in s or "0.0.0.0" in s


def _is_streamlit_cloud_runtime() -> bool:
    if os.getenv("STREAMLIT_SHARING_MODE", ""):
        return True
    if os.getenv("ARKAD_CLOUD_MODE", ""):
        return True
    return os.getenv("HOME", "") == "/home/appuser"


def _is_endpoint_connection_error(source_label: str) -> bool:
    s = (source_label or "").lower()
    if not s.startswith("endpoint indisponivel"):
        return False
    connection_markers = [
        "connection",
        "refused",
        "failed to establish",
        "max retries exceeded",
        "httpconnectionpool",
    ]
    return any(marker in s for marker in connection_markers)


def _is_local_fallback(source_label: str) -> bool:
    return str(source_label or "").lower().startswith("fallback local")


def _is_cloud_fallback(source_label: str) -> bool:
    return str(source_label or "").lower().startswith("modo cloud")


def _is_network_timeout_fallback(source_label: str) -> bool:
    """True quando o fallback ocorreu por timeout de rede (ex: cloud sem acesso à API)."""
    s = (source_label or "").lower()
    return "timed out" in s or "connect timeout" in s or "timeout" in s


def _is_auth_denied_fallback(source_label: str) -> bool:
    s = (source_label or "").lower()
    return "acesso negado (401/403)" in s or "401" in s or "403" in s


def _compact_source_label(source_label: str) -> str:
    s = str(source_label or "").strip()
    sl = s.lower()
    if not s:
        return "Fonte de dados indisponivel"
    if sl.startswith("ingestao em tempo real ativa"):
        return "Ingestao em tempo real ativa"
    if sl.startswith("endpoint em tempo real"):
        return "Endpoint em tempo real ativo"
    if sl.startswith("fallback local"):
        if _is_auth_denied_fallback(s):
            return "Fallback local: autenticacao da API negada (401/403)."
        if _is_network_timeout_fallback(s):
            return "Fallback local: timeout/rede na API ao vivo."
        if "endpoint local indisponivel no streamlit cloud" in sl:
            return "Fallback local: endpoint local indisponivel no Streamlit Cloud."
        return "Fallback local: API indisponivel no momento."
    if len(s) > 180:
        return s[:177] + "..."
    return s


def _server_status(source_label: str) -> tuple[str, str]:
    s = (source_label or "").lower()
    if s.startswith("endpoint em tempo real"):
        return "🟢", "Servidor Online"
    if s.startswith("ingestao em tempo real ativa"):
        return "🟢", "Servidor Online"
    if s.startswith("modo cloud"):
        return "🟡", "Modo Cloud (base local)"
    if s.startswith("fallback local"):
        return "🟡", "Fallback Local Ativo"
    if s.startswith("endpoint indisponivel"):
        return "🔴", "Servidor Offline"
    return "🟠", "Status Indefinido"


def _render_server_badge(source_label: str) -> None:
    icon, text = _server_status(source_label)
    st.markdown(f"**Status do Servidor:** {icon} {text}")


def _update_connection_state(source_label: str) -> None:
    icon, text = _server_status(source_label)
    st.session_state["server_connection"] = {
        "icon": icon,
        "text": text,
        "source": source_label,
        "updated_at": _now_br().isoformat(timespec="seconds"),
    }


def _extract_rodo_cuts(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Coleta rodos de todas as fontes possiveis e retorna lista deduplicada por id."""
    all_cuts: list[dict[str, Any]] = []
    seen_ids: set = set()

    def _add(source: Any) -> None:
        if not isinstance(source, list):
            return
        for c in source:
            if not isinstance(c, dict):
                continue
            cid = c.get("id")
            if cid is not None:
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
            all_cuts.append(c)

    # 1. top-level filtros_rodo
    _add(data.get("filtros_rodo"))
    # 2. filters.filtros_rodo e filters.toxic_cuts (podem ter rodos extras)
    filters = data.get("filters", {})
    if isinstance(filters, dict):
        _add(filters.get("filtros_rodo"))
        _add(filters.get("toxic_cuts"))

    return all_cuts


def _load_master_rodo_cuts(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str, str | None]:
    runtime = cfg.get("runtime_data", {})
    configured_path = str(runtime.get("rodo_master_path", "")).strip()
    path = Path(configured_path) if configured_path else RODOS_MASTER_PATH
    if not path.is_absolute():
        path = ROOT_DIR / path

    if not path.exists():
        return [], "Rodo master ausente", f"Arquivo de rodo master nao encontrado: {path.name}"

    try:
        master_cfg = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], "Rodo master invalido", f"Falha ao ler rodo master: {exc}"

    cuts = _extract_rodo_cuts(master_cfg)
    if not cuts:
        return [], "Rodo master vazio", "Rodo master sem filtros_rodo validos"

    return cuts, f"Rodo master: {path.name}", None


def _load_local_fallback_dataframe(target_date_iso: str, date_col: str, reason: str) -> tuple[pd.DataFrame, str]:
    reason_l = str(reason or "").lower()
    if "resposta do endpoint invalida" in reason_l:
        reason_ui = "resposta invalida da API"
    elif "endpoint indisponivel" in reason_l or "connection" in reason_l or "refused" in reason_l:
        reason_ui = "endpoint local offline"
    elif "endpoint nao configurado" in reason_l:
        reason_ui = "endpoint nao configurado"
    else:
        reason_ui = "falha de comunicacao com API"

    fallback_frames: list[pd.DataFrame] = []
    base_names: list[str] = []

    # Base operacional do dia: somente planilhas Apostas_*.xlsx na pasta Apostas_Diarias/.
    for glob_pattern in OPERATIONAL_GLOBS:
        for p_file in sorted(APOSTAS_DIR.glob(glob_pattern)):
            try:
                df_op = pd.read_excel(p_file)
                if not df_op.empty:
                    fallback_frames.append(df_op)
                    base_names.append(p_file.name)
            except Exception:
                continue

    # Base complementar local (recalculo).
    if LOCAL_FALLBACK_PATH.exists():
        try:
            df_csv = pd.read_csv(LOCAL_FALLBACK_PATH)
            if not df_csv.empty:
                fallback_frames.append(df_csv)
                base_names.append(LOCAL_FALLBACK_PATH.name)
        except Exception as exc:
            return pd.DataFrame(), f"Endpoint indisponivel ({reason}) | fallback local falhou: {exc}"

    if not fallback_frames:
        return pd.DataFrame(), f"Endpoint indisponivel ({reason_ui}) | fallback local ausente"

    df = pd.concat(fallback_frames, ignore_index=True, sort=False)
    if date_col not in df.columns:
        df = df.copy()
        df[date_col] = target_date_iso

    bases_txt = ", ".join(base_names)
    return df, f"Fallback local ({bases_txt}) | motivo: {reason_ui} | detalhe: {reason}"


def _load_live_then_local_fallback(
    target_date_iso: str,
    date_col: str,
    cfg: dict[str, Any],
    reason: str,
) -> tuple[pd.DataFrame, str]:
    try:
        live_df, live_source = load_live_dataframe(target_date_iso, cfg)
        if not live_df.empty:
            if date_col not in live_df.columns:
                live_df = live_df.copy()
                live_df[date_col] = target_date_iso
            return live_df, f"{live_source} | fallback apos: {reason}"
    except Exception:
        pass
    return _load_local_fallback_dataframe(target_date_iso, date_col, reason)


def _read_source_dataframe(target_date_iso: str, date_col: str, cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    runtime = cfg.get("runtime_data", {})

    # Prioriza ingestao live para evitar dependencia de localhost em ambientes cloud.
    live_df, live_source = load_live_dataframe(target_date_iso, cfg)
    if not live_df.empty:
        if date_col not in live_df.columns:
            live_df = live_df.copy()
            live_df[date_col] = target_date_iso
        return live_df, live_source

    # live_source contém o motivo pelo qual a API live falhou; propagar para diagnóstico.
    live_fail_reason = live_source if live_df.empty else ""

    endpoint_url = _resolve_endpoint_url(cfg)
    if _is_streamlit_cloud_runtime() and _is_local_endpoint(endpoint_url):
        # No Streamlit Cloud, localhost nunca estará disponível.
        # Se a ingestão live já trouxe motivo (ex: 401/403), prioriza esse diagnóstico.
        reason = live_fail_reason or "endpoint local indisponivel no Streamlit Cloud"
        return _load_local_fallback_dataframe(target_date_iso, date_col, reason)

    if not endpoint_url:
        reason = f"endpoint nao configurado | live: {live_fail_reason}" if live_fail_reason else "endpoint nao configurado"
        return _load_live_then_local_fallback(target_date_iso, date_col, cfg, reason)

    method = str(runtime.get("method", "GET")).strip().upper()
    date_param = str(runtime.get("date_param", "date")).strip() or "date"
    timeout_sec = max(float(runtime.get("timeout_sec", 10.0)), 10.0)
    headers = dict(runtime.get("headers", {}) or {})
    proxies = {"http": None, "https": None}

    token_env = str(runtime.get("auth_token_env", "")).strip()
    if token_env:
        token = os.getenv(token_env, "").strip()
        if token and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {token}"

    params = dict(runtime.get("fixed_query_params", {}) or {})
    params[date_param] = target_date_iso

    body = dict(runtime.get("fixed_body", {}) or {})
    body[date_param] = target_date_iso

    try:
        if method == "POST":
            response = requests.post(endpoint_url, json=body, headers=headers, timeout=timeout_sec, proxies=proxies)
        else:
            response = requests.get(endpoint_url, params=params, headers=headers, timeout=timeout_sec, proxies=proxies)
        response.raise_for_status()
    except Exception as exc:
        reason = f"endpoint indisponivel: {exc}"
        if live_fail_reason:
            reason = f"{reason} | live: {live_fail_reason}"
        return _load_live_then_local_fallback(target_date_iso, date_col, cfg, reason)

    content_type = (response.headers.get("Content-Type") or "").lower()
    df = pd.DataFrame()
    try:
        if "json" in content_type:
            records = _extract_records(response.json())
            df = pd.DataFrame(records)
        else:
            try:
                records = _extract_records(response.json())
                df = pd.DataFrame(records)
            except Exception:
                df = pd.read_csv(StringIO(response.text))
    except Exception as exc:
        return _load_live_then_local_fallback(target_date_iso, date_col, cfg, f"resposta do endpoint invalida: {exc}")

    if df.empty:
        return pd.DataFrame(), f"Endpoint em tempo real: {endpoint_url}"

    if date_col not in df.columns:
        df = df.copy()
        df[date_col] = target_date_iso

    return df, f"Endpoint em tempo real: {endpoint_url}"


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


def _build_apostas_excel(df: pd.DataFrame, data_iso: str) -> bytes:
    """Monta Excel de apostas com colunas extras para preenchimento manual."""
    cols_base = ["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real"]
    out = df[[c for c in cols_base if c in df.columns]].copy()
    out.rename(columns={"Odd real": "Odd_Base"}, inplace=True)
    out.insert(0, "Data", data_iso)
    # Colunas para preencher após o jogo
    out["Odd_Real_Pega"] = ""       # odd exata pega na hora
    out["Stake"] = ""               # responsabilidade apostada (R$)
    out["Resultado"] = ""           # GREEN / RED / VOID
    out["Lucro_Prejuizo"] = ""      # preencher se quiser sobrescrever o cálculo automático
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        out.to_excel(w, index=False, sheet_name="Apostas")
        ws = w.sheets["Apostas"]
        # largura das colunas
        for col_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)
    return buf.getvalue()


@st.cache_data(ttl=60)
def _load_games_for_date(cfg_path: str, target_date_iso: str) -> tuple[pd.DataFrame, str]:
    p_cfg = Path(cfg_path)
    if not p_cfg.exists():
        return pd.DataFrame(), "Configuracao indisponivel"

    try:
        cfg = json.loads(p_cfg.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame(), "Configuracao invalida"

    dt_cfg = cfg.get("input", {}).get("datetime", {})
    col_cfg = cfg.get("input", {}).get("columns", {})
    date_col = dt_cfg.get("date_col", "Data_Arquivo")
    time_col = dt_cfg.get("time_col", "Horario_Entrada")
    league_col = col_cfg.get("league_col", "Liga")
    method_col = col_cfg.get("method_col", "Metodo")
    odd_col = col_cfg.get("odd_signal_col", "Odd_Base")

    df, source_label = _read_source_dataframe(target_date_iso, date_col, cfg)
    if df.empty:
        return pd.DataFrame(), source_label

    # Filtro de range de odd por metodo (espelha servidor_arkad.py)
    filtros_metodo = cfg.get("runtime_data", {}).get("filtros_metodo", {})
    
    def _get_odd_limits(metodo: str) -> tuple[float | None, float | None]:
        flt = filtros_metodo.get(metodo)
        if not flt:
            return None, None
        return flt.get("odd_min"), flt.get("odd_max")

    if filtros_metodo and method_col in df.columns and odd_col in df.columns:
        def _passes_odd_filter(row: pd.Series) -> bool:
            m = str(row.get(method_col, ""))
            flt = filtros_metodo.get(m)
            if not flt:
                return True
            odd = pd.to_numeric(row.get(odd_col), errors="coerce")
            if pd.isna(odd):
                return False
            omn, omx = _get_odd_limits(m)
            if omn is not None and float(odd) < float(omn):
                return False
            if omx is not None and float(odd) > float(omx):
                return False
            ligas_perm = flt.get("ligas_permitidas")
            if ligas_perm:
                liga = str(row.get(league_col, "")).strip().upper()
                if liga not in {l.strip().upper() for l in ligas_perm}:
                    return False
            return True
        df = df[df.apply(_passes_odd_filter, axis=1)].copy()
        if df.empty:
            return pd.DataFrame(), source_label

    rodo_mode = str(cfg.get("runtime_data", {}).get("rodo_mode", "whitelist")).strip().lower()
    if rodo_mode not in {"whitelist", "blacklist"}:
        rodo_mode = "whitelist"

    cuts, rodo_label, rodo_err = _load_master_rodo_cuts(cfg)
    if rodo_err:
        return pd.DataFrame(), f"{source_label} | {rodo_label} | {rodo_err}"

    # Mescla rodos definidos diretamente no cfg (ex: config_universo_97.json tem rodos extras)
    cfg_cuts = _extract_rodo_cuts(cfg)
    if cfg_cuts:
        existing_ids = {c.get("id") for c in cuts if c.get("id") is not None}
        cuts = cuts + [c for c in cfg_cuts if c.get("id") not in existing_ids]

    if date_col not in df.columns or time_col not in df.columns:
        return pd.DataFrame(), source_label

    target_date = pd.Timestamp(pd.to_datetime(target_date_iso, errors="coerce").date())
    df = df.copy()
    df["__date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    df = df[df["__date"] == target_date].copy()
    if df.empty:
        return pd.DataFrame(), source_label

    if cuts:
        matched_rodo = df.apply(lambda r: any(_matches_cut(r, cut, league_col, method_col, odd_col) for cut in cuts), axis=1)
    else:
        matched_rodo = pd.Series([False] * len(df), index=df.index)

    df["__mins"] = df[time_col].apply(_parse_hhmm_to_minutes)
    df = df[df["__mins"].notna()].copy()
    if df.empty:
        return pd.DataFrame(), source_label
    if rodo_mode == "whitelist":
        df["Status"] = matched_rodo.reindex(df.index).map(lambda x: "EXECUTED" if bool(x) else "SKIP")
    else:
        df["Status"] = matched_rodo.reindex(df.index).map(lambda x: "SKIP" if bool(x) else "EXECUTED")

    odd_betfair = pd.to_numeric(df.get("Odd_Betfair"), errors="coerce")
    odd_source = pd.to_numeric(df.get(odd_col), errors="coerce")
    # Odd real deve refletir a odd da Betfair. Em registros BF puros, usa a odd da linha.
    odd_real = odd_betfair.copy()
    if "Fonte" in df.columns:
        fonte_is_betfair = df["Fonte"].astype(str).str.contains("fair", case=False, na=False)
        odd_real = odd_real.where(odd_real.notna(), odd_source.where(fonte_is_betfair))
    # Fallback final para evitar N/A quando nao houver casamento Betfair.
    odd_real = odd_real.where(odd_real.notna(), odd_source)

    # Bloqueia odds absurdas (>= 100): na Betfair significa mercado suspenso/indisponível.
    suspended = odd_real >= 100
    if suspended.any():
        df.loc[suspended[suspended].index, "Status"] = "SKIP"

    # Regra de confirmacao dupla: Lay_CS_1x0_B365 so executa se Lay_CS_0x1_B365 tambem EXECUTED no mesmo jogo
    if "Jogo" in df.columns and method_col in df.columns:
        exec_mask = df["Status"] == "EXECUTED"
        jogos_com_0x1_exec = set(
            df.loc[exec_mask & (df[method_col] == "Lay_CS_0x1_B365"), "Jogo"].astype(str).str.strip()
        )
        solo_1x0 = (
            (df[method_col] == "Lay_CS_1x0_B365") &
            (df["Status"] == "EXECUTED") &
            (~df["Jogo"].astype(str).str.strip().isin(jogos_com_0x1_exec))
        )
        df.loc[solo_1x0, "Status"] = "SKIP"

    def _calc_prio(metodo: str, odd: float) -> str:
        if metodo == "Lay_CS_0x1_B365":
            return "P1 ⭐" if odd >= 9 else "P2"
        if metodo == "Lay_CS_1x0_B365":
            return "P3" if odd < 9 else "P4"
        return "P?"

    prio_col = [
        _calc_prio(str(m), float(o) if pd.notna(o) else 0.0)
        for m, o in zip(df.get(method_col, ""), odd_real)
    ]

    out = pd.DataFrame(
        {
            "Hora": df[time_col],
            "Prio": prio_col,
            "Liga": df.get(league_col, ""),
            "Jogo": df.get("Jogo", ""),
            "Metodo": df.get(method_col, ""),
            "Odd real": odd_real,
            "Status": df["Status"],
            "__mins": pd.to_numeric(df["__mins"], errors="coerce"),
            "PnL_Linha": pd.to_numeric(df.get("PnL_Linha"), errors="coerce"),
        }
    )
    return out.sort_values(["Prio", "__mins", "Jogo"]).reset_index(drop=True), f"{source_label} | {rodo_label}"


def main() -> None:
    st.set_page_config(page_title="Arkad Sinais", layout="centered")
    st.title("🎯 Arkad: Sinais de Hoje")

    if "server_connection" not in st.session_state:
        st.session_state["server_connection"] = {
            "icon": "🟠",
            "text": "Status Indefinido",
            "source": "Nao testado",
            "updated_at": _now_br().isoformat(timespec="seconds"),
        }

    selected_date = st.sidebar.date_input("📅 Ver Outra Data", value=_now_br().date(), format="YYYY-MM-DD")

    now = _now_br()
    today_iso = now.date().isoformat()
    selected_iso = selected_date.isoformat()
    is_today_selected = selected_iso == today_iso

    if _is_streamlit_cloud_runtime():
        st.caption("No Streamlit Cloud, dados em tempo real dependem do token da API FutPython nos Secrets.")
    else:
        st.caption("Se o status estiver vermelho, verifique o servidor local no PC de BH (porta 8080).")

    if is_today_selected:
        games_today, source_label = _load_games_for_date(str(PROD_CFG_PATH), today_iso)
        _update_connection_state(source_label)
        _render_server_badge(source_label)
        st.caption(f"Fonte de dados: {_compact_source_label(source_label)}")
        show_technical_details = False
        expand_technical_details = False
        if _is_local_fallback(source_label):
            if _is_auth_denied_fallback(source_label):
                st.error("Falha de autenticacao na API (401/403). Verifique FUTPYTHON_TOKEN nos Secrets do Streamlit Cloud.")
                expand_technical_details = True
            elif _is_network_timeout_fallback(source_label):
                st.info("API ao vivo indisponivel por rede/timeout. Exibindo sinais da base local.")
            else:
                st.warning("API indisponivel no momento. Exibindo sinais do arquivo local.")
            show_technical_details = True
        elif _is_cloud_fallback(source_label):
            st.info("Rodando no Streamlit Cloud. API FutPython não é acessível remotamente. Use o app local para sinais em tempo real.")
        if show_technical_details:
            with st.expander("Detalhes tecnicos da fonte de dados", expanded=expand_technical_details):
                st.code(source_label, language=None)
        if _is_endpoint_connection_error(source_label):
            st.warning("⚠️ Aguardando conexão com o servidor de sinais...")
            st.caption("Nova tentativa automática em 30 segundos...")
            time.sleep(30)
            st.rerun()
        approved_today = games_today[games_today["Status"] == "EXECUTED"].copy() if not games_today.empty else pd.DataFrame()
        now_minutes = now.hour * 60 + now.minute

        st.divider()
        st.subheader("✅ Jogos Aprovados do Dia Inteiro")
        if approved_today.empty:
            st.info("Sem jogos aprovados no dia inteiro.")
        else:
            full_day_view = approved_today[["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status", "__mins"]].copy()
            full_day_view["Destaque"] = full_day_view["__mins"].map(
                lambda m: "ENTRADA AGORA" if (pd.notna(m) and now_minutes <= float(m)) else (
                    "HOJE MAIS TARDE" if (pd.notna(m) and float(m) > now_minutes) else "JA PASSOU"
                )
            )
            full_day_view["Odd real"] = pd.to_numeric(full_day_view["Odd real"], errors="coerce").map(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )
            full_day_view = full_day_view[["Destaque", "Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status"]]

            def _highlight_row(row: pd.Series) -> list[str]:
                tag = str(row.get("Destaque", ""))
                if tag == "ENTRADA AGORA":
                    return ["background-color: #d1fae5; font-weight: 700"] * len(row)
                if tag == "HOJE MAIS TARDE":
                    return ["background-color: #eff6ff"] * len(row)
                return [""] * len(row)

            st.dataframe(full_day_view.style.apply(_highlight_row, axis=1), use_container_width=True)

        if approved_today.empty:
            _xls_data = b""
            _xls_disabled = True
        else:
            _xls_data = _build_apostas_excel(approved_today, today_iso)
            _xls_disabled = False
        st.download_button(
            "📥 Baixar Apostas de Hoje (Excel)",
            data=_xls_data,
            file_name=f"Apostas_{today_iso.replace('-','')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            disabled=_xls_disabled,
            use_container_width=True,
        )
    else:
        historical_games, source_label = _load_games_for_date(str(PROD_CFG_PATH), selected_iso)
        _update_connection_state(source_label)
        _render_server_badge(source_label)
        is_future = selected_iso > today_iso
        st.caption(f"Fonte de dados: {source_label}")
        if _is_local_fallback(source_label):
            if is_future:
                st.info("📂 Usando base histórica local (sem dados ao vivo para datas futuras).")
            else:
                st.info("📂 Usando base histórica local para esta data. API só fornece dados do dia atual.")
        elif _is_cloud_fallback(source_label):
            st.info("💻 Modo cloud: usando base local.")
        if _is_endpoint_connection_error(source_label):
            st.warning("⚠️ Aguardando conexão com o servidor de sinais...")
            st.caption("Nova tentativa automática em 30 segundos...")
            time.sleep(30)
            st.rerun()
        historical_exec = historical_games[historical_games["Status"] == "EXECUTED"].copy() if not historical_games.empty else pd.DataFrame()

        st.divider()
        label_data = "🔜 Jogos Aprovados para" if is_future else "✅ Jogos Selecionados em"
        st.subheader(f"{label_data} {selected_iso}")
        if historical_exec.empty:
            st.info("Nenhum jogo encontrado para esta data.")
        else:
            _cols_hist = [c for c in ["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status", "PnL_Linha"] if c in historical_exec.columns]
            hist_view = historical_exec[_cols_hist].copy()
            if "Odd real" in hist_view.columns:
                hist_view["Odd real"] = pd.to_numeric(hist_view["Odd real"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if "PnL_Linha" in hist_view.columns:
                hist_view = hist_view.rename(columns={"PnL_Linha": "PnL"})
                hist_view["PnL"] = pd.to_numeric(hist_view["PnL"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            st.dataframe(hist_view, use_container_width=True, hide_index=True)

        # Download Excel para qualquer data selecionada
        if not historical_exec.empty:
            _xls2 = _build_apostas_excel(historical_exec, selected_iso)
            _dis2 = False
        else:
            _xls2 = b""
            _dis2 = True
        st.download_button(
            "📥 Baixar Apostas (Excel)",
            data=_xls2,
            file_name=f"Apostas_{selected_iso.replace('-','')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            disabled=_dis2,
            use_container_width=True,
        )

if __name__ == "__main__":
    main()

