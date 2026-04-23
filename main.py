from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st

from ingestao_tempo_real import load_live_dataframe

ROOT_DIR = Path(__file__).resolve().parent
PROD_CFG_PATH = ROOT_DIR / "config_prod_v1.json"
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


def _resolve_endpoint_url() -> str:
    # Fluxo fixo local para evitar URL antiga de tunel/externa.
    return FIXED_ENDPOINT_URL


def _probe_api_url(api_url: str) -> tuple[bool, str]:
    if not api_url:
        return False, "URL vazia"
    headers: dict[str, str] = {}
    proxies = {"http": None, "https": None}
    test_date = datetime.now().date().isoformat()
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


def _server_status(source_label: str) -> tuple[str, str]:
    s = (source_label or "").lower()
    if s.startswith("endpoint em tempo real"):
        return "🟢", "Servidor Online"
    if s.startswith("ingestao em tempo real ativa"):
        return "🟢", "Servidor Online"
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
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _extract_rodo_cuts(data: dict[str, Any]) -> list[dict[str, Any]]:
    cuts = data.get("filtros_rodo")
    if isinstance(cuts, list):
        return [c for c in cuts if isinstance(c, dict)]

    filters = data.get("filters", {})
    if isinstance(filters, dict):
        nested_cuts = filters.get("filtros_rodo") or filters.get("toxic_cuts")
        if isinstance(nested_cuts, list):
            return [c for c in nested_cuts if isinstance(c, dict)]
    return []


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

    # Base operacional do dia: somente planilhas Apostas_*.xlsx na raiz do projeto.
    for glob_pattern in OPERATIONAL_GLOBS:
        for p_file in sorted(ROOT_DIR.glob(glob_pattern)):
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
    return df, f"Fallback local ({bases_txt}) | motivo: {reason_ui}"


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

    endpoint_url = _resolve_endpoint_url()
    if not endpoint_url:
        return _load_live_then_local_fallback(target_date_iso, date_col, cfg, "endpoint nao configurado")

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
        return _load_live_then_local_fallback(target_date_iso, date_col, cfg, f"endpoint indisponivel: {exc}")

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

    rodo_mode = str(cfg.get("runtime_data", {}).get("rodo_mode", "whitelist")).strip().lower()
    if rodo_mode not in {"whitelist", "blacklist"}:
        rodo_mode = "whitelist"

    cuts, rodo_label, rodo_err = _load_master_rodo_cuts(cfg)
    if rodo_err:
        return pd.DataFrame(), f"{source_label} | {rodo_label} | {rodo_err}"

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
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    if st.sidebar.button("🧪 Testar Conexao da API", use_container_width=True):
        active_url = _resolve_endpoint_url()
        ok, msg = _probe_api_url(active_url)
        if ok:
            st.sidebar.success(f"Conexao OK: {msg}")
        else:
            st.sidebar.error(f"Falha na API: {msg}")

    if st.sidebar.button("🔄 Tentar Reconectar Agora", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    selected_date = st.sidebar.date_input("📅 Ver Outra Data", value=date.today(), format="YYYY-MM-DD")

    now = datetime.now()
    today_iso = now.date().isoformat()
    selected_iso = selected_date.isoformat()
    is_today_selected = selected_iso == today_iso

    try:
        endpoint_dbg = _resolve_endpoint_url()
        if endpoint_dbg:
            st.write(f"Debug API URL: {endpoint_dbg}")
    except Exception:
        pass

    st.caption("Se o status estiver vermelho, verifique o servidor local no PC de BH (porta 8080).")

    if is_today_selected:
        games_today, source_label = _load_games_for_date(str(PROD_CFG_PATH), today_iso)
        _update_connection_state(source_label)
        _render_server_badge(source_label)
        st.caption(f"Fonte de dados: {source_label}")
        if _is_local_fallback(source_label):
            st.warning("API indisponivel. Operando com fallback local em arquivo.")
        if _is_endpoint_connection_error(source_label):
            st.warning("⚠️ Aguardando conexão com o servidor de sinais...")
            st.caption("Nova tentativa automática em 30 segundos...")
            time.sleep(30)
            st.rerun()
        approved_today = games_today[games_today["Status"] == "EXECUTED"].copy() if not games_today.empty else pd.DataFrame()
        now_minutes = now.hour * 60 + now.minute

        agenda = approved_today[approved_today["__mins"] >= now_minutes].copy() if not approved_today.empty else pd.DataFrame()
        entry_window = agenda.copy() if not agenda.empty else pd.DataFrame()

        lucro_hoje = float(pd.to_numeric(approved_today.get("PnL_Linha"), errors="coerce").fillna(0).sum()) if not approved_today.empty else 0.0
        status_txt = "Oportunidade encontrada" if not entry_window.empty else "Aguardando oportunidade"

        col1, col2 = st.columns(2)
        col1.metric("Lucro Hoje", f"R$ {lucro_hoje:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        col2.metric("Status", status_txt)

        st.divider()
        st.subheader("🔥 ENTRADA AGORA")

        if entry_window.empty:
            st.info("Aguardando próxima oportunidade confirmada pelo Rodo")
        else:
            entry_view = entry_window[["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status"]].copy()
            entry_view["Odd real"] = pd.to_numeric(entry_view["Odd real"], errors="coerce").map(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )
            st.success(f"{len(entry_view)} oportunidade(s) confirmada(s) pelo Rodo")
            st.table(entry_view)
            st.info("👉 Procure os jogos acima na Betfair no mercado 'Resultado Correto' (Lay 0x1 / Lay 1x0)")

        st.divider()
        st.subheader("📅 Proximos Jogos Aprovados")
        if agenda.empty:
            st.info("Nenhuma oportunidade segura no momento. Aguardando mercado.")
        else:
            agenda_view = agenda[["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status"]].copy()
            agenda_view["Odd real"] = pd.to_numeric(agenda_view["Odd real"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            st.table(agenda_view)

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
            csv_data = "Prio,Hora,Liga,Jogo,Metodo,Odd real,Status\n"
        else:
            download_df = approved_today[["Prio", "Hora", "Liga", "Jogo", "Metodo", "Odd real", "Status"]].copy()
            download_df["Odd real"] = pd.to_numeric(download_df["Odd real"], errors="coerce")
            csv_data = download_df.to_csv(index=False)
        st.download_button(
            "📥 Baixar Lista de Hoje",
            data=csv_data,
            file_name=f"jogos_hoje_{today_iso}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    else:
        historical_games, source_label = _load_games_for_date(str(PROD_CFG_PATH), selected_iso)
        _update_connection_state(source_label)
        _render_server_badge(source_label)
        st.caption(f"Fonte de dados: {source_label}")
        if _is_local_fallback(source_label):
            st.warning("API indisponivel. Operando com fallback local em arquivo.")
        if _is_endpoint_connection_error(source_label):
            st.warning("⚠️ Aguardando conexão com o servidor de sinais...")
            st.caption("Nova tentativa automática em 30 segundos...")
            time.sleep(30)
            st.rerun()
        historical_exec = historical_games[historical_games["Status"] == "EXECUTED"].copy() if not historical_games.empty else pd.DataFrame()

        st.divider()
        st.subheader(f"✅ Jogos Selecionados em {selected_iso}")
        if historical_exec.empty:
            st.info("Nenhum jogo processado nesta data.")
        else:
            hist_view = historical_exec[["Prio", "Hora", "Liga", "Jogo", "Odd real", "PnL_Linha"]].copy()
            hist_view = hist_view.rename(columns={"Odd real": "Odd", "PnL_Linha": "PnL"})
            hist_view["Odd"] = pd.to_numeric(hist_view["Odd"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            hist_view["PnL"] = pd.to_numeric(hist_view["PnL"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            st.table(hist_view)

    if st.sidebar.button("🚨 EMERGENCY ROLLBACK", type="primary"):
        st.sidebar.error("SISTEMA REVERTIDO!")


if __name__ == "__main__":
    main()

