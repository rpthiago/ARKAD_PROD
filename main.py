from __future__ import annotations

import json
import os
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent
PROD_CFG_PATH = ROOT_DIR / "config_prod_v1.json"
RODOS_MASTER_PATH = ROOT_DIR / "config_rodos_master.json"


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


def _read_source_dataframe(target_date_iso: str, date_col: str, cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    runtime = cfg.get("runtime_data", {})
    endpoint_url = str(runtime.get("endpoint_url", "")).strip()
    if not endpoint_url:
        return pd.DataFrame(), "Endpoint nao configurado"

    method = str(runtime.get("method", "GET")).strip().upper()
    date_param = str(runtime.get("date_param", "date")).strip() or "date"
    timeout_sec = float(runtime.get("timeout_sec", 20.0))
    headers = dict(runtime.get("headers", {}) or {})

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
            response = requests.post(endpoint_url, json=body, headers=headers, timeout=timeout_sec)
        else:
            response = requests.get(endpoint_url, params=params, headers=headers, timeout=timeout_sec)
        response.raise_for_status()
    except Exception as exc:
        return pd.DataFrame(), f"Endpoint indisponivel: {exc}"

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
        return pd.DataFrame(), f"Resposta do endpoint invalida: {exc}"

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

    cut_leagues = set(cut.get("leagues", []))
    if cut.get("league"):
        cut_leagues.add(str(cut["league"]))
    if cut_leagues and league not in cut_leagues:
        return False

    method_equals = cut.get("method_equals")
    method_contains = cut.get("method_contains")
    if method_equals and method != str(method_equals):
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
        blocked = df.apply(lambda r: any(_matches_cut(r, cut, league_col, method_col, odd_col) for cut in cuts), axis=1)
    else:
        blocked = pd.Series([False] * len(df), index=df.index)

    df["__mins"] = df[time_col].apply(_parse_hhmm_to_minutes)
    df = df[df["__mins"].notna()].copy()
    if df.empty:
        return pd.DataFrame(), source_label
    df["Status"] = blocked.reindex(df.index).map(lambda x: "SKIP" if bool(x) else "EXECUTED")

    out = pd.DataFrame(
        {
            "Hora": df[time_col],
            "Jogo": df.get("Jogo", ""),
            "Odd sugerida": pd.to_numeric(df.get(odd_col), errors="coerce"),
            "Status": df["Status"],
            "__mins": pd.to_numeric(df["__mins"], errors="coerce"),
            "PnL_Linha": pd.to_numeric(df.get("PnL_Linha"), errors="coerce"),
        }
    )
    return out.sort_values("__mins").reset_index(drop=True), f"{source_label} | {rodo_label}"


def main() -> None:
    st.set_page_config(page_title="Arkad Sinais", layout="centered")
    st.title("🎯 Arkad: Sinais de Hoje")

    selected_date = st.sidebar.date_input("📅 Ver Outra Data", value=date.today(), format="YYYY-MM-DD")

    now = datetime.now()
    today_iso = now.date().isoformat()
    selected_iso = selected_date.isoformat()
    is_today_selected = selected_iso == today_iso

    if is_today_selected:
        games_today, source_label = _load_games_for_date(str(PROD_CFG_PATH), today_iso)
        st.caption(f"Fonte de dados: {source_label}")
        approved_today = games_today[games_today["Status"] == "EXECUTED"].copy() if not games_today.empty else pd.DataFrame()
        now_minutes = now.hour * 60 + now.minute

        agenda = approved_today[approved_today["__mins"] >= now_minutes].copy() if not approved_today.empty else pd.DataFrame()
        entry_window = agenda[agenda["__mins"] <= (now_minutes + 120)].copy() if not agenda.empty else pd.DataFrame()

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
            jogo_atual = entry_window.iloc[0]
            jogo_nome = str(jogo_atual.get("Jogo", "Jogo sem nome"))
            hora = str(jogo_atual.get("Hora", "--:--"))
            odd = pd.to_numeric(jogo_atual.get("Odd sugerida"), errors="coerce")
            odd_txt = f"{float(odd):.2f}" if pd.notna(odd) else "N/A"

            st.success(f"### {jogo_nome}")
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Horario:** {hora}")
            c2.write(f"**Odd Sugerida:** {odd_txt}")
            if c3.button("COPIAR NOME"):
                st.session_state["nome_jogo_copiado"] = jogo_nome
                st.toast("Nome pronto para copiar", icon="📋")

            if st.session_state.get("nome_jogo_copiado"):
                st.caption("Copie o nome abaixo:")
                st.code(st.session_state["nome_jogo_copiado"], language="text")

            st.info("👉 Va para a Betfair e procure por este jogo no mercado 'Resultado Correto' (Lay 0x1)")

        st.divider()
        st.subheader("📅 Proximos Jogos Aprovados")
        if agenda.empty:
            st.info("Nenhuma oportunidade segura no momento. Aguardando mercado.")
        else:
            agenda_view = agenda[["Hora", "Jogo", "Odd sugerida", "Status"]].copy()
            agenda_view["Odd sugerida"] = pd.to_numeric(agenda_view["Odd sugerida"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            st.table(agenda_view)

        st.divider()
        st.subheader("✅ Jogos Aprovados do Dia Inteiro")
        if approved_today.empty:
            st.info("Sem jogos aprovados no dia inteiro.")
        else:
            full_day_view = approved_today[["Hora", "Jogo", "Odd sugerida", "Status", "__mins"]].copy()
            full_day_view["Destaque"] = full_day_view["__mins"].map(
                lambda m: "ENTRADA AGORA" if (pd.notna(m) and now_minutes <= float(m) <= (now_minutes + 120)) else (
                    "HOJE MAIS TARDE" if (pd.notna(m) and float(m) > (now_minutes + 120)) else "JA PASSOU"
                )
            )
            full_day_view["Odd sugerida"] = pd.to_numeric(full_day_view["Odd sugerida"], errors="coerce").map(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )
            full_day_view = full_day_view[["Destaque", "Hora", "Jogo", "Odd sugerida", "Status"]]

            def _highlight_row(row: pd.Series) -> list[str]:
                tag = str(row.get("Destaque", ""))
                if tag == "ENTRADA AGORA":
                    return ["background-color: #d1fae5; font-weight: 700"] * len(row)
                if tag == "HOJE MAIS TARDE":
                    return ["background-color: #eff6ff"] * len(row)
                return [""] * len(row)

            st.dataframe(full_day_view.style.apply(_highlight_row, axis=1), use_container_width=True)

        if approved_today.empty:
            csv_data = "Hora,Jogo,Odd sugerida,Status\n"
        else:
            download_df = approved_today[["Hora", "Jogo", "Odd sugerida", "Status"]].copy()
            download_df["Odd sugerida"] = pd.to_numeric(download_df["Odd sugerida"], errors="coerce")
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
        st.caption(f"Fonte de dados: {source_label}")
        historical_exec = historical_games[historical_games["Status"] == "EXECUTED"].copy() if not historical_games.empty else pd.DataFrame()

        st.divider()
        st.subheader(f"✅ Jogos Selecionados em {selected_iso}")
        if historical_exec.empty:
            st.info("Nenhum jogo processado nesta data.")
        else:
            hist_view = historical_exec[["Hora", "Jogo", "Odd sugerida", "PnL_Linha"]].copy()
            hist_view = hist_view.rename(columns={"Odd sugerida": "Odd", "PnL_Linha": "PnL"})
            hist_view["Odd"] = pd.to_numeric(hist_view["Odd"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            hist_view["PnL"] = pd.to_numeric(hist_view["PnL"], errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            st.table(hist_view)

    if st.sidebar.button("🚨 EMERGENCY ROLLBACK", type="primary"):
        st.sidebar.error("SISTEMA REVERTIDO!")


if __name__ == "__main__":
    main()

