from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent
CSV_PATH = ROOT_DIR / "recalculo_sem_combos_usuario.csv"
PROD_CFG_PATH = ROOT_DIR / "config_prod_v1.json"


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
def _load_games_for_date(csv_path: str, cfg_path: str, target_date_iso: str) -> pd.DataFrame:
    p_csv = Path(csv_path)
    p_cfg = Path(cfg_path)
    if (not p_csv.exists()) or (not p_cfg.exists()):
        return pd.DataFrame()

    try:
        df = pd.read_csv(p_csv)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    try:
        cfg = json.loads(p_cfg.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame()

    dt_cfg = cfg.get("input", {}).get("datetime", {})
    col_cfg = cfg.get("input", {}).get("columns", {})
    date_col = dt_cfg.get("date_col", "Data_Arquivo")
    time_col = dt_cfg.get("time_col", "Horario_Entrada")
    league_col = col_cfg.get("league_col", "Liga")
    method_col = col_cfg.get("method_col", "Metodo")
    odd_col = col_cfg.get("odd_signal_col", "Odd_Base")

    if date_col not in df.columns or time_col not in df.columns:
        return pd.DataFrame()

    target_date = pd.Timestamp(pd.to_datetime(target_date_iso, errors="coerce").date())
    df = df.copy()
    df["__date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    df = df[df["__date"] == target_date].copy()
    if df.empty:
        return pd.DataFrame()

    cuts = cfg.get("filtros_rodo") or cfg.get("filters", {}).get("filtros_rodo", [])
    if cuts:
        blocked = df.apply(lambda r: any(_matches_cut(r, cut, league_col, method_col, odd_col) for cut in cuts), axis=1)
    else:
        blocked = pd.Series([False] * len(df), index=df.index)

    df["__mins"] = df[time_col].apply(_parse_hhmm_to_minutes)
    df = df[df["__mins"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
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
    return out.sort_values("__mins").reset_index(drop=True)


def main() -> None:
    st.set_page_config(page_title="Arkad Sinais", layout="centered")
    st.title("🎯 Arkad: Sinais de Hoje")

    selected_date = st.sidebar.date_input("📅 Ver Outra Data", value=date.today(), format="YYYY-MM-DD")

    now = datetime.now()
    today_iso = now.date().isoformat()
    selected_iso = selected_date.isoformat()
    is_today_selected = selected_iso == today_iso

    if is_today_selected:
        games_today = _load_games_for_date(str(CSV_PATH), str(PROD_CFG_PATH), today_iso)
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
        historical_games = _load_games_for_date(str(CSV_PATH), str(PROD_CFG_PATH), selected_iso)
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

