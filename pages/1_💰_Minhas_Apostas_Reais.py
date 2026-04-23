"""
Resumo de Apostas Reais — ARKAD_PROD
Lê todas as planilhas Apostas_*.xlsx na raiz do projeto e consolida P&L.
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Minhas Apostas Reais", page_icon="💰", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIR_APOSTAS = ROOT_DIR  # planilhas ficam na raiz: Apostas_*.xlsx


# ─── Carregamento ─────────────────────────────────────────────────────────────

def _carregar_planilhas() -> pd.DataFrame:
    arquivos = sorted(glob.glob(str(DIR_APOSTAS / "Apostas_*.xlsx")))
    if not arquivos:
        return pd.DataFrame()
    frames = []
    for arq in arquivos:
        try:
            df = pd.read_excel(arq)
            df["__arquivo"] = Path(arq).name
            frames.append(df)
        except Exception as e:
            st.warning(f"Erro ao ler {Path(arq).name}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ─── Cálculo de PnL ───────────────────────────────────────────────────────────

def _calcular_pnl(row: pd.Series) -> float:
    resultado = str(row.get("Resultado", "")).strip().upper()
    if resultado in ("", "NAN", "PENDENTE", "-"):
        return np.nan
    # Se já preenchido manualmente
    pnl_manual = row.get("Lucro_Prejuizo") or row.get("PnL") or row.get("PnL_Linha")
    if pd.notna(pnl_manual) and str(pnl_manual).strip() not in ("", "nan"):
        try:
            return float(pnl_manual)
        except Exception:
            pass
    try:
        stake = float(row.get("Stake") or row.get("Responsabilidade_Sugerida_R$") or 0)
        odd = float(row.get("Odd_Real_Pega") or row.get("Odd real") or row.get("Odd_Base") or 0)
    except Exception:
        return np.nan
    if stake <= 0 or odd <= 1:
        return np.nan
    if any(k in resultado for k in ("GREEN", "G", "VITORIA", "VIT")):
        return round(stake * (1.0 / (odd - 1.0)), 2)  # ganho no lay
    if any(k in resultado for k in ("RED", "R", "DERROTA", "DER")):
        return round(-stake, 2)
    if any(k in resultado for k in ("VOID", "V", "DEVOLVIDA", "REEMBOLSO")):
        return 0.0
    return np.nan


def _prio_label(row: pd.Series) -> str:
    metodo = str(row.get("Metodo", ""))
    odd = pd.to_numeric(row.get("Odd_Base") or row.get("Odd real") or row.get("Odd_Real_Pega"), errors="coerce")
    odd = float(odd) if pd.notna(odd) else 0.0
    if metodo == "Lay_CS_0x1_B365":
        return "P1 ⭐" if odd >= 9 else "P2"
    if metodo == "Lay_CS_1x0_B365":
        return "P3" if odd < 9 else "P4"
    return "P?"


# ─── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.title("💰 Minhas Apostas Reais")
    st.caption("Consolidação baseada nas planilhas `Apostas_*.xlsx` na raiz do projeto.")

    df_raw = _carregar_planilhas()
    if df_raw.empty:
        st.info("📂 Nenhuma planilha `Apostas_*.xlsx` encontrada. Salve as planilhas operacionais na raiz do projeto.")
        return

    # ── Normalizar coluna de resultado
    res_col = next((c for c in df_raw.columns if "resultado" in c.lower()), None)
    if res_col:
        df_raw["Resultado"] = df_raw[res_col].fillna("").astype(str).str.strip().str.upper()
    else:
        df_raw["Resultado"] = ""

    df_raw["PnL_Calc"] = df_raw.apply(_calcular_pnl, axis=1)
    df_raw["Prio"] = df_raw.apply(_prio_label, axis=1)

    # ── Separar resolvidas
    resolvidas = df_raw[df_raw["PnL_Calc"].notna()].copy()
    pendentes  = df_raw[df_raw["PnL_Calc"].isna()].copy()

    # ── Métricas gerais ──────────────────────────────────────────────────────
    st.markdown("### 📊 Desempenho Geral")
    total = len(resolvidas)
    greens = int((resolvidas["PnL_Calc"] > 0).sum())
    reds   = int((resolvidas["PnL_Calc"] < 0).sum())
    voids  = int((resolvidas["PnL_Calc"] == 0).sum())
    lucro_total = float(resolvidas["PnL_Calc"].sum())
    wr = greens / (greens + reds) * 100 if (greens + reds) > 0 else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Resolvidas", total)
    c2.metric("Win Rate", f"{wr:.1f}%")
    c3.metric("Greens / Reds", f"{greens}G / {reds}R")
    c4.metric("Pendentes", len(pendentes))
    lucro_fmt = f"R$ {lucro_total:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    c5.metric("Lucro Total", lucro_fmt, delta_color="normal" if lucro_total >= 0 else "inverse")

    st.divider()

    # ── Gráficos ──────────────────────────────────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 📈 Evolução do P&L Acumulado")
        df_ord = resolvidas.copy()
        # Tenta ordenar por data
        date_col = next((c for c in df_ord.columns if "data" in c.lower() or "date" in c.lower()), None)
        if date_col:
            df_ord[date_col] = pd.to_datetime(df_ord[date_col], errors="coerce")
            df_ord = df_ord.sort_values(date_col)
        df_ord["P&L Acum"] = df_ord["PnL_Calc"].cumsum()
        color = "#27ae60" if lucro_total >= 0 else "#e74c3c"
        fig_ev = go.Figure()
        fig_ev.add_trace(go.Scatter(
            x=list(range(1, len(df_ord) + 1)),
            y=df_ord["P&L Acum"],
            mode="lines+markers",
            line=dict(color=color, width=2),
            fill="tozeroy",
            name="P&L Acum",
        ))
        fig_ev.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_ev.update_layout(
            xaxis_title="Nº Aposta",
            yaxis_title="Lucro Acumulado (R$)",
            height=320,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_ev, use_container_width=True)

    with col_b:
        st.markdown("#### 🎯 Resultado por Método")
        if "Metodo" in resolvidas.columns:
            gm = resolvidas.groupby("Metodo").agg(
                Apostas=("PnL_Calc", "count"),
                Lucro=("PnL_Calc", "sum"),
                WR=("PnL_Calc", lambda x: (x > 0).mean() * 100),
            ).reset_index().sort_values("Lucro", ascending=False)
            gm["Lucro"] = gm["Lucro"].round(2)
            gm["WR"] = gm["WR"].round(1).astype(str) + "%"
            fig_bar = px.bar(gm, x="Metodo", y="Lucro", color="Lucro",
                             color_continuous_scale=["#e74c3c", "#f39c12", "#27ae60"],
                             text="Lucro", height=320)
            fig_bar.update_traces(texttemplate="R$ %{text:.0f}", textposition="outside")
            fig_bar.update_layout(margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()

    # ── Desempenho por Prioridade ─────────────────────────────────────────────
    st.markdown("#### ⭐ Desempenho por Prioridade (P1-P4)")
    gp = resolvidas.groupby("Prio").agg(
        Apostas=("PnL_Calc", "count"),
        Lucro=("PnL_Calc", "sum"),
        WR=("PnL_Calc", lambda x: (x > 0).mean() * 100),
    ).reset_index().sort_values("Prio")
    gp["Lucro"] = gp["Lucro"].map(lambda x: f"R$ {x:+,.2f}")
    gp["WR"] = gp["WR"].map(lambda x: f"{x:.1f}%")
    st.dataframe(gp, use_container_width=True, hide_index=True)

    st.divider()

    # ── Desempenho por Liga ───────────────────────────────────────────────────
    liga_col = next((c for c in resolvidas.columns if c.lower() == "liga"), None)
    if liga_col:
        st.markdown("#### 🏟️ Top Ligas")
        gl = resolvidas.groupby(liga_col).agg(
            Apostas=("PnL_Calc", "count"),
            Lucro=("PnL_Calc", "sum"),
            WR=("PnL_Calc", lambda x: (x > 0).mean() * 100),
        ).reset_index().sort_values("Lucro", ascending=False).head(15)
        gl["Lucro"] = gl["Lucro"].map(lambda x: f"R$ {x:+,.2f}")
        gl["WR"] = gl["WR"].map(lambda x: f"{x:.1f}%")
        st.dataframe(gl, use_container_width=True, hide_index=True)
        st.divider()

    # ── Tabela completa ───────────────────────────────────────────────────────
    st.markdown("#### 📋 Todas as Apostas Resolvidas")

    col_exibir = []
    for c in ["Prio", date_col, "Horario_Entrada", "Liga", "Jogo", "Metodo",
              "Odd_Base", "Odd_Real_Pega", "Stake", "Responsabilidade_Sugerida_R$",
              "Resultado", "PnL_Calc"]:
        if c and c in resolvidas.columns:
            col_exibir.append(c)
    # fallback: mostra tudo
    if not col_exibir:
        col_exibir = list(resolvidas.columns)

    tbl = resolvidas[col_exibir].copy()
    tbl["PnL_Calc"] = tbl["PnL_Calc"].map(lambda x: f"R$ {x:+,.2f}" if pd.notna(x) else "")

    def _cor_linha(row: pd.Series) -> list[str]:
        res = str(row.get("Resultado", "")).upper()
        if "GREEN" in res or res == "G":
            return ["background-color: #d1fae5"] * len(row)
        if "RED" in res or res == "R":
            return ["background-color: #fee2e2"] * len(row)
        return [""] * len(row)

    st.dataframe(tbl.style.apply(_cor_linha, axis=1), use_container_width=True)

    # ── Pendentes ─────────────────────────────────────────────────────────────
    if not pendentes.empty:
        st.divider()
        st.markdown(f"#### ⏳ Apostas Pendentes de Resultado ({len(pendentes)})")
        col_pend = [c for c in ["Prio", date_col, "Liga", "Jogo", "Metodo", "Odd_Base", "Stake", "Resultado"]
                    if c and c in pendentes.columns]
        st.dataframe(pendentes[col_pend] if col_pend else pendentes, use_container_width=True, hide_index=True)

    # ── Download ──────────────────────────────────────────────────────────────
    st.divider()
    csv_out = resolvidas[col_exibir].copy()
    st.download_button(
        "📥 Baixar Apostas Resolvidas (CSV)",
        data=csv_out.to_csv(index=False),
        file_name="apostas_reais_consolidado.csv",
        mime="text/csv",
        use_container_width=True,
    )


main()
