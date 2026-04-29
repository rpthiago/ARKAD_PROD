"""
Universo 97/96 — ARKAD_PROD
Padrão Ouro: 25 entradas de abril/2026, WR 96%, P&L +522
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from engine_ciclo_producao import (
    apply_config_filters,
    load_config,
    prepare_dataframe,
    _run_cycle_no_monitor,
)

st.set_page_config(page_title="Universo 97", page_icon="🏆", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
BENCH_DIR = ROOT_DIR / "Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97"
CSV_PATH   = BENCH_DIR / "universo_97_96_exec.csv"
CFG_PATH   = ROOT_DIR / "config_universo_97.json"
OPS_PATH   = BENCH_DIR / "universo_97_96_exec_ops.csv"
SUM_PATH   = BENCH_DIR / "universo_97_96_exec_summary.json"


# ── helpers ───────────────────────────────────────────────────────────────────

def _rodar() -> tuple[pd.DataFrame, dict]:
    cfg = load_config(CFG_PATH)
    df_raw = pd.read_csv(CSV_PATH)
    df_prep = prepare_dataframe(df_raw, cfg, environment="historico")
    df_prep = apply_config_filters(df_prep, cfg)
    ops, summary = _run_cycle_no_monitor(df_prep, cfg, environment="historico")
    return ops, summary


def _equity_chart(ops: pd.DataFrame) -> go.Figure:
    lucro = pd.to_numeric(ops["Lucro_Acumulado"], errors="coerce").fillna(0)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=lucro,
        mode="lines+markers",
        line=dict(color="#00c78c", width=2),
        marker=dict(size=5),
        name="P&L Acumulado",
        hovertemplate="Entrada %{x+1}<br>P&L: R$ %{y:,.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dot", line_color="gray", line_width=1)
    fig.update_layout(
        title="Equity Curve — Universo 97/96",
        xaxis_title="Entrada",
        yaxis_title="P&L Acumulado (R$)",
        height=380,
        margin=dict(l=0, r=0, t=40, b=0),
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font=dict(color="#fafafa"),
        xaxis=dict(gridcolor="#1e2228"),
        yaxis=dict(gridcolor="#1e2228"),
    )
    return fig


def _resultado_badge(r) -> str:
    try:
        v = int(float(r))
    except Exception:
        return str(r)
    return "🟢 GREEN" if v == 1 else "🔴 RED"


# ── estado ────────────────────────────────────────────────────────────────────

if "u97_ops" not in st.session_state:
    st.session_state.u97_ops = None
    st.session_state.u97_summary = None

# tenta carregar resultado já salvo do disco
if st.session_state.u97_ops is None and OPS_PATH.exists() and SUM_PATH.exists():
    st.session_state.u97_ops = pd.read_csv(OPS_PATH)
    with open(SUM_PATH, encoding="utf-8") as f:
        st.session_state.u97_summary = json.load(f)


# ── layout ────────────────────────────────────────────────────────────────────

st.title("🏆 Universo 97 / 96 — Padrão Ouro")
st.caption("Recorte canônico do commit `eb4e32e` · 25 entradas · Abril 2026")

col_run, _ = st.columns([1, 4])
with col_run:
    if st.button("▶ Rodar agora", type="primary", use_container_width=True):
        with st.spinner("Executando engine..."):
            try:
                ops, summary = _rodar()
                st.session_state.u97_ops = ops
                st.session_state.u97_summary = summary
                st.success("Executado com sucesso!")
            except Exception as e:
                st.error(f"Erro ao executar engine: {e}")
                st.stop()

ops: pd.DataFrame | None = st.session_state.u97_ops
summary: dict | None = st.session_state.u97_summary

if ops is None or summary is None:
    st.info("Clique em **▶ Rodar agora** para carregar os resultados.")
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────────

wr   = summary.get("Win_Rate_Executadas_%", 0)
lucro = summary.get("Lucro_Final", 0)
entr = summary.get("Entradas_Executadas", 0)
skip = summary.get("Entradas_Skipadas", 0)
dd   = summary.get("Max_Drawdown_Abs", 0)
dd_p = summary.get("Max_Drawdown_%", 0)
step = summary.get("Step_Ups", 0)
saques = summary.get("Saques_Realizados", 0)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Win Rate", f"{wr:.1f}%", delta="Padrão Ouro ✓" if wr >= 95 else None)
k2.metric("P&L Acumulado", f"R$ {lucro:,.2f}")
k3.metric("Entradas", f"{entr}", delta=f"{skip} skipped" if skip else None, delta_color="off")
k4.metric("Max Drawdown", f"R$ {dd:,.2f}", delta=f"{dd_p:.1f}%", delta_color="inverse")
k5.metric("Step-Ups / Saques", f"{step} / {saques}")

st.divider()

# ── Equity Curve ──────────────────────────────────────────────────────────────

st.plotly_chart(_equity_chart(ops), use_container_width=True)

st.divider()

# ── Tabela de operações ───────────────────────────────────────────────────────

st.subheader("Operações")

COLS_DISPLAY = [
    "Data_Arquivo", "Liga", "Jogo", "Metodo",
    "Odd_Base", "Status_Execucao", "1/0",
    "PnL_Linha", "Lucro_Acumulado",
]
cols_ok = [c for c in COLS_DISPLAY if c in ops.columns]
df_show = ops[cols_ok].copy()

# badge resultado
if "1/0" in df_show.columns:
    df_show["Resultado"] = df_show["1/0"].apply(_resultado_badge)
    df_show = df_show.drop(columns=["1/0"])

# formata P&L
for col in ("PnL_Linha", "Lucro_Acumulado"):
    if col in df_show.columns:
        df_show[col] = pd.to_numeric(df_show[col], errors="coerce").map(
            lambda v: f"R$ {v:+,.2f}" if pd.notna(v) else ""
        )

st.dataframe(df_show, use_container_width=True, height=600)

st.divider()

# ── Distribuição por liga ─────────────────────────────────────────────────────

if "Liga" in ops.columns and "1/0" in ops.columns:
    st.subheader("WR por Liga")
    result_col = "1/0"
    grp = (
        ops.groupby("Liga")[result_col]
        .agg(Entradas="count", Greens="sum")
        .reset_index()
    )
    grp["WR_%"] = (grp["Greens"] / grp["Entradas"] * 100).round(1)
    grp = grp.sort_values("WR_%", ascending=False)
    st.dataframe(grp, use_container_width=True, hide_index=True)

# ── Config resumida ───────────────────────────────────────────────────────────

with st.expander("Ver config operacional (config_universo_97.json)"):
    try:
        cfg_disp = load_config(CFG_PATH)
        filtros = cfg_disp.get("runtime_data", {}).get("filtros_metodo", {})
        rodos = cfg_disp.get("filtros_rodo", [])
        st.write("**Filtros de método:**")
        for metodo, f in filtros.items():
            ligas = f.get("ligas_permitidas", [])
            st.write(f"- `{metodo}`: odd {f.get('odd_min')}–{f.get('odd_max')} · {len(ligas)} ligas")
        st.write(f"**Rodos ativos:** {len(rodos)}")
        for r in rodos:
            st.write(f"  - {r.get('name', r.get('id'))}")
    except Exception as e:
        st.warning(f"Não foi possível carregar config: {e}")
