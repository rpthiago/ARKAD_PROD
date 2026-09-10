# -*- coding: utf-8 -*-
"""
06_⚡_Radar_Steam_Moves.py — Radar de Movimentação de Odds e Fluxo Institucional Betfair (Linha 3 ARKAD)

Monitora em tempo real a injeção de volume e quedas de odds (Steam Moves)
provocadas por apostadores institucionais e sindicatos profissionais europeus.
"""

import sys
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from steam_tracker import capturar_snapshot, detectar_steam_moves, enviar_alertas_telegram, STEAM_DIR

st.set_page_config(
    page_title="ARKAD — Radar de Steam Moves & Sharp Money",
    page_icon="⚡",
    layout="wide"
)

# ── Header ──
st.title("⚡ Radar de Steam Moves & Sharp Money (Betfair Exchange)")

st.info("""
🔍 **COMO FUNCIONA O RASTREAMENTO PROFISSIONAL (LINHA 3 ARKAD):**
* **O Fenômeno Steam Move:** Sindicatos profissionais europeus e fundos quantitativos operam no fechamento das linhas. Quando escalações, desfalques ou modelos internos sinalizam vantagem desproporcional, eles despejam centenas de milhares de euros no mercado, causando uma **queda rápida e violenta na cotação** (*steam*).
* **Closing Line Value (CLV):** Entrar a favor do fluxo institucional antes do encerramento garante odds superiores à linha de fechamento, gerando valor esperado positivo a longo prazo.
* **Governança:** Este radar atua como ferramenta analítica e de geração de alertas em tempo real.
""")

# ── Sidebar Controls ──
st.sidebar.header("⚙️ Controles do Radar")

# Seletor de Data
hoje_str = datetime.now().strftime("%Y-%m-%d")
data_selecionada = st.sidebar.date_input("Data de Monitoramento", value=datetime.now())
data_str = data_selecionada.strftime("%Y-%m-%d")

# Botão de captura sob demanda
st.sidebar.markdown("---")
st.sidebar.subheader("📡 Captura em Tempo Real")
if st.sidebar.button("🔄 Capturar Novo Snapshot Agora", use_container_width=True):
    with st.spinner("Consultando API Betfair e gravando novo snapshot..."):
        df_new = capturar_snapshot(data_str)
        if not df_new.empty:
            st.sidebar.success(f"Snapshot capturado com {len(df_new)} jogos!")
        else:
            st.sidebar.warning("Nenhum dado retornado para esta data.")

if st.sidebar.button("📲 Disparar Alertas no Telegram", use_container_width=True):
    with st.spinner("Analisando e disparando alertas no Telegram..."):
        df_moves_temp = detectar_steam_moves(data_str, min_drop_pct=5.0)
        if not df_moves_temp.empty:
            n_sent = enviar_alertas_telegram(df_moves_temp, min_drop_pct=8.0)
            if n_sent > 0:
                st.sidebar.success(f"🚀 {n_sent} novos alertas enviados ao Telegram!")
            else:
                st.sidebar.info("Nenhum novo Steam Move pendente de envio.")
        else:
            st.sidebar.warning("Sem movimentações registradas.")

# Filtros analíticos
st.sidebar.markdown("---")
st.sidebar.subheader("🎯 Filtros de Sensibilidade")
min_drop = st.sidebar.slider("Queda Mínima de Odd (Drop %)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)

mercados_filtro = st.sidebar.multiselect(
    "Mercados",
    options=["Home", "Away", "Draw", "Over25"],
    default=["Home", "Away", "Draw", "Over25"]
)

# ── Carregar Dados ──
clean_date = data_str.replace("-", "")
snap_file = STEAM_DIR / f"snapshots_{clean_date}.csv"

if not snap_file.exists():
    st.warning(f"⚠️ Nenhum snapshot gravado ainda para **{data_str}**.")
    st.markdown("Clique no botão **'🔄 Capturar Novo Snapshot Agora'** na barra lateral para iniciar o rastreamento do dia.")
    st.stop()

# Ler dados
df_moves = detectar_steam_moves(data_str, min_drop_pct=min_drop)

if df_moves.empty:
    st.info(f"Ainda não há variações registradas para {data_str}. São necessários pelo menos 2 snapshots ao longo do dia para medir movimentações de linha.")
    st.stop()

# Aplicar filtros
if mercados_filtro:
    df_moves = df_moves[df_moves["Mercado"].isin(mercados_filtro)]

# Identificar categorias
mega_steams = df_moves[df_moves["Drop_Pct"] >= 10.0]
mod_steams = df_moves[(df_moves["Drop_Pct"] >= min_drop) & (df_moves["Drop_Pct"] < 10.0)]
drifts = df_moves[df_moves["Drop_Pct"] <= -8.0]

# ── KPIs ──
col1, col2, col3, col4, col5 = st.columns(5)
total_jogos = df_moves["Home"].nunique()
num_snaps = df_moves["Snapshots_Qtd"].max() if "Snapshots_Qtd" in df_moves.columns else 1

col1.metric("Jogos Rastreados", f"{total_jogos}")
col2.metric("Snapshots no Dia", f"{num_snaps}")
col3.metric("🔴 Mega Steams (≥10%)", f"{len(mega_steams)}")
col4.metric("🟡 Steams Moderados", f"{len(mod_steams)}")
col5.metric("🔵 Drifts Fortes (Rejeição)", f"{len(drifts)}")

st.markdown("---")

# ── Seção 1: Alertas Críticos de Sharp Money ──
st.subheader("🔥 Alertas de Movimentação Institucional (Steam Moves)")

df_view = df_moves[df_moves["Drop_Pct"] >= min_drop].copy()

if df_view.empty:
    st.write(f"Nenhum mercado apresentou queda $\ge {min_drop}\%$ até o momento.")
else:
    # Formatação amigável
    col_order = [
        "Time", "League", "Home", "Away", "Mercado",
        "Odd_Abertura", "Odd_Atual", "Delta_Odd", "Drop_Pct", "Intensidade",
        "Primeira_Captura", "Ultima_Captura"
    ]
    df_show = df_view[[c for c in col_order if c in df_view.columns]].copy()
    
    st.dataframe(
        df_show.style.format({
            "Odd_Abertura": "{:.2f}",
            "Odd_Atual": "{:.2f}",
            "Delta_Odd": "{:+.2f}",
            "Drop_Pct": "{:+.1f}%"
        }).background_gradient(subset=["Drop_Pct"], cmap="YlOrRd"),
        use_container_width=True,
        hide_index=True
    )

# ── Seção 2: Evolução Temporal por Partida ──
st.markdown("---")
st.subheader("📈 Análise de Trajetória da Odd por Partida")

jogos_com_steam = sorted(df_moves["Home"].unique())
jogo_selecionado = st.selectbox("Selecione uma partida para ver a evolução temporal:", jogos_com_steam)

if jogo_selecionado:
    df_raw_snaps = pd.read_csv(snap_file)
    df_raw_snaps["captured_at"] = pd.to_datetime(df_raw_snaps["captured_at"])
    df_match = df_raw_snaps[df_raw_snaps["Home"] == jogo_selecionado].sort_values("captured_at")
    
    if len(df_match) >= 1:
        st.write(f"**Confronto:** {df_match.iloc[0]['Home']} vs {df_match.iloc[0]['Away']} | **Liga:** {df_match.iloc[0].get('League', 'N/A')} | **Horário:** {df_match.iloc[0].get('Time', 'N/A')}")
        
        # Preparar dados para gráfico
        chart_cols = [c for c in ["Odd_H_Back", "Odd_D_Back", "Odd_A_Back", "Odd_Over25_FT_Back"] if c in df_match.columns]
        chart_df = df_match.set_index("captured_at")[chart_cols]
        chart_df.columns = [c.replace("Odd_", "").replace("_Back", "") for c in chart_df.columns]
        
        st.line_chart(chart_df)
        
        # Tabela de histórico de capturas
        with st.expander("Ver histórico tabular de capturas deste jogo"):
            st.dataframe(df_match[["captured_at"] + chart_cols], use_container_width=True)

# ── Seção 3: Drifts (Onde o mercado está fugindo) ──
st.markdown("---")
st.subheader("🔵 Drifts Relevantes (Odds em Alta / Mercado Rejeitando)")
if not drifts.empty:
    st.dataframe(
        drifts[["Time", "League", "Home", "Away", "Mercado", "Odd_Abertura", "Odd_Atual", "Delta_Odd", "Drop_Pct", "Intensidade"]].style.format({
            "Odd_Abertura": "{:.2f}",
            "Odd_Atual": "{:.2f}",
            "Delta_Odd": "{:+.2f}",
            "Drop_Pct": "{:+.1f}%"
        }),
        use_container_width=True,
        hide_index=True
    )
else:
    st.write("Nenhum drift relevante registrado hoje.")

# ── Download ──
csv_data = df_moves.to_csv(index=False).encode("utf-8")
st.download_button(
    label="📥 Baixar Dados de Movimentação em CSV",
    data=csv_data,
    file_name=f"steam_moves_{data_str}.csv",
    mime="text/csv"
)
