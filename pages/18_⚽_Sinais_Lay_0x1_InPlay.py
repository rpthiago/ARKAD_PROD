# -*- coding: utf-8 -*-
"""
18_⚽_Sinais_Lay_0x1_InPlay.py — Monitor In-Play Quantitativo Lay 0x1 (Minuto 55-75)
==================================================================================
ARKAD — Plataforma Quantitativa de Apostas Esportivas
GOVERNANÇA: EM OBSERVAÇÃO FORWARD (Stake-Zero) — Lei 10 do GEMINI.md.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from estrategia_lay_0x1_inplay import (
    validar_entrada_lay0x1_inplay,
    calcular_break_even,
    MINUTO_INPLAY_MIN,
    MINUTO_INPLAY_MAX,
    ODD_LAY_0X1_MIN,
    ODD_LAY_0X1_MAX
)
import tracker_lay0x1_inplay as tracker

st.set_page_config(
    page_title="Sinais Lay 0x1 In-Play — ARKAD",
    page_icon="⚽",
    layout="wide"
)

# ── Cabeçalho & Governança ──
st.title("⚽ Sinais Lay 0x1 In-Play (Minuto 55'–75')")
st.warning("""
⚠️ **STATUS DE GOVERNANÇA (10/09/2026): EM OBSERVAÇÃO FORWARD IN-PLAY (Stake-Zero)**
* **Tese Operacional (Rota C):** No pré-jogo, o Lay 0x1 exige odds altas (15 a 35) e o mercado é hiper-eficiente (Rotas A e B reprovadas em 37k partidas). No In-Play tardio (minuto 55' a 75' em 0x0), a odd de Lay 0x1 cai para a faixa de **2.00 a 5.50**, reduzindo o liability para apenas 1.0x a 4.5x a stake.
* **Gatilho de Vitória:** GREEN se o mandante marcar (1x0, 2x0, 2x1), se o jogo terminar 0x0 ou se o visitante fizer 2+ gols. O único placar perdedor (**RED**) é o 0x1 FT exato.
* **Regra de Ouro (GEMINI.md):** Operação estritamente em **Stake Zero (`R$ 0,00`)** até acumular $N \ge 200$ entradas e validar o gap real contra a odd executável da Betfair.
""")

# ── KPIs de Risco no Topo ──
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Janela In-Play", f"{MINUTO_INPLAY_MIN}' a {MINUTO_INPLAY_MAX}'", "Placar Atual: 0x0")
with k2:
    st.metric("Faixa de Odd Alvo", f"{ODD_LAY_0X1_MIN:.1f} a {ODD_LAY_0X1_MAX:.1f}", "Liability Baixo")
with k3:
    st.metric("Break-Even Médio", "66% a 78%", "vs 93%+ do Pré-Jogo")
with k4:
    st.metric("Status Operacional", "Stake-Zero 🔬", "Observação Ativa")

st.divider()

# ── Abas da Página ──
tab1, tab2 = st.tabs(["📡 Radar In-Play Ao Vivo", "📊 Paper Trading & Auditoria Científica"])

with tab1:
    st.subheader("📡 Radar de Oportunidades Ao Vivo")
    st.markdown(
        f"Varredura em tempo real dos jogos na faixa de **{MINUTO_INPLAY_MIN}' a {MINUTO_INPLAY_MAX}'** com odd de Lay 0x1 entre **{ODD_LAY_0X1_MIN:.2f} e {ODD_LAY_0X1_MAX:.2f}**."
    )
    
    col_btn, col_info = st.columns([1, 4])
    with col_btn:
        executar_scan = st.button("🔄 Atualizar Radar In-Play", use_container_width=True)
        
    if executar_scan:
        with st.spinner("Varrendo telemetria ao vivo da Betfair..."):
            novos = tracker.escanear_jogos_inplay(verbose=False)
            tracker.liquidar_paper_trading(verbose=False)
            if novos:
                st.success(f"🎯 {len(novos)} nova(s) oportunidade(s) detectada(s) e registrada(s) no log de papel!")
            else:
                st.info("Nenhuma partida atendeu a todos os critérios estritos no momento exato da varredura.")

    # Exibir partidas capturadas recentemente
    if tracker.LOG_CSV.exists():
        df_log_recent = pd.read_csv(tracker.LOG_CSV)
        if not df_log_recent.empty:
            df_recent = df_log_recent.tail(10).iloc[::-1]
            st.markdown("##### 🕒 Últimos Sinais Detectados pelo Rastreador:")
            
            # Formatando para exibição limpa
            cols_show = ["Timestamp", "Jogo", "Minuto", "Placar_Entrada", "Odd_CS_0x1_Lay", "Break_Even_WR", "Status", "Placar_FT", "Resultado"]
            cols_exist = [c for c in cols_show if c in df_recent.columns]
            st.dataframe(df_recent[cols_exist], use_container_width=True, hide_index=True)
        else:
            st.info("O log de telemetria in-play está aguardando as primeiras partidas ao vivo do dia.")
    else:
        st.info("Inicie o rastreador para gerar os primeiros registros de paper trading.")

with tab2:
    st.subheader("📊 Histórico de Validação Forward & Métricas")
    
    if tracker.LOG_CSV.exists():
        df_paper = pd.read_csv(tracker.LOG_CSV)
        if not df_paper.empty:
            n_total = len(df_paper)
            df_liq = df_paper[df_paper["Resultado"].isin(["GREEN", "RED"])].copy()
            n_liq = len(df_liq)
            n_pend = n_total - n_liq
            
            p1, p2, p3, p4 = st.columns(4)
            with p1:
                st.metric("Total Sinais Observados", n_total, f"{n_pend} Pendentes")
            with p2:
                if n_liq > 0:
                    greens = (df_liq["Resultado"] == "GREEN").sum()
                    wr = (greens / n_liq) * 100
                    st.metric("Win Rate Real", f"{wr:.1f}%", f"{greens} Greens")
                else:
                    st.metric("Win Rate Real", "N/A", "Aguardando liquidação")
            with p3:
                if n_liq > 0:
                    be_med = df_liq["Break_Even_WR"].mean()
                    gap = wr - be_med
                    st.metric("Break-Even Médio", f"{be_med:.1f}%", f"Gap: {gap:+.1f} pp")
                else:
                    st.metric("Break-Even Médio", "N/A", "Aguardando")
            with p4:
                if n_liq > 0:
                    pnl_total = df_liq["PnL_u"].sum()
                    st.metric("Lucro Acumulado (u)", f"{pnl_total:+.2f} u", "Stake 1.0u")
                else:
                    st.metric("Lucro Acumulado (u)", "0.00 u", "Paper Trading")
                    
            st.markdown("---")
            st.markdown("##### 📋 Ledger Completo de Observações (Stake-Zero):")
            st.dataframe(df_paper, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum registro encontrado no arquivo de paper trading.")
    else:
        st.info("Arquivo de log ainda não criado. O arquivo será inicializado no primeiro sinal.")

    st.markdown("---")
    st.markdown("### 🔬 Por que a Rota C substitui as Rotas A e B?")
    st.markdown("""
    1. **Rotas A e B (Pré-Jogo) Reprovadas Cientificamente:**
       - O scan em 37.442 partidas da Betfair provou que o mercado precifica o 0x1 com precisão matemática implacável.
       - A odd média de 25 a 35 em super favoritos gera um liability de 24x a 34x a stake, onde 1 único gol tardio destrói dezenas de greens.
    2. **A Vantagem da Rota C (In-Play):**
       - Ao esperar o jogo atingir os 55' em 0x0, a incerteza do tempo restante comprime a odd de Lay para **2.00 a 5.50**.
       - O liability cai para 1x a 4.5x a stake, permitindo uma gestão de risco sustentável.
       - O método aproveita a pressão e volume ofensivo do mandante para evitar que a zebra visitante vença com um gol solitário no final.
    """)
