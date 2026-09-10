# -*- coding: utf-8 -*-
"""
main.py — ARKAD Central de Inteligência Desportiva & Dashboard Executivo
Exibe os resultados auditados de 2026, as estratégias ativas (Top 3 Menor Odd),
a simulação de alavancagem de banca e a auditoria forense de ligas.
"""
import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="ARKAD — Dashboard Principal",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🤖 ARKAD — Central de Inteligência Quantitativa")
st.caption("Estratégias de Exchange (Betfair) baseadas em Sweet Spots matemáticos, Odds Reais de Lay e Auditoria Forense.")

st.markdown("---")

# Métricas Principais no Topo
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="🎯 Lay 0x3 (Top 3 Menor Odd)",
        value="99,6% WR",
        delta="491 G / 2 R (Ano 2026)",
        help="Apenas 2 reds no ano de 2026 inteiro em 493 operações."
    )

with col2:
    st.metric(
        label="⚽ Lay 2x2 (Top 3 Menor Odd)",
        value="96,2% WR",
        delta="+R$ 26.500 (100u)",
        help="Taxa de acerto 96,2% vs 92,5% de Break-even (+3,7% de Edge puro sobre a Betfair)."
    )

with col3:
    st.metric(
        label="💼 Portfólio Combinado 2026",
        value="97,4% WR",
        delta="1.064 G / 28 R",
        help="1.092 jogos operados no ano com risco diversificado entre 0x3 e 2x2."
    )

with col4:
    st.metric(
        label="🚀 Alavancagem (Trava R$ 2k)",
        value="R$ 73.847,46",
        delta="+7.284% (de R$ 1.000)",
        help="Simulação começando com R$ 1.000, risco de 20% e responsabilidade travada em R$ 2.000."
    )

st.markdown("---")

# Abas de Navegação e Conteúdo
tab_gestao, tab_0x3, tab_2x2, tab_ligas = st.tabs([
    "📈 Simulação de Banca (R$ 1.000 → R$ 73.847)",
    "🎯 Lay 0x3 (Top 3)",
    "⚽ Lay 2x2 (Top 3)",
    "🛡️ Padrão de Ligas & Auditoria"
])

# ==============================================================================
# TAB 1: GESTÃO DE BANCA & ALAVANCAGEM
# ==============================================================================
with tab_gestao:
    st.subheader("📈 Simulação de Alavancagem com Trava de Liquidez (R$ 2.000 por jogo)")
    st.markdown("""
    * **Banca Inicial:** R$ 1.000,00
    * **Regra de Risco:** 20% de responsabilidade dinâmica por jogo.
    * **Trava de Segurança:** Ao atingir R$ 10.000 de banca (responsabilidade de R$ 2.000), a responsabilidade congela em **R$ 2.000,00 fixos por entrada** (respeitando a liquidez máxima recomendada da Betfair).
    """)
    
    dados_mensais = [
        {"Mês": "Jan/2026", "Jogos": 161, "Greens": 158, "Reds": 3, "Lucro Mês": "R$ +4.209,19", "Saldo Final": "R$ 5.209,19", "Retorno Mês": "+420,9%"},
        {"Mês": "Fev/2026", "Jogos": 168, "Greens": 167, "Reds": 1, "Lucro Mês": "R$ +23.971,71", "Saldo Final": "R$ 29.180,90", "Retorno Mês": "+460,2% (Bateu Trava 2k)"},
        {"Mês": "Mar/2026", "Jogos": 183, "Greens": 181, "Reds": 2, "Lucro Mês": "R$ +24.582,47", "Saldo Final": "R$ 53.763,37", "Retorno Mês": "+84,2%"},
        {"Mês": "Abr/2026", "Jogos": 170, "Greens": 161, "Reds": 9, "Lucro Mês": "R$ +6.879,38", "Saldo Final": "R$ 60.642,75", "Retorno Mês": "+12,8% (9 reds no 2x2)"},
        {"Mês": "Mai/2026", "Jogos": 145, "Greens": 142, "Reds": 3, "Lucro Mês": "R$ +8.111,05", "Saldo Final": "R$ 68.753,81", "Retorno Mês": "+13,4%"},
        {"Mês": "Jun/2026", "Jogos": 42, "Greens": 39, "Reds": 3, "Lucro Mês": "R$ -2.237,17", "Saldo Final": "R$ 66.516,64", "Retorno Mês": "-3,3%"},
        {"Mês": "Jul/2026", "Jogos": 97, "Greens": 94, "Reds": 3, "Lucro Mês": "R$ +3.179,59", "Saldo Final": "R$ 69.696,22", "Retorno Mês": "+4,8%"},
        {"Mês": "Ago/2026", "Jogos": 100, "Greens": 96, "Reds": 4, "Lucro Mês": "R$ +1.575,19", "Saldo Final": "R$ 71.271,42", "Retorno Mês": "+2,3%"},
        {"Mês": "Set/2026", "Jogos": 26, "Greens": 26, "Reds": 0, "Lucro Mês": "R$ +2.576,04", "Saldo Final": "R$ 73.847,46", "Retorno Mês": "+3,6%"},
    ]
    df_mensal = pd.DataFrame(dados_mensais)
    st.dataframe(df_mensal, use_container_width=True, hide_index=True)
    
    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.info("""
        **🛡️ Como a Trava Salvou a Banca em Abril:**
        Em abril, o Lay 2x2 sofreu 9 reds. Sem a trava, a responsabilidade estaria em R$ 10.000+ e causaria um rombo severo. Com a responsabilidade limitada em R$ 2.000, o Lay 0x3 (87 Greens e 0 Reds) pagou todas as contas e o mês fechou no positivo (+R$ 6.879)!
        """)
    with col_c2:
        st.success("""
        **🎯 Desaceleração Natural do Risco:**
        * Em **07/02**, R$ 2.000 representava **20%** da banca.
        * Em **Setembro**, os mesmos R$ 2.000 representavam apenas **2,7%** da banca!
        * O risco de ruína foi caindo para perto de zero conforme a banca acumulava gordura.
        """)

# ==============================================================================
# TAB 2: LAY 0X3
# ==============================================================================
with tab_0x3:
    st.subheader("🎯 Lay 0x3 (Top 3 Menor Odd Diária)")
    st.markdown("""
    * **Filtro Oficial:** Odd Lay 0x3 entre 14.0 e 35.0 | Odd Under 2.5 Back <= 2.10 | Odd Visitante Back >= 1.85.
    * **Critério de Seleção:** Exibir estritamente os **3 jogos com a menor odd de Lay** do dia.
    """)
    
    dados_0x3 = [
        {"Mês": "Jan/2026", "Jogos": 75, "Greens": 75, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "94.9%", "Odd Média": 19.77, "Lucro (100u)": "R$ +7.125,00"},
        {"Mês": "Fev/2026", "Jogos": 84, "Greens": 84, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "94.4%", "Odd Média": 17.85, "Lucro (100u)": "R$ +7.980,00"},
        {"Mês": "Mar/2026", "Jogos": 93, "Greens": 93, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "94.4%", "Odd Média": 17.75, "Lucro (100u)": "R$ +8.835,00"},
        {"Mês": "Abr/2026", "Jogos": 87, "Greens": 87, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "94.7%", "Odd Média": 18.94, "Lucro (100u)": "R$ +8.265,00"},
        {"Mês": "Mai/2026", "Jogos": 59, "Greens": 58, "Reds": 1, "Taxa Acerto": "98.3%", "BE Médio": "96.3%", "Odd Média": 26.25, "Lucro (100u)": "R$ +3.510,00"},
        {"Mês": "Jun/2026", "Jogos": 12, "Greens": 12, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "96.7%", "Odd Média": 29.12, "Lucro (100u)": "R$ +1.140,00"},
        {"Mês": "Jul/2026", "Jogos": 33, "Greens": 32, "Reds": 1, "Taxa Acerto": "97.0%", "BE Médio": "96.6%", "Odd Média": 28.09, "Lucro (100u)": "R$ +140,00"},
        {"Mês": "Ago/2026", "Jogos": 39, "Greens": 39, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "96.3%", "Odd Média": 26.09, "Lucro (100u)": "R$ +3.705,00"},
        {"Mês": "Set/2026", "Jogos": 11, "Greens": 11, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "96.4%", "Odd Média": 26.91, "Lucro (100u)": "R$ +1.045,00"},
        {"Mês": "TOTAL 2026", "Jogos": 493, "Greens": 491, "Reds": 2, "Taxa Acerto": "99.6%", "BE Médio": "95.2%", "Odd Média": 21.13, "Lucro (100u)": "R$ +41.745,00"}
    ]
    st.dataframe(pd.DataFrame(dados_0x3), use_container_width=True, hide_index=True)
    st.caption("Acesse a página lateral **'16 ⚽ Sinais Lay 0x3'** para visualizar as entradas de hoje.")

# ==============================================================================
# TAB 3: LAY 2X2
# ==============================================================================
with tab_2x2:
    st.subheader("⚽ Lay 2x2 (Top 3 Menor Odd Diária)")
    st.markdown("""
    * **Filtro Oficial:** Odd Lay 2x2 entre 8.0 e 20.0 | (Under 2.5 FT Back <= 2.00 OU Mandante Back <= 1.55 OU Visitante Back <= 1.60).
    * **Critério de Seleção:** Exibir estritamente os **3 jogos com a menor odd de Lay** do dia.
    """)
    
    dados_2x2 = [
        {"Mês": "Jan/2026", "Jogos": 86, "Greens": 83, "Reds": 3, "Taxa Acerto": "96.5%", "BE Médio": "91.7%", "Lucro (100u)": "R$ +4.015,00"},
        {"Mês": "Fev/2026", "Jogos": 84, "Greens": 83, "Reds": 1, "Taxa Acerto": "98.8%", "BE Médio": "90.3%", "Lucro (100u)": "R$ +7.145,00"},
        {"Mês": "Mar/2026", "Jogos": 90, "Greens": 87, "Reds": 3, "Taxa Acerto": "96.7%", "BE Médio": "90.9%", "Lucro (100u)": "R$ +4.875,00"},
        {"Mês": "Abr/2026", "Jogos": 83, "Greens": 74, "Reds": 9, "Taxa Acerto": "89.2%", "BE Médio": "90.7%", "Lucro (100u)": "R$ -1.040,00"},
        {"Mês": "Mai/2026", "Jogos": 86, "Greens": 84, "Reds": 2, "Taxa Acerto": "97.7%", "BE Médio": "94.6%", "Lucro (100u)": "R$ +4.730,00"},
        {"Mês": "Jun/2026", "Jogos": 30, "Greens": 28, "Reds": 2, "Taxa Acerto": "93.3%", "BE Médio": "94.9%", "Lucro (100u)": "R$ -940,00"},
        {"Mês": "Jul/2026", "Jogos": 64, "Greens": 62, "Reds": 2, "Taxa Acerto": "96.9%", "BE Médio": "94.7%", "Lucro (100u)": "R$ +2.540,00"},
        {"Mês": "Ago/2026", "Jogos": 61, "Greens": 57, "Reds": 4, "Taxa Acerto": "93.4%", "BE Médio": "94.6%", "Lucro (100u)": "R$ -1.085,00"},
        {"Mês": "Set/2026", "Jogos": 15, "Greens": 15, "Reds": 0, "Taxa Acerto": "100.0%", "BE Médio": "94.5%", "Lucro (100u)": "R$ +1.425,00"},
        {"Mês": "TOTAL 2026", "Jogos": 599, "Greens": 576, "Reds": 23, "Taxa Acerto": "96.2%", "BE Médio": "92.5%", "Lucro (100u)": "R$ +26.500,00"}
    ]
    st.dataframe(pd.DataFrame(dados_2x2), use_container_width=True, hide_index=True)
    st.caption("Acesse a página lateral **'17 ⚽ Sinais Lay 2x2'** para visualizar as entradas de hoje.")

# ==============================================================================
# TAB 4: AUDITORIA DE LIGAS
# ==============================================================================
with tab_ligas:
    st.subheader("🛡️ Auditoria Forense dos Reds (Portfólio Completo - 2.636 jogos)")
    st.markdown("""
    Auditamos todos os 140 Reds do Lay 2x2 em 2026. Duas conclusões essenciais:
    1. **Ligas Periféricas/Discrepantes Concentram Prejuízo:** Países com grande disparidade entre 2 times ricos e o resto (Sérvia, Irlanda, Turquia, Escócia) têm taxa de 2x2 muito acima do normal.
    2. **Ligas de Elite são Muralhas:** França, Argentina, Inglaterra 2, Espanha 2 e Itália tiveram quase zero reds.
    """)
    
    col_l1, col_l2 = st.columns(2)
    
    with col_l1:
        st.error("🚨 **Ligas com Maior Taxa de Red (Prejuízo Matemático):**")
        df_piores = pd.DataFrame([
            {"Liga": "Sérvia 1", "Jogos": 24, "Reds": 4, "Taxa Red": "16,7%", "Edge": "-10,1%"},
            {"Liga": "Irlanda 1", "Jogos": 28, "Reds": 4, "Taxa Red": "14,3%", "Edge": "-8,0%"},
            {"Liga": "Inglaterra Copas", "Jogos": 21, "Reds": 3, "Taxa Red": "14,3%", "Edge": "-5,8%"},
            {"Liga": "Portugal 1", "Jogos": 46, "Reds": 6, "Taxa Red": "13,0%", "Edge": "-5,9%"},
            {"Liga": "Brasil 1 (Série A)", "Jogos": 67, "Reds": 7, "Taxa Red": "10,4%", "Edge": "-4,7%"},
            {"Liga": "Escócia 1", "Jogos": 32, "Reds": 3, "Taxa Red": "9,4%", "Edge": "-2,3%"},
        ])
        st.dataframe(df_piores, use_container_width=True, hide_index=True)
        
    with col_l2:
        st.success("🏆 **Ligas Mais Seguras (Muralhas de Green):**")
        df_melhores = pd.DataFrame([
            {"Liga": "França 1 (Ligue 1)", "Jogos": 32, "Reds": 0, "Taxa Red": "0,0%", "Edge": "+6,7%"},
            {"Liga": "Argentina 1", "Jogos": 73, "Reds": 1, "Taxa Red": "1,4%", "Edge": "+5,3%"},
            {"Liga": "Inglaterra 2 (Championship)", "Jogos": 60, "Reds": 1, "Taxa Red": "1,7%", "Edge": "+4,5%"},
            {"Liga": "Espanha 2 (La Liga 2)", "Jogos": 56, "Reds": 1, "Taxa Red": "1,8%", "Edge": "+3,9%"},
            {"Liga": "Itália 1 (Serie A)", "Jogos": 75, "Reds": 2, "Taxa Red": "2,7%", "Edge": "+4,7%"},
            {"Liga": "Itália 2 (Serie B)", "Jogos": 44, "Reds": 1, "Taxa Red": "2,3%", "Edge": "+4,5%"},
        ])
        st.dataframe(df_melhores, use_container_width=True, hide_index=True)

st.markdown("---")
st.info("👈 **Para visualizar as entradas geradas para hoje, selecione '16 ⚽ Sinais Lay 0x3' ou '17 ⚽ Sinais Lay 2x2' no menu lateral.**")

