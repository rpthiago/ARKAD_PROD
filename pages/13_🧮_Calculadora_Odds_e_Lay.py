import os
import sys
import io
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="Calculadora de Odds, LAY & BACK",
    page_icon="🧮",
    layout="wide"
)

st.title("🧮 Calculadora Interativa de Odds, LAY & BACK")
st.caption("Ferramenta de Precisão Matemática para Simulação de Lucro, Responsabilidade, Alavancagem e Break-Even na Betfair Exchange e Casas Tradicionais.")

st.markdown("""
Esta calculadora permite simular entradas em **LAY (Betfair Exchange)** e **BACK (Punter)** com total precisão matemática.
""")

tab_lay, tab_back, tab_tabela = st.tabs([
    "🔴 Calculadora LAY (Betfair Exchange)",
    "🔵 Calculadora BACK (Apostar A Favor)",
    "📊 Tabela de Referência Rápida (Lay 1.01 a 2.00)"
])

# -----------------------------------------------------------------------------
# TAB 1: CALCULADORA LAY
# -----------------------------------------------------------------------------
with tab_lay:
    st.subheader("🔴 Simulação de Entrada em LAY (Apostar CONTRA)")
    st.caption("No LAY, você assume o papel da Casa de Apostas contra quem apostou no BACK.")

    col1, col2, col3 = st.columns([1.5, 1.5, 1.5])
    
    with col1:
        odd_lay = st.number_input("Odd Lay (Betfair Exchange)", min_value=1.01, max_value=100.0, value=1.20, step=0.01, format="%.2f", key="lay_odd_input")
        comissao = st.number_input("Comissão Betfair (%)", min_value=0.0, max_value=10.0, value=5.0, step=0.5, format="%.1f", key="lay_comm_input")

    with col2:
        modo_calculo = st.radio(
            "Definir Valor por:",
            options=["Responsabilidade Fixa (Risco Máximo)", "Stake (Lucro Alvo Bruto)"],
            index=0,
            key="lay_mode_input"
        )
        valor_input = st.number_input("Valor em R$", min_value=1.0, max_value=100000.0, value=5.0, step=1.0, format="%.2f", key="lay_val_input")

    comm_frac = comissao / 100.0

    if modo_calculo == "Responsabilidade Fixa (Risco Máximo)":
        responsa = valor_input
        stake_back = responsa / (odd_lay - 1.0)
        lucro_bruto = stake_back
        lucro_liquido = lucro_bruto * (1.0 - comm_frac)
        retorno_pct = (lucro_liquido / responsa) * 100.0 if responsa > 0 else 0.0
        multiplicador = lucro_liquido / responsa if responsa > 0 else 0.0
    else:
        stake_back = valor_input
        responsa = stake_back * (odd_lay - 1.0)
        lucro_bruto = stake_back
        lucro_liquido = lucro_bruto * (1.0 - comm_frac)
        retorno_pct = (lucro_liquido / responsa) * 100.0 if responsa > 0 else 0.0
        multiplicador = lucro_liquido / responsa if responsa > 0 else 0.0

    be_win_rate = ((odd_lay - 1.0) / ((odd_lay - 1.0) + (1.0 - comm_frac))) * 100.0

    st.divider()

    st.markdown("### 📊 Resultado da Simulação LAY:")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🔴 Risco Máximo (Responsabilidade)", f"R$ {responsa:,.2f}", help="Valor máximo que sai da sua conta se o favorito vencer (RED).")
    m2.metric("🟢 Lucro Líquido no Bolso (GREEN)", f"R$ {lucro_liquido:,.2f}", help="Valor líquido creditado na sua conta se a zebra der GREEN.")
    m3.metric("🚀 Retorno / Multiplicador", f"{multiplicador:.2f}x ({retorno_pct:+,.1f}%)", help="Vezes o seu dinheiro investido de risco.")
    m4.metric("🎯 Break-Even (Acerto Mínimo)", f"{be_win_rate:.2f}%", help="Win Rate necessário para não ter prejuízo no longo prazo.")

    st.info(f"""
    💡 **Entendendo a operação na Betfair Exchange:**
    * **Se der GREEN:** Entram **R$ {lucro_liquido:,.2f} líquidos** na sua conta (Retorno de **{multiplicador:.2f}x** o seu risco).
    * **Se der RED:** Você perde apenas os **R$ {responsa:,.2f}** da Responsabilidade travada.
    """)

# -----------------------------------------------------------------------------
# TAB 2: CALCULADORA BACK
# -----------------------------------------------------------------------------
with tab_back:
    st.subheader("🔵 Simulação de Entrada em BACK (Apostar A Favor)")
    st.caption("No BACK, você aposta que o evento VAI acontecer (Punter tradicional).")

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        odd_back = st.number_input("Odd Back", min_value=1.01, max_value=100.0, value=1.20, step=0.01, format="%.2f", key="back_odd_input")
    with col_b2:
        stake_back_input = st.number_input("Stake Apostada (R$)", min_value=1.0, max_value=100000.0, value=100.0, step=10.0, format="%.2f", key="back_stake_input")

    retorno_bruto_back = stake_back_input * odd_back
    lucro_back = stake_back_input * (odd_back - 1.0)
    retorno_pct_back = ((odd_back - 1.0)) * 100.0

    st.divider()
    
    st.markdown("### 📊 Resultado da Simulação BACK:")
    mb1, mb2, mb3 = st.columns(3)
    mb1.metric("🔵 Stake Investida", f"R$ {stake_back_input:,.2f}")
    mb2.metric("🟢 Lucro Líquido (GREEN)", f"R$ {lucro_back:,.2f}")
    mb3.metric("📈 Retorno em % sobre a Stake", f"+{retorno_pct_back:.2f}%")

# -----------------------------------------------------------------------------
# TAB 3: TABELA DE REFERÊNCIA
# -----------------------------------------------------------------------------
with tab_tabela:
    st.subheader("📊 Tabela Comparativa LAY (Responsabilidade Fixa R$ 100)")
    
    odds_ref = [1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.10, 1.12, 1.15, 1.18, 1.20, 1.30, 1.50, 2.00]
    ref_rows = []
    
    for o in odds_ref:
        res = 100.0
        stk = res / (o - 1.0)
        luc_l = stk * 0.95
        ret_p = (luc_l / res) * 100.0
        be_w = (o - 1.0) / ((o - 1.0) + 0.95) * 100.0
        ref_rows.append({
            "Odd Lay": f"{o:.2f}",
            "Risco (Responsabilidade)": f"R$ {res:.2f}",
            "Stake Back Equivalente": f"R$ {stk:,.2f}",
            "Lucro Líquido no GREEN": f"R$ {luc_l:,.2f}",
            "Retorno em %": f"+{ret_p:,.2f}%",
            "Break-Even Win Rate": f"{be_w:.2f}%"
        })

    df_ref = pd.DataFrame(ref_rows)
    st.dataframe(df_ref, use_container_width=True)
