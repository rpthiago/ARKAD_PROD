import os
import sys
import io
import time
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="Sinais Lay 0x2 - Ao Vivo",
    page_icon="🎯",
    layout="wide",
)

import traceback
import importlib
try:
    import coleta_lay_cs_aovivo
    importlib.reload(coleta_lay_cs_aovivo)
    import b365_data_utils
except Exception as e:
    st.error("Erro ao carregar os módulos locais do Lay 0x2:")
    st.code(traceback.format_exc())
    st.stop()

st.title("🎯 Sinais Lay 0x2 (Random Forest v2)")
st.markdown("""
Esta página bate na **API da Betfair em tempo real**, calcula as inteligências do motor **Random Forest (RF)**, e aplica os **filtros estritos e realistas** validados no nosso backtest de longo prazo (2025-2026):

*   **🏆 Limite de Favoritismo Casa:** Odd Pré-live do Mandante (Odd Casa) **<= 3.00** (para evitar jogar contra zebras severas fora de casa).
*   **⚠️ Blacklist de Ligas:** Bloqueia automaticamente ligas estruturalmente perdedoras (Ex: Leste Europeu e campeonatos de alta disparidade).
*   **📈 Filtro de Valor (EV):** EV Mínimo $\ge 0.02$ calculado sobre probabilidade do Random Forest e odds live.

> ⚠️ **IMPORTANTE (FULL MATCH):** A estratégia opera em **Full Match** (deixando a operação correr até o final do jogo). Não faça Cash Out aos 60 minutos. O robô só toma Red se o placar final for exatamente 0x2.
""")

if "sinais_brutos_0x2" not in st.session_state:
    st.session_state.sinais_brutos_0x2 = None
if "sinais_date_0x2" not in st.session_state:
    st.session_state.sinais_date_0x2 = None

col1, col2 = st.columns([1, 3])
with col1:
    import config
    if not config.API_TOKEN:
        st.warning("⚠️ **FUTPYTHON_TOKEN** não está configurada nos Secrets do seu Streamlit Cloud! A coleta ao vivo não funcionará sem ela.")
    
    target_date = st.date_input("Data dos Jogos", value=date.today())
    
    st.markdown("### 💰 Calculadora de Gestão de Banca")
    banca_val = st.number_input("Saldo da Banca (R$)", min_value=10.0, value=1000.0, step=100.0)
    gestao_op = st.selectbox(
        "Perfil de Risco (Juros Compostos)",
        options=[
            "Kelly 0.25 (Recomendado - Responsabilidade Máx 2.5%)",
            "Agressivo (20% Responsabilidade - Ruína < 15%)",
            "Conservador (11% Responsabilidade - Drawdown < 15%)",
            "Personalizado (%)"
        ]
    )
    if gestao_op.startswith("Kelly"):
        use_kelly = True
        f_risk_fixed = 0.025
    elif gestao_op.startswith("Agressivo"):
        use_kelly = False
        f_risk_fixed = 0.20
    elif gestao_op.startswith("Conservador"):
        use_kelly = False
        f_risk_fixed = 0.11
    else:
        use_kelly = False
        f_risk_fixed = st.number_input("Responsabilidade (%)", min_value=0.5, max_value=50.0, value=5.0, step=0.5) / 100.0
        
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary")

if st.session_state.sinais_date_0x2 != target_date:
    st.session_state.sinais_brutos_0x2 = None

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, montando Histórico Rolante e executando modelos..."):
        try:
            coleta_lay_cs_aovivo._hist_df()
            cfg = coleta_lay_cs_aovivo.MERCADOS["0x2"]
            
            # Rodar a predição real
            mod = __import__("lay_0x2_rf_v2_strategy", fromlist=["predict_and_evaluate_live"])
            bf = coleta_lay_cs_aovivo.fetch_betfair_daily(date_str)
            if bf is not None and not bf.empty:
                payload = bf.to_dict("records")
                hist = coleta_lay_cs_aovivo._hist_df()
                res = mod.predict_and_evaluate_live(payload, hist)
                # Filtrar somente os que a estratégia deu APOSTA
                sinais_aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
                st.session_state.sinais_brutos_0x2 = sinais_aprovados
            else:
                st.session_state.sinais_brutos_0x2 = []
            st.session_state.sinais_date_0x2 = target_date
        except Exception as e:
            st.error("Erro durante a execução do motor de sinais Lay 0x2:")
            st.code(traceback.format_exc())
            st.stop()

if st.session_state.sinais_brutos_0x2 is not None:
    sinais = st.session_state.sinais_brutos_0x2
    date_str = target_date.strftime("%Y-%m-%d")
    
    if not sinais:
        st.info(f"✅ Não foram encontradas oportunidades de Lay 0x2 para a data **{date_str}** que passaram em todos os filtros. É normal a estratégia ser seletiva — **guarde a banca**.")
    else:
        rows_final = []
        for j in sinais:
            odd_val = pd.to_numeric(j.get("Odd_CS_0x2_Lay"), errors="coerce")
            prob_pct = pd.to_numeric(j.get("Prob_ML"), errors="coerce") * 100
            
            if use_kelly and pd.notna(odd_val) and odd_val > 1.0 and pd.notna(prob_pct):
                p = prob_pct / 100.0
                q = 1.0 - p
                b_net = (1.0 / (odd_val - 1.0)) * 0.95
                kf = p - q / b_net
                f_applied = 0.25 * max(0.0, kf)
                f_risk = min(0.025, f_applied)
            else:
                f_risk = f_risk_fixed
                
            resp_max = banca_val * f_risk
            if pd.notna(odd_val) and odd_val > 1.0:
                stake_back = resp_max / (odd_val - 1.0)
            else:
                stake_back = np.nan
            
            rows_final.append({
                "Data": j.get("Date") or date_str,
                "Horário": str(j.get("Time", ""))[:5],
                "Liga": j.get("League", ""),
                "Mandante": j.get("Home", ""),
                "Visitante": j.get("Away", ""),
                "Odd Lay": odd_val,
                "Probabilidade (RF)": f"{prob_pct:.1f}%",
                "Responsabilidade (R$)": f"R$ {resp_max:,.2f}" if pd.notna(resp_max) else "-",
                "Stake Recomendada (R$)": f"R$ {stake_back:,.2f}" if pd.notna(stake_back) else "-",
            })
            
        df_out = pd.DataFrame(rows_final)
        
        with col2:
            st.success(f"🔥 Encontramos **{len(df_out)} oportunidades** de Lay 0x2 para **{date_str}**!")
            st.dataframe(
                df_out,
                use_container_width=True,
                hide_index=True
            )
