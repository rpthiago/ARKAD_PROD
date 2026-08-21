import os
import sys
import io
import time
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="Sinais Lay Empate - Ao Vivo",
    page_icon="🤝",
    layout="wide",
)

import traceback
import importlib
try:
    import coleta_lay_cs_aovivo
    importlib.reload(coleta_lay_cs_aovivo)
    import b365_data_utils
    importlib.reload(b365_data_utils)
    import lay_draw_rf_v2_strategy
    importlib.reload(lay_draw_rf_v2_strategy)
except Exception as e:
    st.error("Erro ao carregar os módulos locais do Lay Draw:")
    st.code(traceback.format_exc())
    st.stop()

st.title("🤝 Sinais Lay Draw / Lay Empate (Random Forest v2)")
st.markdown("""
Esta página bate na **API da Betfair em tempo real**, calcula as inteligências estatísticas do modelo **Random Forest (RF v2)**, e aplica os **filtros quantitativos de liquidez e valor** validados no backtest de longo prazo:

*   **🏆 Limite de Odd Lay Empate:** Odd Betfair Lay entre **3.00 e 5.50** (mercado de alta liquidez).
*   **🛡️ Filtro de Tendência Decisiva:** Bloqueia automaticamente ligas empatadoras com taxa histórica $> 0.23$.
*   **📈 Filtro de Valor (EV):** EV Mínimo $\ge 0.02$ calculado sobre a probabilidade do modelo e a cotação real.
*   **⚽ Full Match:** A operação corre até o final. Se houver qualquer vencedor (Mandante ou Visitante), a aposta é **Green**!
""")

if "sinais_brutos_draw" not in st.session_state:
    st.session_state.sinais_brutos_draw = None
if "sinais_date_draw" not in st.session_state:
    st.session_state.sinais_date_draw = None

col1, col2 = st.columns([1, 3])
with col1:
    import config
    token_configurado = bool(getattr(config, "API_TOKEN", None) or os.getenv("FUTPYTHON_TOKEN") or os.getenv("API_TOKEN"))
    if not token_configurado:
        st.warning("⚠️ **FUTPYTHON_TOKEN** não está configurada nos Secrets do seu Streamlit Cloud! A busca utilizará bases locais e abertas.")
    
    target_date = st.date_input("Data dos Jogos", value=date.today(), key="date_input_draw")
    
    st.markdown("### 💰 Calculadora de Gestão de Banca")
    banca_val = st.number_input("Saldo da Banca (R$)", min_value=10.0, value=1000.0, step=100.0, key="banca_draw")
    gestao_op = st.selectbox(
        "Perfil de Risco (Juros Compostos)",
        options=[
            "Kelly 0.25 (Recomendado - Responsabilidade Máx 2.5%)",
            "Agressivo (20% Responsabilidade - Ruína < 15%)",
            "Conservador (11% Responsabilidade - Drawdown < 15%)",
            "Personalizado (%)"
        ],
        key="gestao_draw"
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
        f_risk_fixed = st.number_input("Responsabilidade (%)", min_value=0.5, max_value=50.0, value=5.0, step=0.5, key="pct_draw") / 100.0
        
    gerar_btn = st.button("Pesquisar Oportunidades Lay Empate", type="primary", key="btn_draw")

if st.session_state.get("sinais_date_draw") != target_date:
    st.session_state.sinais_brutos_draw = None

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Consultando grade de {date_str} na Betfair e executando Random Forest Lay Draw..."):
        try:
            mod = __import__("lay_draw_rf_v2_strategy", fromlist=["predict_and_evaluate_live"])
            bf = b365_data_utils.fetch_betfair_daily(date_str)
            if bf is not None and not bf.empty:
                payload = bf.to_dict("records")
                hist = coleta_lay_cs_aovivo._hist_df()
                res = mod.predict_and_evaluate_live(payload, hist)
                sinais_aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
                st.session_state.sinais_brutos_draw = sinais_aprovados
            else:
                st.session_state.sinais_brutos_draw = []
            st.session_state.sinais_date_draw = target_date
        except Exception as e:
            st.error("Erro durante a execução do motor de sinais Lay Draw:")
            st.code(traceback.format_exc())
            st.stop()

if st.session_state.sinais_brutos_draw is not None:
    sinais = st.session_state.sinais_brutos_draw
    date_str = target_date.strftime("%Y-%m-%d")
    
    if not sinais:
        st.info(f"✅ Não foram encontradas oportunidades de Lay Empate para a data **{date_str}** que passaram em todos os filtros. É normal a estratégia ser seletiva — **guarde a banca**.")
    else:
        rows_final = []
        for j in sinais:
            odd_val = pd.to_numeric(j.get("Odd_D_FT"), errors="coerce")
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
                "Data": str(j.get("Date") or date_str)[:10],
                "Horário": str(j.get("Time", ""))[:5],
                "Liga": j.get("League", ""),
                "Mandante": j.get("Home", ""),
                "Visitante": j.get("Away", ""),
                "Odd Lay Empate": odd_val,
                "Probabilidade (Não-Empate)": f"{prob_pct:.1f}%",
                "Responsabilidade (R$)": f"R$ {resp_max:,.2f}" if pd.notna(resp_max) else "-",
                "Stake Recomendada (R$)": f"R$ {stake_back:,.2f}" if pd.notna(stake_back) else "-",
            })
            
        df_out = pd.DataFrame(rows_final)
        
        with col2:
            st.success(f"🔥 Encontramos **{len(df_out)} oportunidades** de Lay Empate para **{date_str}**!")
            st.dataframe(
                df_out,
                use_container_width=True,
                hide_index=True
            )
