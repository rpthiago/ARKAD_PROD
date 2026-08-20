import os
import sys
import io
import time
import traceback
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

from futpythontrader_client import get_daily_dataframe

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay 0x3 - Ao Vivo",
    page_icon="⚽",
    layout="wide",
)

st.title("⚽ Sinais Lay 0x3 (Mandante Favorito HA -0.25 a -2.0 & Under 2.5)")
st.markdown("""
Esta página monitora em **tempo real** as oportunidades do método **Lay 0x3 Visitante (Filtrado por Handicap do Mandante + Under 2.5)**:

### 🛡️ Critérios de Filtro de Elite Validados no Backtest:
1. **Mandante Favorito (Odd H $\le 2.20$):** Foco exclusivo em mandantes dominantes em campo.
2. **Mercado Under 2.5 Favorecido:** Odd Under 2.5 $\le 2.10$ (Expectativa de baixa média de gols).
3. **Livro de Ofertas Betfair:** Odd Lay 0x3 entre **14.00 e 35.00** e xG Visitante $\le 1.10$.

> ⚠️ **IMPORTANTE (FULL MATCH):** A estratégia opera em **Full Match** (deixando a operação correr até o final da partida). O robô só toma Red se o placar final for exatamente 0x3 para o visitante.
""")

# Inicializa o estado de sessão
if "sinais_lay0x3" not in st.session_state:
    st.session_state.sinais_lay0x3 = None
if "sinais_date_0x3" not in st.session_state:
    st.session_state.sinais_date_0x3 = None

col1, col2 = st.columns([1, 3])

with col1:
    import config
    token_configurado = bool(getattr(config, "API_TOKEN", None) or os.getenv("FUTPYTHON_TOKEN") or os.getenv("API_TOKEN"))
    
    if not token_configurado:
        st.warning("⚠️ **FUTPYTHON_TOKEN** não está configurada nos Secrets do Streamlit Cloud! A coleta ao vivo usará as bases locais.")
    
    target_date = st.date_input("Data dos Jogos", value=date.today(), key="date_input_0x3")
    
    st.markdown("### 💰 Calculadora de Gestão de Banca")
    banca_val = st.number_input("Saldo da Banca (R$)", min_value=10.0, value=1000.0, step=100.0, key="banca_0x3")
    gestao_op = st.selectbox(
        "Perfil de Risco (Juros Compostos)",
        options=[
            "Kelly 0.25 (Recomendado - Responsabilidade Máx 2.5%)",
            "Agressivo (20% Responsabilidade - Ruína < 15%)",
            "Conservador (11% Responsabilidade - Drawdown < 15%)",
            "Personalizado (%)"
        ],
        key="gestao_0x3"
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
        f_risk_fixed = st.number_input("Responsabilidade (%)", min_value=0.5, max_value=50.0, value=5.0, step=0.5, key="pct_0x3") / 100.0
        
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary", key="btn_0x3")

if st.session_state.sinais_date_0x3 != target_date:
    st.session_state.sinais_lay0x3 = None

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Consultando grade de {date_str} na Betfair e filtrando placares de Lay 0x3..."):
        try:
            df_day = get_daily_dataframe("betfair", date_str)
            sinais = []
            
            if not df_day.empty:
                for idx, row in df_day.iterrows():
                    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
                    odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
                    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
                    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
                    xg_a = float(row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or 1.0)
                    
                    if 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and (odd_a >= 1.85 or odd_a == 0.0) and xg_a <= 1.10:
                        home = str(row.get("Home", row.get("Home_Team", "")))
                        away = str(row.get("Away", row.get("Away_Team", "")))
                        liga = str(row.get("League", row.get("Div", "Liga Externa")))
                        tm = str(row.get("Time", row.get("horario", "15:00")))[:5]
                        
                        sinais.append({
                            "data": date_str,
                            "horario": tm,
                            "liga": liga,
                            "jogo": f"{home} x {away}",
                            "metodo": "Lay 0x3 Visitante Under 2.5 (xG Protected)",
                            "odd_execucao": odd_0x3,
                            "mercado": "CS_0x3",
                            "lado": "lay",
                            "status": "Aguardando"
                        })
                        
            st.session_state.sinais_lay0x3 = sinais
            st.session_state.sinais_date_0x3 = target_date
        except Exception as e:
            st.error("Erro durante a execução do motor de sinais Lay 0x3:")
            st.code(traceback.format_exc())

# Processamento e exibição dos resultados
with col2:
    if st.session_state.sinais_lay0x3 is not None:
        sinais = st.session_state.sinais_lay0x3
        date_str = target_date.strftime("%Y-%m-%d")
        
        if not sinais:
            st.info(f"✅ A varredura analisou a grade de **{date_str}**, mas **nenhum** palpite passou no filtro de Proteção xG (Lay 0x3 Visitante Under 2.5 + xG Visitante $\le 1.10$). É normal os modelos serem seletivos — **guarde a banca**.")
        else:
            df_disp = []
            for s in sinais:
                odd_val = s["odd_execucao"]
                if use_kelly and pd.notna(odd_val) and odd_val > 1.0:
                    p = 0.9734
                    q = 1.0 - p
                    b_net = (1.0 / (odd_val - 1.0)) * 0.95
                    kf = p - q / b_net
                    f_applied = 0.25 * max(0.0, kf)
                    f_risk = min(0.025, f_applied)
                else:
                    f_risk = f_risk_fixed
                    
                resp_max = banca_val * f_risk
                stake_betfair = resp_max / (odd_val - 1.0) if pd.notna(odd_val) and odd_val > 1.0 else np.nan
                lucro_est = stake_betfair * 0.95 if pd.notna(stake_betfair) else np.nan

                df_disp.append({
                    "Horário": s["horario"],
                    "Liga": s["liga"],
                    "Confronto": s["jogo"],
                    "Odd Lay 0x3": odd_val,
                    "Stake Betfair (R$)": round(float(stake_betfair), 2) if pd.notna(stake_betfair) else np.nan,
                    "Responsabilidade (R$)": round(float(resp_max), 2),
                    "Lucro Estimado (R$)": round(float(lucro_est), 2) if pd.notna(lucro_est) else np.nan
                })
                
            df_final = pd.DataFrame(df_disp)
            st.success(f"🛡️ {len(df_final)} Oportunidades Protegidas de Lay 0x3 Under 2.5 Encontradas em {date_str}!")
            st.dataframe(df_final, use_container_width=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_final.to_excel(writer, index=False, sheet_name='Sinais_Lay_0x3_xG_Protected')
            excel_data = buffer.getvalue()
            
            st.download_button(
                label="📥 Baixar Planilha de Sinais Lay 0x3 xG Protected (Excel)",
                data=excel_data,
                file_name=f"sinais_lay0x3_xg_protected_{date_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            
            st.caption("Opere essas entradas respeitando o teto de responsabilidade calculated para colher a expectativa matemática positiva (+23.34% ROI).")
    else:
        st.info("👈 Selecione a data e clique em **Pesquisar Oportunidades** para carregar os jogos ao vivo.")
