import os
import sys
import io
import time
import subprocess
import traceback
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

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
1. **Mandante Favorito (HA -0.25 a -2.0 / Odd H $\le 2.30$):** Foco exclusivo nos 3 grupos lucrativos:
   * **HA -1.5 / -2.0 (Super Favorito):** Win Rate **99.78%** | ROI **+9.81%**
   * **HA -0.75 / -1.0 (Favorito Claro):** Win Rate **99.30%** | ROI **+5.63%**
   * **HA -0.25 / -0.5 (Ligeiro Favorito):** Win Rate **98.72%** | ROI **+2.17%**
2. **Mercado Under 2.5 Favorecido:** Odd Under 2.5 $\le 1.85$ (Expectativa de baixa média de gols).
3. **Livro de Ofertas Betfair:** Odd Lay 0x3 entre **15.00 e 35.00** e xG Visitante $\le 1.10$.

> ⚠️ **IMPORTANTE (FULL MATCH):** A estratégia opera em **Full Match** (deixando a operação correr até o final da partida). O robô só toma Red se o placar final for exatamente 0x3 para o visitante.
""")

# Inicializa o estado de sessão
if "sinais_brutos" not in st.session_state:
    st.session_state.sinais_brutos = None
if "sinais_date" not in st.session_state:
    st.session_state.sinais_date = None

col1, col2 = st.columns([1, 3])

with col1:
    import config
    token_configurado = bool(getattr(config, "API_TOKEN", None) or os.getenv("FUTPYTHON_TOKEN") or os.getenv("API_TOKEN"))
    
    if not token_configurado:
        st.warning("⚠️ **FUTPYTHON_TOKEN** não está configurada nos Secrets do Streamlit Cloud! A coleta ao vivo usará as bases locais.")
    
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

# Se mudou a data, limpa o cache de sinais brutos
if st.session_state.sinais_date != target_date:
    st.session_state.sinais_brutos = None

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, filtrando placares de Lay 0x3..."):
        try:
            subprocess.run([sys.executable, "rodar_jogos_hoje.py", "--data", date_str], check=True)
            
            csv_path = "paper_trading_forward_setembro_2026.csv"
            if os.path.exists(csv_path):
                df_all = pd.read_csv(csv_path)
                df_day = df_all[(df_all['data'] == date_str) & (df_all['metodo'].str.contains("Lay 0x3", na=False))].to_dict(orient='records')
                st.session_state.sinais_brutos = df_day
            else:
                st.session_state.sinais_brutos = []
                
            st.session_state.sinais_date = target_date
        except Exception as e:
            st.error("Erro durante a execução do motor de sinais Lay 0x3:")
            st.code(traceback.format_exc())
            st.stop()

# Carregamento dos dados salvos se não houver clique no botão
if st.session_state.sinais_brutos is None:
    csv_path = "paper_trading_forward_setembro_2026.csv"
    if os.path.exists(csv_path):
        df_all = pd.read_csv(csv_path)
        date_str = target_date.strftime("%Y-%m-%d")
        df_day = df_all[(df_all['data'] == date_str) & (df_all['metodo'].str.contains("Lay 0x3", na=False))].to_dict(orient='records')
        if df_day:
            st.session_state.sinais_brutos = df_day
            st.session_state.sinais_date = target_date

# Processamento e exibição dos resultados no painel principal (col2)
with col2:
    if st.session_state.sinais_brutos is not None:
        sinais_brutos = st.session_state.sinais_brutos
        date_str = target_date.strftime("%Y-%m-%d")
        
        if not sinais_brutos:
            st.info(f"✅ A varredura analisou a grade de **{date_str}**, mas **nenhum** palpite passou no filtro de Proteção xG (Lay 0x3 Visitante Under 2.5 + xG Visitante $\le 1.10$). É normal os modelos serem seletivos — **guarde a banca**.")
        else:
            df = pd.DataFrame(sinais_brutos)
            
            rows_final = []
            for d_idx, row in df.iterrows():
                odd_val = pd.to_numeric(row.get("odd_execucao"), errors="coerce")
                metodo = row.get("metodo", "")
                lado = row.get("lado", "lay").lower()
                jogo = row.get("jogo", "Mandante x Visitante")
                parts = jogo.split(" x ")
                mandante = parts[0] if len(parts) > 0 else "Mandante"
                visitante = parts[1] if len(parts) > 1 else "Visitante"
                
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
                if pd.notna(odd_val) and odd_val > 1.0:
                    stake_betfair = resp_max / (odd_val - 1.0)
                else:
                    stake_betfair = np.nan
                
                raw_time = row.get("horario") or row.get("Horario") or row.get("Hora") or row.get("Time") or ""
                game_time = str(raw_time).strip()[:5] if (pd.notna(raw_time) and str(raw_time).strip().lower() != "nan") else ""

                rows_final.append({
                    "Data": row.get("data", date_str),
                    "Horário": game_time,
                    "Liga": row.get("liga", ""),
                    "Mandante": mandante,
                    "Visitante": visitante,
                    "Método": metodo,
                    "Mercado": row.get("mercado", ""),
                    "Lado": lado.upper(),
                    "Odd Lay Betfair": odd_val,
                    "Responsabilidade (R$)": round(float(resp_max), 2),
                    "Stake Betfair (R$)": round(float(stake_betfair), 2) if pd.notna(stake_betfair) else np.nan,
                    "Status": row.get("status", "Pendente"),
                    "Resultado": row.get("resultado", ""),
                    "Lucro (R$)": row.get("pnl_dolar", "")
                })
                
            df_final = pd.DataFrame(rows_final)
            
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
            
            st.caption("Opere essas entradas respeitando o teto de responsabilidade calculado para colher a expectativa matemática positiva (+23.34% ROI).")
            if use_kelly:
                st.info(f"ℹ️ **Configuração de banca aplicada:** R$ {banca_val:.2f} | Gestão: Kelly 0.25 com teto de 2.5% de Responsabilidade Máxima.")
            else:
                st.info(f"ℹ️ **Configuração de banca aplicada:** R$ {banca_val:.2f} | Responsabilidade por jogo: {f_risk_fixed*100:.1f}% (R$ {banca_val*f_risk_fixed:.2f}). Se a banca aumentar ou diminuir, reajuste o valor do saldo para atualizar as stakes de juros compostos.")
