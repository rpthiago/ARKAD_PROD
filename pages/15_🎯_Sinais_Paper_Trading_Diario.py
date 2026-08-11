import streamlit as st
import pandas as pd
import numpy as np
import os
import sys
import subprocess
from datetime import datetime, date, timedelta

# Configuração da página Streamlit
st.set_page_config(
    page_title="Sinais por Dia — Paper Trading ARKAD",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização CSS (Dark Mode Premium)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    .kpi-card {
        background: linear-gradient(135deg, #161b22 0%, #21262d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 18px;
        text-align: center;
        box-shadow: 0 4px 16px rgba(0,0,0,0.4);
    }
    .kpi-title {
        font-size: 0.8rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 6px;
    }
    .kpi-green { font-size: 1.7rem; font-weight: 700; color: #3fb950; }
    .kpi-red { font-size: 1.7rem; font-weight: 700; color: #f85149; }
    .kpi-blue { font-size: 1.7rem; font-weight: 700; color: #58a6ff; }
    .kpi-yellow { font-size: 1.7rem; font-weight: 700; color: #d29922; }
    
    .banner-sinais {
        background: linear-gradient(90deg, #1f6feb 0%, #388bfd 50%, #8957e5 100%);
        padding: 22px;
        border-radius: 14px;
        color: white;
        margin-bottom: 25px;
    }
    .game-card {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Banner Principal
st.markdown("""
<div class="banner-sinais">
    <h1 style="margin:0; font-size: 2.1rem; font-weight:700;">🎯 Navegador de Jogos & Sinais por Dia</h1>
    <p style="margin:4px 0 0 0; opacity: 0.95; font-size: 1.05rem;">
        Consulte facilmente os jogos, palpites e resultados de qualquer data do Paper Trading.
    </p>
</div>
""", unsafe_allow_html=True)

# Função de Carregamento de Dados de Sinais
@st.cache_data(ttl=30)
def load_all_signals():
    csv_path = "paper_trading_forward_setembro_2026.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df['data_dt'] = pd.to_datetime(df['data'], errors='coerce')
        df['data_str'] = df['data_dt'].dt.strftime('%Y-%m-%d')
        return df
    return pd.DataFrame()

df_all = load_all_signals()

# Lista de Datas Disponíveis no Histórico
if not df_all.empty:
    datas_disponiveis = sorted(df_all['data_str'].dropna().unique().tolist(), reverse=True)
else:
    datas_disponiveis = [datetime.now().strftime('%Y-%m-%d')]

# Sidebar - Seleção de Data e Ação
st.sidebar.header("🗓️ Navegação de Datas")

# Seletor Rápido de Datas com Jogos Registrados
data_rapida = st.sidebar.selectbox("Lista de Datas com Jogos", datas_disponiveis, index=0)

# Date Input Manual
data_manual = st.sidebar.date_input("Ou Escolha uma Data no Calendário", value=pd.to_datetime(data_rapida).date())
data_str = data_manual.strftime("%Y-%m-%d")

st.sidebar.markdown("---")
st.sidebar.header("⚡ Ações da Data")

if st.sidebar.button("🔄 Executar Varredura nesta Data", use_container_width=True, type="primary"):
    with st.spinner(f"Buscando jogos e gerando palpites para {data_str}..."):
        try:
            res = subprocess.run(
                [sys.executable, "rodar_jogos_hoje.py", "--data", data_str],
                capture_output=True, text=True, check=True
            )
            st.sidebar.success(f"✅ Sinais de {data_str} gerados!")
            st.cache_data.clear()
            df_all = load_all_signals()
        except subprocess.CalledProcessError as e:
            err_msg = e.stderr if e.stderr else str(e)
            st.sidebar.error(f"Erro ao gerar sinais: {err_msg}")
        except Exception as e:
            st.sidebar.error(f"Erro inesperado: {e}")

# Botões de Navegação Dia Anterior / Dia Seguinte no Corpo da Página
c_nav1, c_nav2, c_nav3 = st.columns([1, 2, 1])

with c_nav1:
    if st.button("◀️ Dia Anterior"):
        prev_dt = pd.to_datetime(data_str) - timedelta(days=1)
        st.session_state['selected_date'] = prev_dt.strftime('%Y-%m-%d')
        st.rerun()

with c_nav2:
    st.markdown(f"<h3 style='text-align:center; margin:0;'>📅 Jogos e Palpites de: <span style='color:#58a6ff;'>{data_str}</span></h3>", unsafe_allow_html=True)

with c_nav3:
    if st.button("Dia Seguinte ▶️"):
        next_dt = pd.to_datetime(data_str) + timedelta(days=1)
        st.session_state['selected_date'] = next_dt.strftime('%Y-%m-%d')
        st.rerun()

st.markdown("<br>", unsafe_allow_html=True)

# Filtrar Sinais pela Data Selecionada
if not df_all.empty:
    df_day = df_all[df_all['data_str'] == data_str].copy()
else:
    df_day = pd.DataFrame()

# Sidebar - Filtros de Exibição
st.sidebar.markdown("---")
st.sidebar.header("🔍 Filtros de Exibição")

if not df_day.empty:
    metodos_disponiveis = sorted(df_day['metodo'].unique().tolist())
    metodos_sel = st.sidebar.multiselect("Filtrar por Método", metodos_disponiveis, default=metodos_disponiveis)

    status_disponiveis = sorted(df_day['status'].unique().tolist())
    status_sel = st.sidebar.multiselect("Filtrar por Status", status_disponiveis, default=status_disponiveis)

    if metodos_sel:
        df_day = df_day[df_day['metodo'].isin(metodos_sel)]
    if status_sel:
        df_day = df_day[df_day['status'].isin(status_sel)]

# KPIs da Data
col1, col2, col3, col4, col5 = st.columns(5)

tot_sinais = len(df_day)
tot_pendentes = (df_day['status'] == 'Pendente').sum() if 'status' in df_day.columns and not df_day.empty else 0
tot_greens = (df_day['resultado'] == 'GREEN').sum() if 'resultado' in df_day.columns and not df_day.empty else 0
tot_reds = (df_day['resultado'] == 'RED').sum() if 'resultado' in df_day.columns and not df_day.empty else 0
lucro_dia = df_day['pnl_dolar'].sum() if 'pnl_dolar' in df_day.columns and not df_day.empty else 0.0

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total de Palpites</div>
        <div class="kpi-blue">{tot_sinais}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Pendentes</div>
        <div class="kpi-yellow">{tot_pendentes}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Greens</div>
        <div class="kpi-green">{tot_greens}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Reds</div>
        <div class="kpi-red">{tot_reds}</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    color = "kpi-green" if lucro_dia >= 0 else "kpi-red"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Resultado do Dia ($)</div>
        <div class="{color}">${lucro_dia:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Tabela Detalhada dos Jogos do Dia
if not df_day.empty:
    st.markdown(f"### 📋 Lista de Jogos e Palpites do Dia ({len(df_day)} palpites)")
    
    disp_cols = ['liga', 'jogo', 'metodo', 'mercado', 'lado', 'odd_execucao', 'stake', 'status', 'resultado', 'pnl_unidades', 'pnl_dolar']
    cols_ok = [c for c in disp_cols if c in df_day.columns]
    
    st.dataframe(
        df_day[cols_ok].style.highlight_max(subset=['pnl_dolar'], color='#238636'),
        use_container_width=True,
        height=500
    )
    
    # Download dos Sinais do Dia Selecionado
    csv_bytes = df_day[cols_ok].to_csv(index=False).encode('utf-8')
    st.download_button(
        label=f"📥 Baixar Palpites de {data_str} (CSV)",
        data=csv_bytes,
        file_name=f"jogos_sinais_{data_str}.csv",
        mime="text/csv"
    )
else:
    st.info(f"ℹ️ Nenhum sinal registrado na planilha para a data **{data_str}**. Clique no botão **`🔄 Executar Varredura nesta Data`** na barra lateral para gerar os palpites deste dia.")

st.markdown("---")
st.caption("ARKAD Day-by-Day Match Browser — Varredura Diária de Jogos & Odds Betfair Exchange")
