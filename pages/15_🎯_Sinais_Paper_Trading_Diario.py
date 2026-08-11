import streamlit as st
import pandas as pd
import numpy as np
import os
import sys
import subprocess
from datetime import datetime, date

# Configuração da página Streamlit
st.set_page_config(
    page_title="Sinais de Hoje — Paper Trading ARKAD",
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
</style>
""", unsafe_allow_html=True)

# Banner Principal
st.markdown("""
<div class="banner-sinais">
    <h1 style="margin:0; font-size: 2.1rem; font-weight:700;">🎯 Sinais & Palpites de Hoje — Paper Trading ARKAD</h1>
    <p style="margin:4px 0 0 0; opacity: 0.95; font-size: 1.05rem;">
        Recomendações e Entradas Diárias para Lay 0x0 Protegido, Lay Draw, Over 2.5 Back e BTTS Lay com Odds Betfair.
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
        return df
    return pd.DataFrame()

df_all = load_all_signals()

# Sidebar - Seleção de Data e Ação
st.sidebar.header("🗓️ Seleção da Data dos Jogos")
data_selecionada = st.sidebar.date_input("Data dos Sinais", value=date.today())
data_str = data_selecionada.strftime("%Y-%m-%d")

st.sidebar.markdown("---")
st.sidebar.header("⚡ Atualizar Sinais da Data")

if st.sidebar.button("🔄 Executar Varredura de Sinais Hoje", use_container_width=True, type="primary"):
    with st.spinner(f"Buscando jogos e gerando palpites para {data_str}..."):
        try:
            res = subprocess.run(
                [sys.executable, "rodar_jogos_hoje.py", "--data", data_str],
                capture_output=True, text=True, check=True
            )
            st.sidebar.success(f"✅ Sinais de {data_str} gerados com sucesso!")
            st.cache_data.clear()
            df_all = load_all_signals()
        except subprocess.CalledProcessError as e:
            err_msg = e.stderr if e.stderr else str(e)
            st.sidebar.error(f"Erro ao gerar sinais: {err_msg}")
        except Exception as e:
            st.sidebar.error(f"Erro inesperado: {e}")

# Filtrar Sinais pela Data Selecionada
if not df_all.empty:
    df_day = df_all[df_all['data_dt'].dt.strftime('%Y-%m-%d') == data_str].copy()
else:
    df_day = pd.DataFrame()

# Se não houver sinais para a data exata, carregar últimos sinais disponíveis como fallback informativo
if df_day.empty and not df_all.empty:
    st.info(f"ℹ️ Nenhum sinal gerado ainda para a data **{data_str}**. Clique no botão na barra lateral para executar a varredura ao vivo. Exibindo os sinais mais recentes abaixo:")
    df_day = df_all.sort_values('data_dt', ascending=False).head(50).copy()
    data_display = "Mais Recentes"
else:
    data_display = data_str

# Sidebar - Filtros da Página
st.sidebar.markdown("---")
st.sidebar.header("🔍 Filtros de Exibição")

if not df_day.empty:
    metodos_disponiveis = sorted(df_day['metodo'].unique().tolist())
    metodos_sel = st.sidebar.multiselect("Filtrar por Método", metodos_disponiveis, default=metodos_disponiveis)

    status_disponiveis = sorted(df_day['status'].unique().tolist())
    status_sel = st.sidebar.multiselect("Filtrar por Status", status_disponiveis, default=status_disponiveis)

    # Aplicação de Filtros
    if metodos_sel:
        df_day = df_day[df_day['metodo'].isin(metodos_sel)]
    if status_sel:
        df_day = df_day[df_day['status'].isin(status_sel)]

# KPIs da Data
st.markdown(f"### 📊 Painel de Sinais ({data_display})")

col1, col2, col3, col4, col5 = st.columns(5)

tot_sinais = len(df_day)
tot_pendentes = (df_day['status'] == 'Pendente').sum() if 'status' in df_day.columns else 0
tot_greens = (df_day['resultado'] == 'GREEN').sum() if 'resultado' in df_day.columns else 0
tot_reds = (df_day['resultado'] == 'RED').sum() if 'resultado' in df_day.columns else 0
lucro_dia = df_day['pnl_dolar'].sum() if 'pnl_dolar' in df_day.columns else 0.0

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total de Sinais</div>
        <div class="kpi-blue">{tot_sinais}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Sinais Pendentes</div>
        <div class="kpi-yellow">{tot_pendentes}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Greens do Dia</div>
        <div class="kpi-green">{tot_greens}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Reds do Dia</div>
        <div class="kpi-red">{tot_reds}</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    color = "kpi-green" if lucro_dia >= 0 else "kpi-red"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Lucro do Dia ($)</div>
        <div class="{color}">${lucro_dia:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Tabela Detalhada de Sinais do Dia
st.markdown("### 📋 Tabela de Sinais & Palpites")

if not df_day.empty:
    disp_cols = ['data', 'liga', 'jogo', 'metodo', 'mercado', 'lado', 'odd_execucao', 'stake', 'status', 'resultado', 'pnl_dolar']
    cols_ok = [c for c in disp_cols if c in df_day.columns]
    
    st.dataframe(
        df_day[cols_ok].style.highlight_max(subset=['pnl_dolar'], color='#238636'),
        use_container_width=True,
        height=450
    )
    
    # Download dos Sinais do Dia
    csv_bytes = df_day[cols_ok].to_csv(index=False).encode('utf-8')
    st.download_button(
        label=f"📥 Baixar Palpites do Dia ({data_str}) (CSV)",
        data=csv_bytes,
        file_name=f"sinais_paper_trading_{data_str}.csv",
        mime="text/csv"
    )
else:
    st.info(f"Nenhum palpite gerado para a data selecionada ({data_str}). Clique no botão 'Executar Varredura de Sinais Hoje' na barra lateral.")

st.markdown("---")
st.caption("ARKAD Signal Generator — Sinais de Paper Trading em Tempo Real com Odds Betfair Exchange")
