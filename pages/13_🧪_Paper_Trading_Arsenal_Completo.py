import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go

# Configuração da página Streamlit
st.set_page_config(
    page_title="Paper Trading — Arsenal Completo ARKAD",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização CSS de Alto Padrão (Dark Mode Premium)
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
    .metric-card {
        background: linear-gradient(135deg, #161b22 0%, #21262d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    .metric-title {
        font-size: 0.85rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
    }
    .metric-value-green {
        font-size: 1.8rem;
        font-weight: 700;
        color: #3fb950;
    }
    .metric-value-red {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f85149;
    }
    .metric-value-yellow {
        font-size: 1.8rem;
        font-weight: 700;
        color: #d29922;
    }
    .metric-value-blue {
        font-size: 1.8rem;
        font-weight: 700;
        color: #58a6ff;
    }
    .header-banner {
        background: linear-gradient(90deg, #1f6feb 0%, #388bfd 50%, #8957e5 100%);
        padding: 24px;
        border-radius: 14px;
        color: white;
        margin-bottom: 25px;
    }
</style>
""", unsafe_allow_html=True)

# Top Banner
st.markdown("""
<div class="header-banner">
    <h1 style="margin:0; font-size: 2.2rem; font-weight:700;">🧪 Paper Trading — Arsenal Completo ARKAD</h1>
    <p style="margin:5px 0 0 0; opacity: 0.9; font-size: 1.05rem;">
        Simulador Interativo de Paper Trading para todos os 312 Métodos & Modelos da Base Auditada.
    </p>
</div>
""", unsafe_allow_html=True)

# Carregamento de Dados
@st.cache_data
def load_data():
    csv_path = "scratch/tabela_consolidada_todos_metodos_arkad.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
    else:
        df = pd.read_csv("scratch/resultado_busca_edge_arkad.csv")
    return df

df_methods = load_data()

# Sidebar - Filtros de Paper Trading
st.sidebar.header("⚙️ Parâmetros de Paper Trading")

banca_inicial = st.sidebar.number_input("Banca Inicial ($)", min_value=100.0, value=10000.0, step=500.0)
stake_fixa = st.sidebar.number_input("Stake por Aposta ($)", min_value=10.0, value=100.0, step=10.0)
comissao_bf = st.sidebar.slider("Comissão Betfair Exchange (%)", min_value=0.0, max_value=10.0, value=5.0, step=0.5)

st.sidebar.markdown("---")
st.sidebar.header("🔍 Filtros de Seleção de Métodos")

# Filtro de Liquidez
filtro_liquidez = st.sidebar.radio("Filtro de Liquidez", ["Apenas Mercados Líquidos (Spread < 10%)", "Todos os Mercados (Inclui Correct Score)"])

# Filtro de Mercado
mercados_disponiveis = sorted(df_methods['mercado'].unique().tolist())
mercados_sel = st.sidebar.multiselect("Mercados", mercados_disponiveis, default=mercados_disponiveis)

# Filtro de Lado (Back / Lay)
lados_sel = st.sidebar.multiselect("Lado da Operação", ["back", "lay"], default=["back", "lay"])

# Filtro de Modelo
modelos_disponiveis = sorted(df_methods['modelo'].unique().tolist())
modelos_sel = st.sidebar.multiselect("Modelos de IA", modelos_disponiveis, default=modelos_disponiveis)

# Aplicar Filtros
df_filtered = df_methods.copy()

if "Apenas Mercados Líquidos" in filtro_liquidez:
    df_filtered = df_filtered[df_filtered['liquido'] == 'Sim']

if mercados_sel:
    df_filtered = df_filtered[df_filtered['mercado'].isin(mercados_sel)]

if lados_sel:
    df_filtered = df_filtered[df_filtered['lado'].isin(lados_sel)]

if modelos_sel:
    df_filtered = df_filtered[df_filtered['modelo'].isin(modelos_sel)]

# Métricas Consolidadas de Paper Trading
st.markdown("### 📊 KPIs de Desempenho do Paper Trading")

col1, col2, col3, col4, col5 = st.columns(5)

total_metodos = len(df_filtered)
total_apostas = df_filtered['val_n'].sum()

# Cálculo de PnL em $
df_filtered['roi_num'] = df_filtered['val_roi%'].astype(str).str.rstrip('%').astype(float)
df_filtered['pnl_dolar'] = (df_filtered['roi_num'] / 100.0) * df_filtered['val_n'] * stake_fixa

pnl_total_dolar = df_filtered['pnl_dolar'].sum()
roi_medio_ponderado = (pnl_total_dolar / (total_apostas * stake_fixa) * 100.0) if total_apostas > 0 else 0.0
banca_final = banca_inicial + pnl_total_dolar

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Métodos Selecionados</div>
        <div class="metric-value-blue">{total_metodos}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total de Apostas</div>
        <div class="metric-value-yellow">{total_apostas:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    color_class = "metric-value-green" if pnl_total_dolar >= 0 else "metric-value-red"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Lucro Líquido Simulado ($)</div>
        <div class="{color_class}">${pnl_total_dolar:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    color_class = "metric-value-green" if roi_medio_ponderado >= 0 else "metric-value-red"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">ROI Ponderado (%)</div>
        <div class="{color_class}">{roi_medio_ponderado:.2f}%</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Banca Final Simulado</div>
        <div class="metric-value-blue">${banca_final:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Gráfico de Curva de PnL Simulado por Método
st.markdown("### 📈 Curva de Desempenho Simulado dos Métodos Selecionados")

if total_metodos > 0:
    df_chart = df_filtered.sort_values('roi_num', ascending=False).reset_index(drop=True)
    df_chart['cum_pnl'] = df_chart['pnl_dolar'].cumsum()
    df_chart['banca_evolucao'] = banca_inicial + df_chart['cum_pnl']
    
    fig = px.line(
        df_chart, 
        y='banca_evolucao', 
        x=df_chart.index,
        title="Evolução da Banca em Paper Trading (Acumulado por Método)",
        labels={'x': 'Índice da Estratégia', 'banca_evolucao': 'Banca Simulado ($)'},
        color_discrete_sequence=['#58a6ff']
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#161b22",
        plot_bgcolor="#161b22",
        margin=dict(l=20, r=20, t=40, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Nenhum método atende aos filtros selecionados na sidebar.")

# Tabela Detalhada dos Métodos para Paper Trading
st.markdown("### 📋 Tabela Completa de Métodos & Resultados da Auditoria")

show_cols = [
    'mercado', 'lado', 'modelo', 'liquido', 'val_n', 'val_wr%', 
    'odd_real', 'val_roi%', 'val_ic95', 'val_p', 'conf_roi%', 'APROVADO', 'motivo_reprovacao'
]
cols_existing = [c for c in show_cols if c in df_filtered.columns]

st.dataframe(
    df_filtered[cols_existing].style.highlight_max(axis=0, color='#1f6feb'),
    use_container_width=True,
    height=500
)

# Botão de Download dos Sinais para Paper Trading
csv_export = df_filtered[cols_existing].to_csv(index=False).encode('utf-8')
st.download_button(
    label="📥 Baixar Tabela de Métodos para Paper Trading (CSV)",
    data=csv_export,
    file_name="paper_trading_metodos_arkad.csv",
    mime="text/csv"
)

st.markdown("---")
st.caption("ARKAD Quantitative Research Lab — Sistema de Paper Trading & Auditoria Invariante de Odds Betfair")
