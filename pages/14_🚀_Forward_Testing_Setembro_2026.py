import streamlit as st
import pandas as pd
import numpy as np
import os
import sys
import subprocess
import plotly.express as px
import plotly.graph_objects as go

# Configuração da página Streamlit
st.set_page_config(
    page_title="Forward Testing Live — Setembro 2026",
    page_icon="🚀",
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
    .kpi-purple { font-size: 1.7rem; font-weight: 700; color: #a371f7; }
    
    .banner {
        background: linear-gradient(90deg, #238636 0%, #2ea043 50%, #1f6feb 100%);
        padding: 22px;
        border-radius: 14px;
        color: white;
        margin-bottom: 25px;
    }
</style>
""", unsafe_allow_html=True)

# Top Banner
st.markdown("""
<div class="banner">
    <h1 style="margin:0; font-size: 2.1rem; font-weight:700;">🚀 Acompanhamento de Forward Testing Live — Setembro 2026</h1>
    <p style="margin:4px 0 0 0; opacity: 0.95; font-size: 1.05rem;">
        Planilha e Monitor em Tempo Real para Auditoria de Campo (Agosto a Setembro de 2026).
    </p>
</div>
""", unsafe_allow_html=True)

# Botão para Executar a Varredura dos Jogos de Hoje diretamente no App
st.sidebar.markdown("### ⚡ Ações Rápidas")
if st.sidebar.button("🔄 Executar Varredura dos Jogos de Hoje", use_container_width=True):
    with st.spinner("Executando varredura dos jogos de hoje via Betfair Exchange..."):
        try:
            # Usa sys.executable para garantir que roda no mesmo ambiente Python do Streamlit
            res = subprocess.run([sys.executable, "rodar_jogos_hoje.py"], capture_output=True, text=True, check=True)
            st.sidebar.success("✅ Jogos de hoje processados e planilha atualizada!")
            st.cache_data.clear()
        except subprocess.CalledProcessError as e:
            err_msg = e.stderr if e.stderr else str(e)
            st.sidebar.error(f"Erro ao executar varredura: {err_msg}")
        except Exception as e:
            st.sidebar.error(f"Erro inesperado: {e}")

@st.cache_data(ttl=60)
def load_forward_data():
    csv_path = "paper_trading_forward_setembro_2026.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df['data'] = pd.to_datetime(df['data'])
        return df
    return pd.DataFrame()

df_forward = load_forward_data()

if df_forward.empty:
    st.warning("⚠️ Nenhum sinal de Forward Testing encontrado. Clique no botão na barra lateral ou execute 'python rodar_jogos_hoje.py'.")
    st.stop()

# Sidebar - Filtros do Forward Test
st.sidebar.header("⚙️ Filtros do Forward Test")

# Parâmetros de Gestão
banca_inicial = st.sidebar.number_input("Banca Inicial ($)", min_value=500.0, value=10000.0, step=500.0)
stake_fixa = st.sidebar.number_input("Stake por Operação ($)", min_value=10.0, value=100.0, step=10.0)

st.sidebar.markdown("---")

# Filtro por Método
metodos_disponiveis = sorted(df_forward['metodo'].unique().tolist())
metodos_sel = st.sidebar.multiselect("Métodos em Teste", metodos_disponiveis, default=metodos_disponiveis)

# Filtro por Mês
df_forward['mes_nome'] = df_forward['data'].dt.strftime('%B / %Y')
meses_disponiveis = df_forward['mes_nome'].unique().tolist()
meses_sel = st.sidebar.multiselect("Período de Teste", meses_disponiveis, default=meses_disponiveis)

# Aplicar Filtros
df_filtro = df_forward.copy()
if metodos_sel:
    df_filtro = df_filtro[df_filtro['metodo'].isin(metodos_sel)]
if meses_sel:
    df_filtro = df_filtro[df_filtro['mes_nome'].isin(meses_sel)]

# Métricas Principais (KPIs)
st.markdown("### 📊 KPIs de Desempenho do Forward Test")

tot_apostas = len(df_filtro)
greens = (df_filtro['resultado'] == 'GREEN').sum()
reds = (df_filtro['resultado'] == 'RED').sum()
win_rate = (greens / tot_apostas * 100.0) if tot_apostas > 0 else 0.0

pnl_total = df_filtro['pnl_dolar'].sum()
roi_ponderado = (pnl_total / (tot_apostas * stake_fixa) * 100.0) if tot_apostas > 0 else 0.0
banca_atual = banca_inicial + pnl_total

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total de Sinais</div>
        <div class="kpi-blue">{tot_apostas}</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Greens / Reds</div>
        <div class="kpi-purple">{greens} <span style="font-size:1rem; color:#8b949e;">/ {reds}</span></div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    color = "kpi-green" if win_rate >= 75.0 else ("kpi-purple" if win_rate >= 50.0 else "kpi-red")
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Taxa de Acerto (WR)</div>
        <div class="{color}">{win_rate:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    color = "kpi-green" if pnl_total >= 0 else "kpi-red"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Lucro Acumulado ($)</div>
        <div class="{color}">${pnl_total:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with c5:
    color = "kpi-green" if roi_ponderado >= 0 else "kpi-red"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">ROI Ponderado (%)</div>
        <div class="{color}">{roi_ponderado:.2f}%</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Tabela Comparativa de Métodos no Forward Test
st.markdown("### 🏆 Comparativo de Métodos no Teste de Campo")

if not df_filtro.empty:
    summary_df = df_filtro.groupby('metodo').agg(
        apostas=('pnl_dolar', 'count'),
        greens=('resultado', lambda x: (x == 'GREEN').sum()),
        reds=('resultado', lambda x: (x == 'RED').sum()),
        win_rate=('resultado', lambda x: (x == 'GREEN').mean() * 100.0),
        lucro_dolar=('pnl_dolar', 'sum')
    ).reset_index()
    
    summary_df['roi%'] = (summary_df['lucro_dolar'] / (summary_df['apostas'] * stake_fixa)) * 100.0
    summary_df = summary_df.sort_values('lucro_dolar', ascending=False).reset_index(drop=True)
    
    summary_df['win_rate'] = summary_df['win_rate'].map('{:.1f}%'.format)
    summary_df['roi%'] = summary_df['roi%'].map('{:.2f}%'.format)
    summary_df['lucro_dolar'] = summary_df['lucro_dolar'].map('${:,.2f}'.format)
    
    st.dataframe(summary_df.style.highlight_max(subset=['lucro_dolar'], color='#238636'), use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# Gráfico de Evolução da Banca no Tempo
st.markdown("### 📈 Curva Diária de Evolução do Forward Test")

if not df_filtro.empty:
    df_chart = df_filtro.sort_values('data').copy()
    df_chart['pnl_acum'] = df_chart['pnl_dolar'].cumsum()
    df_chart['banca'] = banca_inicial + df_chart['pnl_acum']
    
    fig = px.line(
        df_chart,
        x='data',
        y='banca',
        color='metodo',
        title="Evolução Diária do Lucro por Método no Forward Testing",
        labels={'data': 'Data do Jogo', 'banca': 'Saldo da Banca ($)'}
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#161b22",
        plot_bgcolor="#161b22",
        margin=dict(l=20, r=20, t=40, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)

# Planilha de Entradas Detalhada para Download
st.markdown("### 📄 Registros Diários de Sinais (Planilha de Anotação)")

st.dataframe(df_filtro, use_container_width=True, height=450)

# Download CSV
csv_bytes = df_filtro.to_csv(index=False).encode('utf-8')
st.download_button(
    label="📥 Baixar Planilha de Forward Testing (CSV para Excel)",
    data=csv_bytes,
    file_name="paper_trading_forward_setembro_2026.csv",
    mime="text/csv"
)

st.markdown("---")
st.caption("ARKAD Forward Testing Engine — Acompanhamento de Sinais Live em Tempo Real")
