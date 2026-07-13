import streamlit as st
import numpy as np
import plotly.graph_objects as go

st.set_page_config(
    page_title="Alavancagem e Gestão Lay 0x1",
    page_icon="🚀",
    layout="wide",
)

st.title("🚀 Planejamento de Alavancagem: Lay 0x1")
st.markdown("""
Esta é a sua **Calculadora de Banca e Gestão de Risco**.
A estratégia foi otimizada para **Full Match** (segurar a operação até o final, sem cashout aos 60 minutos), utilizando a gestão ideal calculada pela nossa simulação de Monte Carlo.
""")

# ---- ENTRADA DE DADOS ----
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Configuração da Operação")
    
    metodo_selecionado = st.selectbox(
        "Selecione a Estratégia",
        ["Random Forest (RF)", "XGBoost (Trader)"]
    )
    
    banca_atual = st.number_input(
        "Banca Atual (R$)", 
        min_value=100.0, 
        value=2000.0, 
        step=100.0,
        format="%.2f"
    )
    
    if metodo_selecionado == "XGBoost (Trader)":
        odd_padrao = 19.5
        stake_sugerida = 5.0
        win_rate_hist = 0.978
        help_txt = "Recomendado pela simulação de Monte Carlo: 5% a 10% de responsabilidade."
    else:
        odd_padrao = 11.5
        stake_sugerida = 5.0
        win_rate_hist = 0.931
        help_txt = "Recomendado pela simulação de Monte Carlo: 3% a 5% de responsabilidade."

    odd_media = st.number_input(
        "Odd Média do Lay",
        min_value=2.0,
        value=odd_padrao,
        step=0.5,
        help="Odd média esperada de entrada na Betfair."
    )
    
    stake_percentual = st.slider(
        "Porcentagem de Risco (Responsabilidade / Liability) sobre a Banca",
        min_value=1.0, max_value=15.0, value=stake_sugerida, step=0.5,
        help=help_txt
    )

# ---- CÁLCULOS DO DIA ----
responsabilidade = banca_atual * (stake_percentual / 100)
lucro_desejado = responsabilidade / (odd_media - 1)

with col2:
    st.subheader("📋 Configuração da Entrada no Software")
    
    c1, c2 = st.columns(2)
    
    c1.metric(
        "Stake de Lay (Lucro Back)", 
        f"R$ {lucro_desejado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    
    c2.metric(
        "Responsabilidade Exigida (Liability)", 
        f"R$ {responsabilidade:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    
    st.info(f"""
    **Instruções Práticas para Operação:**
    1. Vá para a página de **Sinais**.
    2. No software da Betfair (ex: Wagertool, Layback), configure o **Lucro (Back)** do Lay para `R$ {lucro_desejado:.2f}`.
    3. Confirme que você tem, no mínimo, `R$ {responsabilidade:.2f}` de saldo livre na corretora para entrar na operação.
    4. ⚠️ **IMPORTANTE:** Não programe Cash Out automático no minuto 60 (isso gera prejuízo). Deixe a operação correr em **Full Match** até o final. Você só toma Red se o placar final for exatamente 0x1.
    """)

st.divider()

# ---- SIMULADOR DE JUROS COMPOSTOS ----
st.subheader("🔮 Projeção de Alavancagem (Juros Compostos)")
st.markdown(f"Veja a evolução simulada da banca baseada nas métricas reais obtidas no backtest de longo prazo para a estratégia **{metodo_selecionado}**.")

num_apostas = st.slider("Simular quantidade de jogos (Entradas):", 10, 500, 100)

def simular_alavancagem_realista(banca_inicial, num_games):
    profit_factor = 0.935
    
    evolucao = [banca_inicial]
    banca_temp = banca_inicial
    
    for i in range(num_games):
        current_liability = banca_temp * (stake_percentual / 100)
        current_stake_gain = (current_liability / (odd_media - 1)) * profit_factor
        crescimento_esperado = (win_rate_hist * current_stake_gain) - ((1 - win_rate_hist) * current_liability)
        banca_temp = banca_temp + crescimento_esperado
        evolucao.append(banca_temp)
        
    return evolucao

evolucao_projetada = simular_alavancagem_realista(banca_atual, num_apostas)

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=list(range(len(evolucao_projetada))),
    y=evolucao_projetada,
    mode='lines',
    fill='tozeroy',
    name='Curva Esperada',
    line=dict(color='#8e44ad', width=3)
))

fig.update_layout(
    title=f"Projeção Matemática de Longo Prazo ({num_apostas} Entradas)",
    xaxis_title="Nº Entradas Lay 0x1 Executadas",
    yaxis_title="Banca Projetada (R$)",
    height=400,
    margin=dict(l=0, r=0, t=40, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.caption("⚠️ **Aviso Profissional:** O gráfico acima mostra a projeção matemática pura baseada no EV (Valor Esperado) da sua estratégia. No mundo real, você enfrentará *Drawdowns* (sequências de Reds). A gestão do psicológico durante as quedas curtas é a única coisa que separa quem atinge o topo de quem quebra a conta.")
