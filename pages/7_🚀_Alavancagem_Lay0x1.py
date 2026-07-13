import streamlit as st
import numpy as np
import plotly.graph_objects as go

st.set_page_config(
    page_title="Alavancagem e Gestão Lay 0x1",
    page_icon="🚀",
    layout="wide",
)

st.title("🚀 Planejamento de Alavancagem: Lay 0x1 (In-play)")
st.markdown("""
Esta é a sua **Calculadora de Banca e Gestão de Risco**.
A estratégia foi otimizada para permitir simulações de **Entrada In-play**, onde você espera os primeiros minutos do jogo em 0-0 para entrar com odds e responsabilidades muito menores, aumentando a rentabilidade.
""")

# ---- DICIONÁRIO DE PARÂMETROS OTIMIZADOS ----
MAP_METODOS = {
    "XGBoost (Trader)": {
        "odd_padrao": 19.5,
        "stake_sugerida": 5.0,
        "help_txt": "Recomendado pelo Monte Carlo: 5% a 10% de responsabilidade.",
        "scenarios": {
            "Minuto 0 (Abertura)": {"win_rate": 0.964, "decay": 1.0, "pnl_ref": "+1.56%"},
            "Minuto 10 (In-play)": {"win_rate": 0.968, "decay": 0.85, "pnl_ref": "+2.89%"},
            "Minuto 15 (In-play)": {"win_rate": 0.967, "decay": 0.78, "pnl_ref": "+3.40%"},
            "Minuto 20 (In-play)": {"win_rate": 0.969, "decay": 0.70, "pnl_ref": "+4.42%"},
        }
    },
    "Random Forest (RF)": {
        "odd_padrao": 11.5,
        "stake_sugerida": 5.0,
        "help_txt": "Recomendado pelo Monte Carlo: 3% a 5% de responsabilidade.",
        "scenarios": {
            "Minuto 0 (Abertura)": {"win_rate": 0.935, "decay": 1.0, "pnl_ref": "+3.68%"},
            "Minuto 10 (In-play)": {"win_rate": 0.937, "decay": 0.85, "pnl_ref": "+5.88%"},
            "Minuto 15 (In-play)": {"win_rate": 0.939, "decay": 0.78, "pnl_ref": "+7.41%"},
            "Minuto 20 (In-play)": {"win_rate": 0.940, "decay": 0.70, "pnl_ref": "+9.41%"},
        }
    }
}

# ---- ENTRADA DE DADOS ----
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Configuração da Operação")
    
    metodo_selecionado = st.selectbox(
        "Selecione a Estratégia",
        list(MAP_METODOS.keys())
    )
    
    dados_metodo = MAP_METODOS[metodo_selecionado]
    
    cenario_selecionado = st.selectbox(
        "Tempo de Espera (In-play)",
        list(dados_metodo["scenarios"].keys()),
        index=3, # Minuto 20 por padrão
        help="Quanto mais você espera em 0-0, menor é a odd e maior o lucro potencial por jogo."
    )
    
    sc_data = dados_metodo["scenarios"][cenario_selecionado]
    
    banca_atual = st.number_input(
        "Banca Atual (R$)", 
        min_value=100.0, 
        value=2000.0, 
        step=100.0,
        format="%.2f"
    )
    
    odd_abertura = st.number_input(
        "Odd de Abertura (Minuto 0)",
        min_value=2.0,
        value=dados_metodo["odd_padrao"],
        step=0.5,
        help="Odd média esperada antes do jogo começar."
    )
    
    # Odd de entrada calculada com base no tempo de espera
    odd_inplay_estimada = round(odd_abertura * sc_data["decay"], 2)
    if odd_inplay_estimada <= 1.01:
        odd_inplay_estimada = 1.01
        
    st.metric(
        label="Odd de Entrada Estimada (In-play)",
        value=f"{odd_inplay_estimada:.2f}",
        delta=f"-{round((1 - sc_data['decay'])*100)}% de queda",
        delta_color="normal"
    )
    
    stake_percentual = st.slider(
        "Porcentagem de Risco (Responsabilidade / Liability) sobre a Banca",
        min_value=1.0, max_value=15.0, value=dados_metodo["stake_sugerida"], step=0.5,
        help=dados_metodo["help_txt"]
    )

# ---- CÁLCULOS DO DIA ----
responsabilidade = banca_atual * (stake_percentual / 100)
# Lucro desejado in-play é baseado na odd in-play estimada
lucro_desejado = responsabilidade / (odd_inplay_estimada - 1)

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
    **Instruções Práticas para Entrada In-play:**
    1. Vá para a página de **Sinais** e verifique os jogos selecionados.
    2. Espere o jogo começar. **NÃO entre antes do início!**
    3. Monitore o placar: só entre se o jogo estiver **0x0** no **{cenario_selecionado}**. 
       *(Se sair um gol antes de qualquer equipe, aborte a missão!)*
    4. No software da Betfair, insira a sua ordem de Lay com a Odd de **{odd_inplay_estimada:.2f}** e configure a Responsabilidade Máxima para **R$ {responsabilidade:.2f}** (Stake de Lay: R$ {lucro_desejado:.2f}).
    5. 🛡️ **Segurança (Full Match):** Deixe a operação correr até o final. Não há necessidade de Cash Out aos 60 minutos.
    """)

st.divider()

# ---- SIMULADOR DE JUROS COMPOSTOS ----
st.subheader("🔮 Projeção de Alavancagem (Juros Compostos)")
st.markdown(f"Veja a evolução simulada da banca baseada nas métricas obtidas no backtest in-play para a estratégia **{metodo_selecionado} ({cenario_selecionado})**.")

num_apostas = st.slider("Simular quantidade de jogos (Entradas):", 10, 500, 100)

def simular_alavancagem_realista(banca_inicial, num_games):
    profit_factor = 0.935
    win_rate = sc_data["win_rate"]
    
    evolucao = [banca_inicial]
    banca_temp = banca_inicial
    
    for i in range(num_games):
        current_liability = banca_temp * (stake_percentual / 100)
        current_stake_gain = (current_liability / (odd_inplay_estimada - 1)) * profit_factor
        crescimento_esperado = (win_rate * current_stake_gain) - ((1 - win_rate) * current_liability)
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
    line=dict(color='#27ae60', width=3)
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
