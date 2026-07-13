import streamlit as st
import numpy as np
import plotly.graph_objects as go

st.set_page_config(
    page_title="Alavancagem e Gestão (Kelly 15%)",
    page_icon="🚀",
    layout="wide",
)

st.title("🚀 Planejamento de Alavancagem: Lay 0x1")
st.markdown("""
Esta é a sua **Calculadora de Banca e Gestão de Risco**.  
A estratégia desenhada é a de **Juros Compostos Agressivos (Kelly 15%)** para catapultar a sua banca até a marca de **R$ 30.000**.
A partir daí, você aciona o *Freio de Ouro* para blindar o seu patrimônio.
""")

# ---- ENTRADA DE DADOS ----
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("🏦 Sua Conta Hoje")
    banca_atual = st.number_input(
        "Digite a sua Banca Atual (R$)", 
        min_value=100.0, 
        value=2000.0, 
        step=100.0,
        format="%.2f"
    )
    
    odd_media = st.number_input(
        "Odd Média de Entrada (Lay)",
        min_value=2.0,
        value=13.0,
        step=0.5,
        help="Usado para calcular a responsabilidade (Liability). Sweet Spot recomenda Odds de 12 a 15."
    )
    
    # Lógica do Freio de Mão
    LIMITE_ALAVANCAGEM = 30000.0
    
    if banca_atual < LIMITE_ALAVANCAGEM:
        st.success(f"🔥 Fase 1: ALAVANCAGEM ATIVADA (Banca abaixo de R$ 30k)")
        stake_percentual = 15.0
    else:
        st.warning(f"🛡️ Fase 2: MODO PRESERVAÇÃO. Patrimônio acima de R$ 30k.")
        stake_percentual = st.slider(
            "Você atingiu o teto de Alavancagem! Reduza seu Stake (Recomendado: 2% a 5%)",
            min_value=1.0, max_value=15.0, value=3.0, step=0.5
        )

# ---- CÁLCULOS DO DIA ----
lucro_desejado = banca_atual * (stake_percentual / 100)
responsabilidade = lucro_desejado * (odd_media - 1)

with col2:
    st.subheader("📋 O Que Você Deve Fazer Hoje (No Software)")
    
    c1, c2 = st.columns(2)
    
    c1.metric(
        "Lucro Desejado (Back) por Jogo", 
        f"R$ {lucro_desejado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    
    # Liability = Stake * (Odd - 1)
    c2.metric(
        "Responsabilidade Exigida (Liability)", 
        f"R$ {responsabilidade:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    
    st.info(f"""
    **Instruções Práticas:**
    1. Vá para a página de **Sinais**.
    2. No software da Betfair (ex: Wagertool, Layback), configure o **Lucro (Back)** para `R$ {lucro_desejado:.2f}`.
    3. Confirme que você tem, no mínimo, `R$ {responsabilidade:.2f}` de saldo livre na corretora para entrar na operação com a Odd média de {odd_media}.
    4. Programe o **Cashout automático para o minuto 60**. (Risco estimado no Red será em torno de 45% a 60% dessa responsabilidade).
    """)

st.divider()

# ---- SIMULADOR DE JUROS COMPOSTOS ----
st.subheader("🔮 Projeção de Alavancagem a Longo Prazo")
st.markdown("Veja como a sua banca se comportará em uma sequência perfeita baseada nas nossas métricas do Backtest (Win Rate ~66%).")

num_apostas = st.slider("Simular quantidade de jogos (Entradas 0x1):", 10, 500, 100)

def simular_alavancagem_perfeita(banca_inicial, num_games):
    # Simulação teórica (determinística) baseada no WR líquido e Perdas
    # Crescimento esperado por jogo = (WR * Stake) + (LossRate * Perda_Media)
    # A perda media (Decay cashout) é ~ -1.22 * Stake (quando Odd=13).
    # Vamos projetar o crescimento medio de um trade (EV puro)
    wr = 0.668
    loss_rate = 1 - wr
    # Ao bater 0x1, ganhamos a Stake. No cashout min60, perdemos ~ 1.22x a Stake
    ev_multiplier = (wr * 1.0) + (loss_rate * (-1.22)) 
    
    # Retorno percentual medio da banca por aposta (EV * Porcentagem da Stake)
    # Ex: EV_multiplier = 0.668 - 0.405 = +0.263 unidades por aposta no longo prazo
    # Crescimento medio % = Stake_Percentual * 0.263
    crescimento_medio_por_aposta = (stake_percentual / 100) * ev_multiplier
    
    evolucao = [banca_inicial]
    banca_temp = banca_inicial
    
    for i in range(num_games):
        # Limite dos 30k
        if banca_temp < 30000:
            crescimento = (15.0 / 100) * ev_multiplier
        else:
            # Finge que cravou nos 3% pos 30k
            crescimento = (3.0 / 100) * ev_multiplier
            
        banca_temp = banca_temp * (1 + crescimento)
        evolucao.append(banca_temp)
        
    return evolucao

evolucao_projetada = simular_alavancagem_perfeita(banca_atual, num_apostas)

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=list(range(len(evolucao_projetada))),
    y=evolucao_projetada,
    mode='lines',
    fill='tozeroy',
    name='Curva Projetada (Matemática Pura)',
    line=dict(color='#8e44ad', width=3)
))

# Linha do Limite de 30k
fig.add_hline(y=30000, line_dash="dash", line_color="red", annotation_text="Teto de Alavancagem (R$ 30k)")

fig.update_layout(
    title=f"Projeção Teórica de Juros Compostos ({num_apostas} Entradas)",
    xaxis_title="Nº Entradas Lay 0x1 Executadas",
    yaxis_title="Banca Projetada (R$)",
    height=400,
    margin=dict(l=0, r=0, t=40, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.caption("⚠️ **Aviso Profissional:** O gráfico acima mostra a projeção matemática pura baseada no EV (Valor Esperado) da sua estratégia. No mundo real, você enfrentará *Drawdowns* (sequências de Reds). A gestão do psicológico durante as quedas curtas é a única coisa que separa quem atinge o topo de quem quebra a conta.")
