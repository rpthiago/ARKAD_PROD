import streamlit as st

st.set_page_config(
    page_title="Arkad Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🤖 ARKAD - Central de Inteligência Desportiva")

st.markdown("""
### Bem-vindo ao Dashboard Oficial do Sistema Arkad.

Este painel foi desenvolvido para gerenciar e auditar as estratégias quantitativas do mercado de Exchange (Betfair). O sistema processa dados ao vivo, executa algoritmos de Machine Learning e gera Sinais filtrados baseados nos **Sweet Spots** matemáticos encontrados em nossos Backtests de longo prazo.

---

### 📚 Nossos Métodos Ativos

#### 1. 🎯 Método Lay 0x1 (Rota 60)
Uma estratégia agressiva e de alta assertividade baseada no mercado de Correct Score (Resultado Exato).
* **Conceito:** Aposta contra o placar exato de 0x1 (Vitória do Visitante por 1 gol de diferença).
* **O Sweet Spot (Filtro):** Entramos apenas em jogos onde o modelo aponta Probabilidade de ML > 60% para o favorito e a Odd Lay do 0x1 pré-live está acima de 12.0.
* **Gestão de Risco (A Rota 60):** A operação é encerrada compulsoriamente no **Minuto 60**. 
  * Se houver um gol do mandante (1x0) ou mais de um gol do visitante (0x2), a operação é vencedora e garante lucro integral.
  * Se o jogo chegar aos 60 minutos sem o placar se resolver (0x0 ou 0x1), fazemos o *Cashout*. Isso blinda o banco de dados contra a perda da Responsabilidade Total (Liability cheia), sacrificando apenas o *decay* da odd (cerca de 10% a 30% da responsabilidade).
* **Gestão de Banca:** Recomendamos o Critério de Kelly Fracionado aplicado sobre a Responsabilidade (Liability), travando a máxima exposição e alavancando os lucros compostos diários.

#### 2. 💎 Método Gemini (Back Favorito / Over)
Nosso modelo clássico focado na força de agressão e métricas de expected goals (xG).
* **Conceito:** Capturar momentos de dominância absoluta do time favorito usando dados da Bet365 e Betfair.
* **Gestão:** Usa a análise de momento de gols para validar se os padrões táticos resultaram em lucro dentro do tempo estipulado pela estratégia.

---
**Navegue pelo menu lateral para acessar os Sinais do dia, a Calculadora de Alavancagem e as Auditorias de Resultados.**
""")
