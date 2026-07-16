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

#### 1. 🎯 Método Lay 0x1
Uma estratégia de alta assertividade baseada no mercado de Correct Score (Resultado Exato).
* **Conceito:** Aposta contra o placar exato de 0x1 (Vitória do Visitante por 1 gol de diferença).
* **O Sweet Spot (Filtro):** Entramos apenas em jogos onde as odds de Lay na Betfair estão entre **13.20 e 18.00**.
* **Gestão:** Operação levada em **Full Match** até o final.

#### 2. 🎯 Método Lay 0x0 (XGBoost v2)
Uma estratégia matematicamente validada e imune a look-ahead bias (Auditoria de Truncamento Aprovada).
* **Conceito:** Aposta contra o empate sem gols (0x0).
* **O Sweet Spot (Filtro):** Entramos apenas em jogos com odds de Lay na Betfair entre **10.00 e 99.00** onde o EV do Lay é maior que 0.02.
* **Gestão:** Operação levada em **Full Match** até o final. Lucro nominal de **+21.76% de ROI** (FDR Aprovado).

#### 3. 💎 Método Gemini (Back Favorito / Over)
Nosso modelo clássico focado na força de agressão e métricas de expected goals (xG).
* **Conceito:** Capturar momentos de dominância absoluta do time favorito usando dados da Bet365 e Betfair.

---
**Navegue pelo menu lateral para acessar os Sinais do dia, a Calculadora de Alavancagem e as Auditorias de Resultados.**
""")
