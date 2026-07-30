import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay 0x1 - Desativado",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Sinais Lay 0x1 (XGBoost & Random Forest)")

st.warning("⚠️ **ESTRATÉGIA DESATIVADA PREVENTIVAMENTE**")

st.markdown("""
A estratégia **Lay 0x1** foi desativada temporariamente do portfólio de produção após uma auditoria rigorosa de integridade de dados e performance.

### 🔍 Motivos da Desativação:
1. **Falta de Edge Estatística (ROI Negativo nos Dados Reais):** A re-análise dos resultados reais do mês de julho de 2026 (excluindo falhas de dados e jogos incompletos) revelou que a estratégia operou no prejuízo com **ROI real de -7.2%** (5 Reds e 74 Greens em 79 jogos válidos de fato), ficando abaixo do break-even matemático de 94.1%.
2. **Corrupção de Dados de Liquidação (Auto-Fetch Bug):** Descobrimos que falhas de busca automática de placares na base de dados marcavam partidas incompletas/futuras temporariamente como `0-0` por padrão. Como a estratégia Lay 0x1 ganha com qualquer placar diferente de `0-1`, cada falha de busca no robô foi convertida em um **green falso fabricado**, inflando artificialmente a curva de banca (mostrando um ROI aparente de +35.9% com 131 jogos).
3. **Reprovação no Teste FDR:** Historicamente, a estratégia reprova no teste de False Discovery Rate (FDR), indicando que os lucros passados eram fruto de variância estatística temporária e não de edge real de longo prazo.

Para proteger o seu capital contra drawdowns matematicamente comprovados, as entradas desta página foram desativadas por tempo indeterminado. Apenas o **Lay 0x0** sob gestão estrita de Kelly 0.25 e Whitelist de Ligas purgada permanece ativo e em operação.
""")
