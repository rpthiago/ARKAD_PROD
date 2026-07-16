import os
import sys
import io
import time
from datetime import datetime, date
import pandas as pd
import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay Home Trader",
    page_icon="🏠",
    layout="wide",
)

import importlib
try:
    import coleta_layhome_sinais
    importlib.reload(coleta_layhome_sinais)
    from coleta_layhome_sinais import sinais_do_dia, _hist_df
    import b365_data_utils
except ImportError as e:
    st.error(f"Erro ao carregar os módulos locais do Lay Home Trader: {e}")
    st.stop()

st.title("🏠 Sinais Lay Home Trader (Oportunidades)")
st.markdown("""
Esta página bate na **API em tempo real**, calcula as inteligências do motor Lay Home Trader (XGBoost), e filtra **SOMENTE AS OPORTUNIDADES AUDITADAS**:
👉 **Probabilidade ML > 52%** (Ajustável)  
👉 **Odd Back Home entre 1.40 e 2.50**

---
### 📋 REGRAS DE OPERAÇÃO (O MÉTODO)

**1. ENTRADA:**
- **Quando:** Pré-live ou nos primeiros minutos de jogo.
- **Como:** Fazer o **LAY ao Mandante** na Betfair (ou apostar dupla chance Empate/Visitante na sua casa de apostas).
- **Filtro de Odd:** Só entre se a Odd de Back ao Mandante estiver entre **1.40 e 2.50**.

**2. SAÍDAS (CASH OUT):**
- ❌ **Gol do Mandante:** Cash Out Imediato (Stop Loss). A perda média esperada nesta situação é de **-31%** da responsabilidade.
- ✅ **Gol do Visitante:** Cash Out Imediato (Take Profit). O lucro médio esperado é de **+56%** da stake investida.
- ⏱️ **0x0 no Intervalo (HT):** Cash Out no apito do primeiro tempo. O lucro médio com a desvalorização da odd do favorito é de **+21.5%**.

---
### 💰 GESTÃO DE BANCA
Utilize uma Stake fixa de **1% da banca**. Como a odd média enfrentada é em torno de 4.30 (no Lay), a perda máxima total (se não conseguir fazer o cash out e o jogo acabar com vitória do mandante) será de ~3.3% num Red. 
Os Greens cobrem os Reds de forma consistente devido à alta taxa de acertos (Win Rate de 76%).
""")

col1, col2 = st.columns([1, 3])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())
    
    st.markdown("### ⚙️ Ajustar Conservadorismo")
    prob_threshold = st.slider(
        "Probabilidade Mínima da IA (ML)",
        min_value=0.45,
        max_value=0.70,
        value=0.52,
        step=0.01,
        help="Valores menores trazem mais jogos, mas reduzem a precisão histórica média."
    )
    
    use_whitelist = st.checkbox(
        "Apenas Ligas Whitelists",
        value=False,
        help="Se desmarcado, avalia jogos de qualquer liga disponível na API Betfair."
    )
    
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary", use_container_width=True)

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, montando Histórico Rolante e executando a IA Lay Home Trader..."):
        # Garante que o histórico ta carregado na memoria
        _hist_df()
        
        # Puxa os sinais do motor
        # Vamos passar os filtros personalizados para a estratégia
        try:
            import lay_home_trader_strategy
            # Atualiza dinamicamente os parâmetros da estratégia na memória para esta execução
            lay_home_trader_strategy.CUSTOM_PROB_THRESHOLD = prob_threshold
            lay_home_trader_strategy.CUSTOM_USE_WHITELIST = use_whitelist
        except Exception:
            pass
            
        sinais_brutos = sinais_do_dia(date_str)
        
        if not sinais_brutos:
            st.warning("Nenhum jogo encontrado para hoje na API Betfair (ou fora de temporada) com os filtros atuais.")
        else:
            df = pd.DataFrame(sinais_brutos)
            
            st.divider()
            
            st.success(f"🏠 {len(df)} Oportunidades de Ouro Encontradas para Hoje!")
            
            # Exibe a tabela bonita
            st.dataframe(df, use_container_width=True)
            
            # Botao de Download
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Sinais')
            excel_data = buffer.getvalue()
            
            st.download_button(
                label="📥 Baixar Planilha para Paper Trading",
                data=excel_data,
                file_name=f"sinais_lay_home_{target_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            
            st.caption("Salve o arquivo, anote o lucro real, e gerencie com sua banca!")
