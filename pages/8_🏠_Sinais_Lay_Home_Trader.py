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

try:
    from coleta_layhome_sinais import sinais_do_dia, _hist_df
    import b365_data_utils
except ImportError as e:
    st.error(f"Erro ao carregar os módulos locais do Lay Home Trader: {e}")
    st.stop()

st.title("🏠 Sinais Lay Home Trader (Oportunidades)")
st.markdown("""
Esta página bate na **API em tempo real**, calcula as inteligências do motor Lay Home Trader (XGBoost), e filtra **SOMENTE AS OPORTUNIDADES AUDITADAS**:
👉 **Probabilidade ML > 56% (Média da Planilha)**  
👉 **Odd Back Home entre 1.40 e 2.50**

**Gestão de Banca Sugerida:**
Utilize uma Stake fixa de **1% da banca**. Como a odd média enfrentada é em torno de 4.30 (no Lay), a perda máxima será de ~3.3% num Red. Os Greens cobrem os Reds de forma consistente devido à alta taxa de acertos (Win Rate de 76%).
""")

col1, col2 = st.columns([1, 3])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary")

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, montando Histórico Rolante e executando a IA Lay Home Trader..."):
        # Garante que o histórico ta carregado na memoria
        _hist_df()
        
        # Puxa os sinais do motor
        sinais_brutos = sinais_do_dia(date_str)
        
        if not sinais_brutos:
            st.warning("Nenhum jogo encontrado para hoje na API Betfair (ou fora de temporada).")
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
