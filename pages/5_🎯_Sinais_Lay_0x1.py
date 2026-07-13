import os
import sys
import io
import time
from datetime import datetime, date
import pandas as pd
import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay 0x1 - Ao Vivo",
    page_icon="🎯",
    layout="wide",
)

# Adiciona o repo do robô ao PATH para rodar os métodos
REPO_ROBOS = r"c:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1"
if REPO_ROBOS not in sys.path:
    sys.path.append(REPO_ROBOS)

try:
    from coleta_lay_cs_aovivo import sinais_do_dia, _hist_df, MERCADOS
    import b365_data_utils
except ImportError as e:
    st.error(f"Erro ao importar robôs (DASHBOARD_ARKAD-1 não encontrado ou incompatível): {e}")
    st.stop()

st.title("🎯 Sinais Lay 0x1 - Sweet Spot")
st.markdown("""
Esta página bate na **API em tempo real**, calcula as inteligências dos motores Agressivo, RF e B365, e filtra **SOMENTE O FILÉ MIGNON**:
👉 **Probabilidade > 60%**  
👉 **Odd Betfair Lay > 12.00**

(Odds menores que 12 são descartadas pois o Risco/Retorno não compensa no longo prazo, conforme nosso Backtest Master de 12.000+ jogos.)
""")

col1, col2 = st.columns([1, 3])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary")

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, montando Histórico Rolante e executando IAs..."):
        # Garante que o histórico ta carregado na memoria
        _hist_df()
        
        # Puxa os sinais do motor 0x1
        cfg = MERCADOS["0x1"]
        sinais_brutos = sinais_do_dia(date_str, cfg)
        
        if not sinais_brutos:
            st.warning("Nenhum jogo encontrado para hoje na API Betfair (ou fora de temporada).")
        else:
            df = pd.DataFrame(sinais_brutos)
            
            # Limpa colunas e força numérico para o filtro
            df["Odd_Num"] = pd.to_numeric(df["Odd_lay_entrada"], errors="coerce")
            df["Prob_Num"] = pd.to_numeric(df["Prob"], errors="coerce")
            
            # FILTRO DE OURO (Sweet Spot)
            # Prob > 60% e Odd > 12
            df_filtro = df[(df["Prob_Num"] >= 60.0) & (df["Odd_Num"] >= 12.0)].copy()
            
            st.divider()
            
            if df_filtro.empty:
                st.info(f"O robô encontrou {len(df)} jogos em potencial hoje, mas **nenhum** bateu a regra de Ouro (Prob>60 e Odd>12). Guarde a banca!")
                with st.expander("Ver todos os palpites rejeitados"):
                    st.dataframe(df.drop(columns=["Odd_Num", "Prob_Num"]), use_container_width=True)
            else:
                st.success(f"🔥 {len(df_filtro)} Oportunidades de Ouro Encontradas para Hoje!")
                
                # Exibe a tabela bonita
                exibir = df_filtro.drop(columns=["Odd_Num", "Prob_Num"])
                st.dataframe(exibir, use_container_width=True)
                
                # Botao de Download
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    exibir.to_excel(writer, index=False, sheet_name='Sinais')
                excel_data = buffer.getvalue()
                
                st.download_button(
                    label="📥 Baixar Planilha para Paper Trading",
                    data=excel_data,
                    file_name=f"sinais_lay0x1_gold_{target_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                
                st.caption("Salve o arquivo, anote o lucro simulado saindo no MINUTO 60, e depois coloque o arquivo na pasta `paper_trading_lay0x1` para ver o gráfico na Página 6!")
