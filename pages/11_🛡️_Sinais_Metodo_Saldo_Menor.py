import os
import sys
import io
import traceback
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

# Configuração da página Streamlit
st.set_page_config(
    page_title="Sinais Método Saldo Menor & Múltiplas",
    page_icon="🛡️",
    layout="wide"
)

# Adiciona o diretório raiz ao sys.path para importações locais
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import b365_data_utils
    import metodo_saldo_menor_strategy
    import betmines_validator
except Exception as e:
    st.error("Erro ao carregar os módulos locais do Método Saldo Menor:")
    st.code(traceback.format_exc())
    st.stop()

st.title("🛡️ Sinais ao Vivo - MÉTODO SALDO MENOR & MÚLTIPLAS TRIPLAS")

st.markdown("""
Esta página analisa a grade de jogos do dia em tempo real na Bet365/Betfair, filtra as melhores partidas no **Handicap Europeu +3 (EH +3) para a Zebra** (jogos com **xG <= 2.0**) e **monta automaticamente bilhetes de Múltiplas Triplas (3 jogos com Odd ~1.20 a 1.30)**.

### 📐 Parâmetros Validados no Backtest:
* **Zebra +3 Goals (`EH_H_pos_3` ou `EH_A_pos_3`):** 3 gols de vantagem no Handicap Europeu.
* **Faixa de Odds (2.20 a 5.00):** Jogos equilibrados/moderados.
* **xG Total ($\le 2.0$):** Baixíssima expectativa de gols.
* **Assertividade no Backtest Histórico:** **95.94%** nas entradas simples | **88.56% nas Múltiplas Triplas (ROI 6.47%)**.
""")

col1, col2, col3 = st.columns([1.5, 1.5, 2])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())

with col2:
    tamanho_multipla = st.selectbox("Tamanho da Múltipla", options=[3, 2], index=0, help="3 Jogos (Tripla - Odd ~1.20 | ROI 6.47%) ou 2 Jogos (Dupla - Odd ~1.13 | ROI 4.08%)")

with col3:
    usar_betmines = st.checkbox("Validar com Betmines", value=False, help="Realiza consulta ao vivo das estimativas do Betmines")

with col1:
    gerar_btn = st.button("🚀 Buscar Oportunidades & Gerar Múltiplas", type="primary")

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str} via API Bet365 e montando Múltiplas Triplas..."):
        try:
            b365_df = b365_data_utils.fetch_b365_daily(date_str)
        except Exception as e:
            st.error("Erro ao buscar grade diária de jogos:")
            st.code(traceback.format_exc())
            st.stop()

        if b365_df.empty:
            st.warning(f"Nenhum jogo encontrado para a data {date_str} na API Bet365.")
        else:
            payloads = b365_df.to_dict('records')
            
            evaluated = metodo_saldo_menor_strategy.predict_and_evaluate_live(
                payloads, check_betmines=usar_betmines
            )

            df_eval = pd.DataFrame(evaluated)
            
            # Filtrar apenas aprovados para a aposta
            df_aprovados = df_eval[df_eval['Decision'] == 'APOSTA'].copy()

            st.divider()

            if df_aprovados.empty:
                st.info(f"O robô analisou {len(df_eval)} jogos para {date_str}, mas **nenhum** atendeu aos critérios estritos de Saldo Menor (EH +3 & xG <= 2.0). Guarde a banca!")
                with st.expander("Ver todos os jogos analisados e motivos de rejeição"):
                    st.dataframe(df_eval[['Date', 'Time', 'League', 'Home', 'Away', 'Odd_H_FT', 'Odd_A_FT', 'Total_xG', 'Reason']], use_container_width=True)
            else:
                st.success(f"🔥 {len(df_aprovados)} Oportunidades de Saldo Menor Encontradas!")

                tab1, tab2 = st.tabs(["📋 Bilhetes de Múltiplas Agrupadas", "📊 Todos os Sinais Individuais"])

                with tab1:
                    num_jogos = len(df_aprovados)
                    num_bilhetes = num_jogos // tamanho_multipla

                    if num_bilhetes == 0:
                        st.warning(f"Existem {num_jogos} jogo(s) aprovado(s) hoje, mas são necessários no mínimo {tamanho_multipla} para montar uma Múltipla.")
                    else:
                        st.subheader(f"🎯 {num_bilhetes} Bilhete(s) de Múltipla ({tamanho_multipla} Jogos cada) Gerados")

                        bilhetes_list = []
                        for i in range(num_bilhetes):
                            chunk = df_aprovados.iloc[i * tamanho_multipla : (i + 1) * tamanho_multipla]
                            odd_combinada = float(chunk['eh_zebra_plus3_odd'].prod())

                            st.markdown(f"#### 🎫 Bilhete #{i+1} — Odd Final Combinada: **{odd_combinada:.2f}**")
                            
                            chunk_display = chunk[['Time', 'League', 'Home', 'Away', 'zebra_team', 'eh_zebra_plus3_odd', 'Total_xG']].copy()
                            chunk_display.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Zebra (+3 EH)', 'Odd EH +3 Zebra', 'xG Total']
                            
                            st.dataframe(chunk_display, use_container_width=True)

                            bilhetes_list.append({
                                'Bilhete_ID': f"Bilhete #{i+1}",
                                'Odd_Final_Combinada': round(odd_combinada, 2),
                                'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({r['zebra_team']} +3)" for _, r in chunk.iterrows()])
                            })

                        # Download Excel de Múltiplas
                        df_bilhetes_export = pd.DataFrame(bilhetes_list)
                        buffer_m = io.BytesIO()
                        with pd.ExcelWriter(buffer_m, engine='openpyxl') as writer:
                            df_bilhetes_export.to_excel(writer, index=False, sheet_name='Multiplas')
                        excel_data_m = buffer_m.getvalue()

                        st.download_button(
                            label="📥 Baixar Planilha de Múltiplas (Excel)",
                            data=excel_data_m,
                            file_name=f"multiplas_saldo_menor_{date_str}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

                with tab2:
                    display_cols = [
                        'Date', 'Time', 'League', 'Home', 'Away', 
                        'zebra_team', 'fav_team', 'fav_odd', 
                        'eh_zebra_plus3_odd', 'Total_xG', 'Reason'
                    ]

                    for col in display_cols:
                        if col not in df_aprovados.columns:
                            df_aprovados[col] = ""

                    df_display = df_aprovados[display_cols].copy()
                    df_display.columns = [
                        'Data', 'Horário', 'Liga', 'Mandante', 'Visitante', 
                        'Zebra', 'Favorito', 'Odd Favorito', 
                        'Odd EH +3 Zebra', 'xG Total', 'Status'
                    ]

                    st.dataframe(df_display, use_container_width=True)

                    # Download Excel de Sinais Individuais
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        df_display.to_excel(writer, index=False, sheet_name='Sinais_Individuais')
                    excel_data = buffer.getvalue()

                    st.download_button(
                        label="📥 Baixar Planilha de Sinais Individuais (Excel)",
                        data=excel_data,
                        file_name=f"sinais_individuais_saldo_menor_{date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
