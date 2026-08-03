import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
import traceback

import b365_data_utils
import metodo_over05_strategy

st.set_page_config(page_title="Sinais Múltiplas Over 0.5 FT", page_icon="⚽", layout="wide")

st.title("⚽ Sinais & Gerador de Múltiplas OVER 0.5 FT")
st.caption("Estratégia Quantitativa baseada em Alta Expectativa de Gols (xG > 2.0) | Taxa de 0x0 de Apenas 2.57%")

st.markdown("""
Esta página analisa a grade de jogos do dia e seleciona partidas com **altíssima probabilidade de pelo menos 1 gol (Over 0.5 FT / Lay 0x0)**.

### 📐 Parâmetros Otimizados no Backtest Histórico:
* **xG Total ($> 2.0$):** Forte apetite ofensivo projetado no mercado.
* **Odd do Empate ($> 3.30$):** Baixa probabilidade de jogo truncado em 0x0.
* **Assertividade no Backtest:** **97.43% em entradas simples** | **92.50% nas Múltiplas Triplas (3 Jogos com Odd ~1.26)**.
""")

col1, col2, col3 = st.columns([1.5, 1.5, 1.5])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today(), key="over05_date")

with col2:
    tamanho_multipla = st.selectbox("Tamanho da Múltipla", options=[3, 2, 4, 5], index=0, key="over05_size", help="2 Jogos (Dupla - Odd ~1.17 | WR 94.9%), 3 Jogos (Tripla - Odd ~1.26 | WR 92.5%), 4 Jogos (Quadrupla - Odd ~1.36 | WR 90.1%)")

with col3:
    st.write("")
    st.write("")
    gerar_btn = st.button("🚀 Buscar Jogos & Gerar Múltiplas Over 0.5", type="primary", use_container_width=True)

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str} via API Bet365 e gerando Múltiplas Over 0.5 FT..."):
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
            evaluated = metodo_over05_strategy.predict_and_evaluate_over05_live(payloads)
            df_eval = pd.DataFrame(evaluated)
            
            df_aprovados = df_eval[df_eval['Decision'] == 'APOSTA'].copy()
            total_analisados = len(df_eval)
            total_aprovados = len(df_aprovados)

            # Métricas
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Jogos Analisados", total_analisados)
            m2.metric("Jogos Aprovados (xG > 2.0)", total_aprovados)
            
            num_bilhetes = total_aprovados // tamanho_multipla
            m3.metric("Bilhetes Múltiplos Criados", num_bilhetes)
            m4.metric("Assertividade Histórica Esperada", "92.50%" if tamanho_multipla == 3 else "94.93%")

            st.divider()

            if df_aprovados.empty:
                st.info("Nenhum jogo atendeu aos critérios estritos de Over 0.5 FT (xG > 2.0 e Odd Empate > 3.30) para esta data.")
            else:
                tab1, tab2 = st.tabs(["🎫 Bilhetes Prontos de Múltiplas", "📋 Tabela Geral de Jogos Aprovados"])

                with tab1:
                    st.subheader(f"🎫 {num_bilhetes} Bilhete(s) de Múltiplas Over 0.5 FT ({tamanho_multipla} Jogos por Bilhete)")
                    
                    bilhetes_list = []
                    for i in range(num_bilhetes):
                        chunk = df_aprovados.iloc[i * tamanho_multipla : (i + 1) * tamanho_multipla]
                        odd_total = float(chunk['odd_over05'].prod())

                        st.markdown(f"#### 🟡 Bilhete #{i+1} — Odd Total Múltipla: **`{odd_total:.2f}`**")
                        
                        chunk_display = chunk[['Time', 'League', 'Home', 'Away', 'odd_over05', 'Total_xG']].copy()
                        chunk_display.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Odd Over 0.5 FT', 'xG Total']
                        
                        st.dataframe(chunk_display, use_container_width=True)

                        bilhetes_list.append({
                            'Bilhete_ID': f"Bilhete Over 0.5 #{i+1}",
                            'Odd_Final_Combinada': round(odd_total, 2),
                            'Jogos': " | ".join([f"{r['Home']} x {r['Away']} (Over 0.5 FT)" for _, r in chunk.iterrows()])
                        })

                    # Download Excel de Múltiplas
                    try:
                        import io
                        df_bilhetes_export = pd.DataFrame(bilhetes_list)
                        buffer_m = io.BytesIO()
                        with pd.ExcelWriter(buffer_m, engine='openpyxl') as writer:
                            df_bilhetes_export.to_excel(writer, index=False, sheet_name='Multiplas_Over05')
                        excel_data_m = buffer_m.getvalue()

                        st.download_button(
                            label="📥 Baixar Planilha de Múltiplas Over 0.5 FT (Excel)",
                            data=excel_data_m,
                            file_name=f"multiplas_over05_{date_str}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="btn_download_page12_excel"
                        )
                    except Exception as ex_m:
                        st.warning(f"Aviso de download Excel: {ex_m}")

                with tab2:
                    display_cols = [
                        'Date', 'Time', 'League', 'Home', 'Away', 
                        'Total_xG', 'Odd_D_FT', 'odd_over05', 'Reason'
                    ]

                    for col in display_cols:
                        if col not in df_aprovados.columns:
                            df_aprovados[col] = ""

                    df_display = df_aprovados[display_cols].copy()
                    df_display.columns = [
                        'Data', 'Horário', 'Liga', 'Mandante', 'Visitante', 
                        'xG Total', 'Odd Empate', 'Odd Over 0.5 FT', 'Status'
                    ]

                    st.dataframe(df_display, use_container_width=True)
