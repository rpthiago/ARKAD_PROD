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

> ⚠️ **ALERTA QUANTITATIVO (MÚLTIPLAS PRE-LIVE):** Apostar em Over 0.5 FT antes do jogo começar (com odds entre 1.02 e 1.06) é matematicamente **-EV (Prejuízo no longo prazo)** devido à margem abusiva (*overround*) das casas. 
>
> ### 🚀 A Solução: Operação em LIVE (Ao Vivo)
> Em vez de entrar pré-jogo, utilize as partidas filtradas nesta página para fazer a entrada **Live (Ao Vivo)**:
> 1. Aguarde o jogo começar em 0x0.
> 2. Entre no mercado de **Over 0.5 FT** (ou Lay 0x0) apenas quando a odd atingir o **limiar mínimo de 1.20** (geralmente entre o minuto 15 e 22 da partida).
> 3. Isso aumenta o seu ganho de $4\%$ para $20\%$ por acerto, garantindo valor esperado positivo real (+EV).
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

# Carregar automaticamente na abertura da página
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
                tab1, tab2, tab3, tab4 = st.tabs([
                    "🎫 Múltiplas Sequenciais (Horário)", 
                    "🏆 Bilhete Golden +EV (Top 3 do Dia)", 
                    "📊 Múltiplas Ranqueadas (+EV Modelo Mestre)", 
                    "📋 Tabela Geral Over 0.5 FT"
                ])

                # Tentar carregar predições do Modelo Mestre de Gols (modelo_mestre_quant.pkl)
                try:
                    import joblib
                    import master_feature_engineer
                    model_mestre = joblib.load('modelo_mestre_quant.pkl')
                    feats_o05 = master_feature_engineer.build_master_features(df_aprovados)
                    df_aprovados['Prob_Master'] = model_mestre.predict_proba(feats_o05)[:, 1]
                except Exception:
                    df_aprovados['Prob_Master'] = 0.50

                # ABA 1: SEQUENCIAL POR HORÁRIO
                with tab1:
                    st.subheader("🎫 Múltiplas Agrupadas por Horário (Método Tradicional)")
                    if num_bilhetes == 0:
                        st.warning(f"Existem {total_aprovados} jogo(s) aprovado(s) hoje, mas são necessários no mínimo {tamanho_multipla} para montar uma Múltipla.")
                    else:
                        st.caption(f"Exibe os bilhetes montados em ordem cronológica de horário ({tamanho_multipla} jogos por bilhete).")
                        bilhetes_seq_list = []
                        for i in range(num_bilhetes):
                            chunk = df_aprovados.iloc[i * tamanho_multipla : (i + 1) * tamanho_multipla]
                            odd_total = float(chunk['odd_over05'].prod())

                            st.markdown(f"#### 🟡 Bilhete #{i+1} (Horário) — Odd Total: **`{odd_total:.2f}`**")
                            
                            chunk_display = chunk[['Time', 'League', 'Home', 'Away', 'odd_over05', 'Total_xG']].copy()
                            chunk_display.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Odd Over 0.5 FT', 'xG Total']
                            st.dataframe(chunk_display, use_container_width=True)

                            bilhetes_seq_list.append({
                                'Bilhete_ID': f"Bilhete Over 0.5 Sequencial #{i+1}",
                                'Odd_Final_Combinada': round(odd_total, 2),
                                'Jogos': " | ".join([f"{r['Home']} x {r['Away']} (Over 0.5 FT)" for _, r in chunk.iterrows()])
                            })

                        # Download Excel Múltiplas Sequenciais
                        try:
                            import io
                            df_b_seq = pd.DataFrame(bilhetes_seq_list)
                            buffer_m = io.BytesIO()
                            with pd.ExcelWriter(buffer_m, engine='openpyxl') as writer:
                                df_b_seq.to_excel(writer, index=False, sheet_name='Multiplas_Over05_Horario')
                            st.download_button(
                                label="📥 Baixar Planilha Múltiplas (Horário)",
                                data=buffer_m.getvalue(),
                                file_name=f"multiplas_over05_horario_{date_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="btn_download_p12_seq"
                            )
                        except Exception as ex_m:
                            pass

                # ABA 2: BILHETE GOLDEN +EV (TOP 3 DO DIA)
                with tab2:
                    st.subheader("🏆 BILHETE GOLDEN +EV — Os 3 Melhores Jogos de Gols do Dia")
                    st.caption("O Modelo Mestre seleciona os 3 jogos com maior probabilidade matemática de gol no jogo.")

                    df_ranked_o05 = df_aprovados.sort_values('Prob_Master', ascending=False).reset_index(drop=True)
                    if len(df_ranked_o05) < 3:
                        st.warning(f"São necessários ao menos 3 jogos aprovados no dia para gerar o Bilhete Golden. Jogos hoje: {len(df_ranked_o05)}")
                    else:
                        chunk_g_o = df_ranked_o05.iloc[0:3]
                        odd_golden_o = float(chunk_g_o['odd_over05'].prod())
                        prob_golden_avg_o = float(chunk_g_o['Prob_Master'].mean()) * 100

                        st.success(f"### 🏆 BILHETE GOLDEN OVER 0.5 | Odd Final: `{odd_golden_o:.2f}` | Confiança Média: `{prob_golden_avg_o:.1f}%`")

                        chunk_g_o_disp = chunk_g_o[['Time', 'League', 'Home', 'Away', 'odd_over05', 'Prob_Master', 'Total_xG']].copy()
                        chunk_g_o_disp['Prob_Master'] = (chunk_g_o_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
                        chunk_g_o_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Odd Over 0.5 FT', 'Confiança Modelo', 'xG Total']
                        st.dataframe(chunk_g_o_disp, use_container_width=True)

                        try:
                            import io
                            df_g_export_o = pd.DataFrame([{
                                'Bilhete_ID': 'BILHETE GOLDEN OVER 0.5',
                                'Odd_Final_Combinada': round(odd_golden_o, 2),
                                'Confianca_Media': f"{prob_golden_avg_o:.1f}%",
                                'Jogos': " | ".join([f"{r['Home']} x {r['Away']} (Over 0.5 FT)" for _, r in chunk_g_o.iterrows()])
                            }])
                            buffer_g_o = io.BytesIO()
                            with pd.ExcelWriter(buffer_g_o, engine='openpyxl') as writer:
                                df_g_export_o.to_excel(writer, index=False, sheet_name='Golden_Over05')
                            st.download_button(
                                label="📥 Baixar Bilhete Golden Over 0.5 (Excel)",
                                data=buffer_g_o.getvalue(),
                                file_name=f"bilhete_golden_over05_{date_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="btn_dl_p12_golden"
                            )
                        except Exception as ex_g:
                            pass

                # ABA 3: MÚLTIPLAS RANQUEADAS (+EV MODELO MESTRE)
                with tab3:
                    st.subheader("📊 Múltiplas Ranqueadas por Confiança Estatística (+EV)")
                    st.caption("Todos os jogos aprovados agrupados em bilhetes ordenados da maior probabilidade para a menor.")

                    df_ranked_o05 = df_aprovados.sort_values('Prob_Master', ascending=False).reset_index(drop=True)
                    if num_bilhetes == 0:
                        st.warning(f"Jogos insuficientes para montar Múltiplas. Jogos hoje: {total_aprovados}")
                    else:
                        bilhetes_rank_o05_list = []
                        for k in range(num_bilhetes):
                            chunk_r_o = df_ranked_o05.iloc[k * tamanho_multipla : (k + 1) * tamanho_multipla]
                            odd_rank_comb_o = float(chunk_r_o['odd_over05'].prod())
                            prob_r_avg_o = float(chunk_r_o['Prob_Master'].mean()) * 100

                            st.markdown(f"#### 🚀 Múltipla Over 0.5 Ranqueada #{k+1} — Odd Final: **`{odd_rank_comb_o:.2f}`** | Confiança Média: **`{prob_r_avg_o:.1f}%`**")

                            chunk_r_o_disp = chunk_r_o[['Time', 'League', 'Home', 'Away', 'odd_over05', 'Prob_Master', 'Total_xG']].copy()
                            chunk_r_o_disp['Prob_Master'] = (chunk_r_o_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
                            chunk_r_o_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Odd Over 0.5 FT', 'Confiança Modelo', 'xG Total']
                            st.dataframe(chunk_r_o_disp, use_container_width=True)

                            bilhetes_rank_o05_list.append({
                                'Bilhete_ID': f"Múltipla Over 0.5 Ranqueada #{k+1}",
                                'Odd_Final_Combinada': round(odd_rank_comb_o, 2),
                                'Confianca_Media': f"{prob_r_avg_o:.1f}%",
                                'Jogos': " | ".join([f"{r['Home']} x {r['Away']} (Over 0.5 FT)" for _, r in chunk_r_o.iterrows()])
                            })

                        try:
                            import io
                            df_b_rank_o = pd.DataFrame(bilhetes_rank_o05_list)
                            buffer_r_o = io.BytesIO()
                            with pd.ExcelWriter(buffer_r_o, engine='openpyxl') as writer:
                                df_b_rank_o.to_excel(writer, index=False, sheet_name='Multiplas_Ranqueadas_Over05')
                            st.download_button(
                                label="📥 Baixar Múltiplas Over 0.5 Ranqueadas (Excel)",
                                data=buffer_r_o.getvalue(),
                                file_name=f"multiplas_over05_ranqueadas_{date_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="btn_dl_p12_rank"
                            )
                        except Exception as ex_r:
                            pass

                # ABA 4: TABELA GERAL & EXCEL INDIVIDUAL
                with tab4:
                    st.subheader("📋 Tabela Geral de Jogos Aprovados")
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

                    try:
                        import io
                        buffer_ind = io.BytesIO()
                        with pd.ExcelWriter(buffer_ind, engine='openpyxl') as writer:
                            df_display.to_excel(writer, index=False, sheet_name='Sinais_Over05')
                        st.download_button(
                            label="📥 Baixar Planilha de Sinais Individuais (Excel)",
                            data=buffer_ind.getvalue(),
                            file_name=f"sinais_over05_{date_str}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="btn_dl_p12_ind"
                        )
                    except Exception as ex_ind:
                        st.warning(f"Aviso de download Excel: {ex_ind}")

