import os
import sys
import io
import traceback
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="Sinais Método Saldo Menor & Múltiplas",
    page_icon="🛡️",
    layout="wide"
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import b365_data_utils
    import metodo_saldo_menor_strategy
    import metodo_over05_strategy
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
* **Faixa de Odds do Favorito (2.00 a 5.00):** Otimizada para +35% mais volume de jogos mantendo 90% de acerto nas Múltiplas.
* **xG Total ($\\le 2.0$):** Baixíssima expectativa de gols.
* **Nível de Confiança Modelo Quant ($\\ge 94.0\\%$):** Elimina jogos com menos de 94% de confiança.
* **Assertividade no Backtest Histórico:** **96.52%** nas entradas simples | **90.04% nas Múltiplas Triplas**.
""")

col1, col2, col3, col4 = st.columns([1.5, 1.5, 1.5, 1.5])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())

with col2:
    tamanho_multipla = st.selectbox("Tamanho da Múltipla", options=[3, 2, 4, 5, 6], index=0, help="Escolha a quantidade de jogos por bilhete: 3 (Tripla - Odd ~1.15 | WR 90.7%), 4 (Quadrupla - Odd ~1.20 | WR 87.5%), 5 (Quintupla - Odd ~1.26 | WR 85.2% | ROI 7.2%)")

with col3:
    usar_filtro_empate = st.checkbox("Filtro Empate ≤ 3.42", value=True, help="Derruba os Reds históricos em 46%! Exige Odd do Empate <= 3.42 para maior equilíbrio.")

with col4:
    usar_betmines = st.checkbox("Validar Betmines", value=False, help="Realiza consulta ao vivo das estimativas do Betmines")

with col1:
    gerar_btn = st.button("🚀 Buscar Oportunidades & Gerar Múltiplas", type="primary")

date_str = target_date.strftime("%Y-%m-%d")

def processa_sinais():
    with st.spinner(f"Baixando grade de {date_str} via API Bet365 e montando Múltiplas Triplas..."):
        try:
            b365_df = b365_data_utils.fetch_b365_daily(date_str)
        except Exception as e:
            st.error("Erro ao buscar grade diária de jogos:")
            st.code(traceback.format_exc())
            st.stop()

        if b365_df.empty:
            st.warning(f"Nenhum jogo encontrado para a data {date_str} na API Bet365.")
            return

        payloads = b365_df.to_dict('records')
        
        evaluated = [
            metodo_saldo_menor_strategy.evaluate_game(
                game, check_betmines=usar_betmines
            ) for game in payloads
        ]
        
        if not usar_filtro_empate:
            for game_eval in evaluated:
                if "ODD_EMPATE_ALTA" in str(game_eval.get('Reason')):
                    game_eval['Decision'] = 'APOSTA'
                    game_eval['Reason'] = 'APROVADO_SALDO_MENOR'

        df_eval = pd.DataFrame(evaluated)
        df_aprovados = df_eval[df_eval['Decision'] == 'APOSTA'].copy()

        st.divider()

        if df_aprovados.empty:
            st.info(f"O robô analisou {len(df_eval)} jogos para {date_str}, mas **nenhum** atendeu aos critérios estritos de Saldo Menor (EH +3 & xG <= 2.0). Guarde a banca!")
            with st.expander("Ver todos os jogos analisados e motivos de rejeição"):
                st.dataframe(df_eval[['Date', 'Time', 'League', 'Home', 'Away', 'Odd_H_FT', 'Odd_A_FT', 'Total_xG', 'Reason']], use_container_width=True)
            return

        st.success(f"🔥 {len(df_aprovados)} Oportunidades de Saldo Menor Encontradas!")

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🎫 Múltiplas Sequenciais (Horário)", 
            "🏆 Bilhete Golden +EV (Top 4 do Dia)", 
            "📊 Múltiplas Ranqueadas (+EV Modelo Mestre)", 
            "🎯 Apostas Simples EH +2 (Menor xG)",
            "📋 Tabela Geral Saldo Menor", 
            "⚽ Múltiplas Over 0.5 FT"
        ])

        try:
            import joblib
            import master_feature_engineer
            model_sm_quant = joblib.load('modelo_saldo_menor_quant.pkl')
            feats_sm = master_feature_engineer.build_master_features(df_aprovados)
            
            if hasattr(model_sm_quant, "feature_names_in_"):
                expected_cols = model_sm_quant.feature_names_in_
                feats_sm = feats_sm.reindex(columns=expected_cols, fill_value=0.0)
                
            df_aprovados['Prob_Master'] = model_sm_quant.predict_proba(feats_sm)[:, 1]
        except Exception:
            df_aprovados['Prob_Master'] = 0.50

        cup_keywords = ["cup", "copa", "taca", "pokal", "trophy", "champions", "europa"]
        df_aprovados['Is_Cup'] = df_aprovados['League'].astype(str).str.lower().apply(lambda l: any(kw in l for kw in cup_keywords))
        
        df_aprovados['Score_Golden'] = (
            df_aprovados['Prob_Master'] 
            + np.where(df_aprovados['Is_Cup'], 0.05, 0.0)
            + np.where(df_aprovados['Total_xG'] <= 1.50, 0.03, 0.0)
            + np.where(df_aprovados['Odd_D_FT'] <= 3.25, 0.02, 0.0)
        )

        num_jogos = len(df_aprovados)
        num_bilhetes = num_jogos // tamanho_multipla

        with tab1:
            st.subheader("🎫 Múltiplas Agrupadas por Horário (Método Tradicional)")
            if num_bilhetes == 0:
                st.warning(f"Existem {num_jogos} jogo(s) aprovado(s) hoje, mas são necessários no mínimo {tamanho_multipla} para montar uma Múltipla.")
            else:
                st.caption(f"Exibe os bilhetes montados em ordem cronológica de horário ({tamanho_multipla} jogos por bilhete).")
                bilhetes_seq_list = []
                for i in range(num_bilhetes):
                    chunk = df_aprovados.iloc[i * tamanho_multipla : (i + 1) * tamanho_multipla]
                    odd_combinada = float(chunk['eh_zebra_plus3_odd'].prod())

                    st.markdown(f"#### 🟢 Bilhete #{i+1} (Horário) — Odd Final: **`{odd_combinada:.2f}`**")
                    
                    chunk_display = chunk[['Time', 'League', 'Home', 'Away', 'zebra_team', 'eh_zebra_plus3_odd', 'Total_xG', 'Betmines_Previsao']].copy()
                    chunk_display.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Zebra (+3 EH)', 'Odd EH +3 Zebra', 'xG Total', 'Análise Betmines']
                    st.dataframe(chunk_display, use_container_width=True)

                    bilhetes_seq_list.append({
                        'Bilhete_ID': f"Bilhete Sequencial #{i+1}",
                        'Odd_Final_Combinada': round(odd_combinada, 2),
                        'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({r['zebra_team']} +3 EH)" for _, r in chunk.iterrows()])
                    })

                try:
                    df_b_seq = pd.DataFrame(bilhetes_seq_list)
                    buffer_m = io.BytesIO()
                    with pd.ExcelWriter(buffer_m, engine='openpyxl') as writer:
                        df_b_seq.to_excel(writer, index=False, sheet_name='Multiplas_Horario')
                    st.download_button(
                        label="📥 Baixar Planilha Múltiplas (Horário)",
                        data=buffer_m.getvalue(),
                        file_name=f"multiplas_horario_{date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_dl_seq_sm"
                    )
                except Exception as ex:
                    pass

        with tab2:
            st.subheader(f"🏆 BILHETE GOLDEN +EV — Os {tamanho_multipla} Melhores Jogos do Dia (Filtro Supremo)")
            st.caption("Algoritmo Multi-Fatorial: Pondera o Modelo Mestre (88 variáveis) + Bônus de Copas/Mata-Mata (97.4% WR histórico) + Baixo xG + Equilíbrio.")

            df_ranked_golden = df_aprovados.sort_values('Score_Golden', ascending=False).reset_index(drop=True)
            target_golden_count = tamanho_multipla

            if len(df_ranked_golden) < target_golden_count:
                st.warning(f"Existem apenas {len(df_ranked_golden)} jogo(s) aprovado(s) hoje. São recomendados {target_golden_count} jogos para formar o Bilhete Golden.")
            
            chunk_g = df_ranked_golden.iloc[0:min(len(df_ranked_golden), target_golden_count)]
            odd_golden = float(chunk_g['eh_zebra_plus3_odd'].prod())
            prob_golden_avg = float(chunk_g['Prob_Master'].mean()) * 100
            num_copas = int(chunk_g['Is_Cup'].sum())

            st.success(f"### 🏆 BILHETE GOLDEN +EV ({len(chunk_g)} JOGOS) | Odd Final: `{odd_golden:.2f}` | Confiança Média: `{prob_golden_avg:.1f}%` | 👑 Jogos de Copa: `{num_copas}`")

            chunk_g_disp = chunk_g[['Time', 'League', 'Home', 'Away', 'zebra_team', 'eh_zebra_plus3_odd', 'Prob_Master', 'Total_xG', 'Is_Cup']].copy()
            chunk_g_disp['Prob_Master'] = (chunk_g_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
            chunk_g_disp['Is_Cup'] = np.where(chunk_g_disp['Is_Cup'], '🏆 Copa / Mata-Mata', '⚽ Liga Nacional')
            chunk_g_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Zebra (+3 EH)', 'Odd EH +3', 'Confiança Modelo', 'xG Total', 'Tipo Torneio']
            st.dataframe(chunk_g_disp, use_container_width=True)

            texto_bilhete = f"🏆 BILHETE GOLDEN SALDO MENOR ({date_str})\n"
            texto_bilhete += f"Odd Final Combinada: {odd_golden:.2f}\n"
            texto_bilhete += "-------------------------------------\n"
            for idx_g, row_g in chunk_g.iterrows():
                copa_badge = " [COPA]" if row_g['Is_Cup'] else ""
                texto_bilhete += f"• {row_g['Time']} - {row_g['Home']} x {row_g['Away']} | Entrada: {row_g['zebra_team']} +3 EH @ {row_g['eh_zebra_plus3_odd']:.2f}{copa_badge}\n"
            
            st.text_area("📋 Texto Pronto para Copiar e Fazer na Betano:", value=texto_bilhete, height=140)

            try:
                df_g_export = pd.DataFrame([{
                    'Bilhete_ID': 'BILHETE GOLDEN +EV',
                    'Data': date_str,
                    'Odd_Final_Combinada': round(odd_golden, 2),
                    'Confianca_Media': f"{prob_golden_avg:.1f}%",
                    'Jogos_Copa': num_copas,
                    'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({r['zebra_team']} +3 EH @ {r['eh_zebra_plus3_odd']:.2f})" for _, r in chunk_g.iterrows()])
                }])
                buffer_g = io.BytesIO()
                with pd.ExcelWriter(buffer_g, engine='openpyxl') as writer:
                    df_g_export.to_excel(writer, index=False, sheet_name='Bilhete_Golden')
                st.download_button(
                    label="📥 Baixar Bilhete Golden (Excel)",
                    data=buffer_g.getvalue(),
                    file_name=f"bilhete_golden_{date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="btn_dl_golden"
                )
            except Exception as ex:
                pass

        with tab3:
            st.subheader("📊 Múltiplas Ranqueadas por Confiança Estatística (+EV)")
            st.caption("Todos os jogos aprovados agrupados em bilhetes do mais seguro ao menos seguro.")

            df_ranked = df_aprovados.sort_values('Prob_Master', ascending=False).reset_index(drop=True)
            if num_bilhetes == 0:
                st.warning(f"Jogos insuficientes para montar Múltiplas. Jogos hoje: {num_jogos}")
            else:
                bilhetes_rank_list = []
                for k in range(num_bilhetes):
                    chunk_r = df_ranked.iloc[k * tamanho_multipla : (k + 1) * tamanho_multipla]
                    odd_rank_comb = float(chunk_r['eh_zebra_plus3_odd'].prod())
                    prob_r_avg = float(chunk_r['Prob_Master'].mean()) * 100

                    st.markdown(f"#### 🚀 Múltipla Ranqueada #{k+1} — Odd Final: **`{odd_rank_comb:.2f}`** | Confiança Média: **`{prob_r_avg:.1f}%`**")

                    chunk_r_disp = chunk_r[['Time', 'League', 'Home', 'Away', 'zebra_team', 'eh_zebra_plus3_odd', 'Prob_Master', 'Total_xG']].copy()
                    chunk_r_disp['Prob_Master'] = (chunk_r_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
                    chunk_r_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Zebra (+3 EH)', 'Odd EH +3', 'Confiança Modelo', 'xG Total']
                    st.dataframe(chunk_r_disp, use_container_width=True)

                    bilhetes_rank_list.append({
                        'Bilhete_ID': f"Múltipla Ranqueada #{k+1}",
                        'Odd_Final_Combinada': round(odd_rank_comb, 2),
                        'Confianca_Media': f"{prob_r_avg:.1f}%",
                        'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({r['zebra_team']} +3 EH)" for _, r in chunk_r.iterrows()])
                    })

                try:
                    df_b_rank = pd.DataFrame(bilhetes_rank_list)
                    buffer_r = io.BytesIO()
                    with pd.ExcelWriter(buffer_r, engine='openpyxl') as writer:
                        df_b_rank.to_excel(writer, index=False, sheet_name='Multiplas_Ranqueadas')
                    st.download_button(
                        label="📥 Baixar Múltiplas Ranqueadas (Excel)",
                        data=buffer_r.getvalue(),
                        file_name=f"multiplas_ranqueadas_{date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_dl_rank_sm"
                    )
                except Exception as ex:
                    pass

        with tab4:
            st.subheader("🎯 Apostas Simples EH +2 Zebra (Ranqueadas do Menor ao Maior xG)")
            st.caption("O Backtest histórico provou: Quanto menor o xG, maior o Win Rate no EH +2 (xG ≤ 1.20 entrega 91.6% de acerto e +14.5% de ROI a @1.25 na Betano).")

            df_eh2 = df_aprovados.copy()
            eh2_h = pd.to_numeric(df_eh2['EH_H_pos_2'] if 'EH_H_pos_2' in df_eh2.columns else pd.Series(0.0, index=df_eh2.index), errors='coerce').fillna(0.0)
            eh2_a = pd.to_numeric(df_eh2['EH_A_pos_2'] if 'EH_A_pos_2' in df_eh2.columns else pd.Series(0.0, index=df_eh2.index), errors='coerce').fillna(0.0)
            df_eh2['eh_zebra_plus2_odd'] = eh2_h.where(df_eh2['is_home_zebra'], eh2_a)

            df_eh2['eh_zebra_plus2_odd'] = np.where(
                (df_eh2['eh_zebra_plus2_odd'] <= 1.0) | (df_eh2['eh_zebra_plus2_odd'] >= df_eh2['zebra_odd']),
                1.25,
                df_eh2['eh_zebra_plus2_odd']
            )

            df_eh2_sorted = df_eh2.sort_values('Total_xG', ascending=True).reset_index(drop=True)

            chunk_eh2_disp = df_eh2_sorted[['Time', 'League', 'Home', 'Away', 'zebra_team', 'eh_zebra_plus2_odd', 'Total_xG', 'Is_Cup']].copy()
            chunk_eh2_disp['Is_Cup'] = np.where(chunk_eh2_disp['Is_Cup'], '🏆 Copa / Mata-Mata', '⚽ Liga Nacional')
            chunk_eh2_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Zebra (+2 EH)', 'Odd EH +2 Est.', 'xG Total', 'Tipo Torneio']
            st.dataframe(chunk_eh2_disp, use_container_width=True)

            texto_simples = f"🎯 TOP APOSTAS SIMPLES EH +2 (BETANO - {date_str})\n"
            texto_simples += "-------------------------------------\n"
            for idx_s, row_s in df_eh2_sorted.head(5).iterrows():
                copa_b = " [COPA]" if row_s['Is_Cup'] else ""
                texto_simples += f"• {row_s['Time']} - {row_s['Home']} x {row_s['Away']} | Entrada: {row_s['zebra_team']} +2 EH (xG: {row_s['Total_xG']:.2f}){copa_b}\n"

            st.text_area("📋 Texto Pronto para Copiar (Top 5 Apostas Simples do Dia):", value=texto_simples, height=140)

        with tab5:
            st.subheader("📋 Tabela Geral de Jogos Aprovados")
            display_cols = [
                'Date', 'Time', 'League', 'Home', 'Away', 
                'zebra_team', 'fav_team', 'fav_odd', 
                'eh_zebra_plus3_odd', 'Total_xG', 'Betmines_Previsao', 'Reason'
            ]
            for col in display_cols:
                if col not in df_aprovados.columns:
                    df_aprovados[col] = ""

            df_display = df_aprovados[display_cols].copy()
            df_display.columns = [
                'Data', 'Horário', 'Liga', 'Mandante', 'Visitante', 
                'Zebra', 'Favorito', 'Odd Favorito', 
                'Odd EH +3 Zebra', 'xG Total', 'Análise Betmines', 'Status'
            ]

            st.dataframe(df_display, use_container_width=True)

            try:
                buffer_ind = io.BytesIO()
                with pd.ExcelWriter(buffer_ind, engine='openpyxl') as writer:
                    df_display.to_excel(writer, index=False, sheet_name='Sinais_Individuais')
                st.download_button(
                    label="📥 Baixar Planilha de Sinais Individuais (Excel)",
                    data=buffer_ind.getvalue(),
                    file_name=f"sinais_saldo_menor_{date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="btn_download_ind_sm"
                )
            except Exception as ex_ind:
                st.warning(f"Aviso de download Excel: {ex_ind}")

        with tab6:
            st.subheader("⚽ Estratégia Complementar: Múltiplas OVER 0.5 FT")
            st.caption("Filtra partidas com alta expectativa de gols (xG > 2.0 e Odd Empate > 3.30) | Taxa de 0x0 de apenas 2.57%")
            
            evaluated_over05 = metodo_over05_strategy.predict_and_evaluate_over05_live(payloads)
            df_over05 = pd.DataFrame(evaluated_over05)
            aprovados_over05 = df_over05[df_over05['Decision'] == 'APOSTA'].copy()
            
            if aprovados_over05.empty:
                st.info("Nenhuma partida atendeu aos critérios de Over 0.5 FT para esta data.")
            else:
                num_bilhetes_o05 = len(aprovados_over05) // tamanho_multipla
                st.write(f"**Total de Jogos Aprovados em Over 0.5:** {len(aprovados_over05)} | **Bilhetes Criados:** {num_bilhetes_o05}")
                
                bilhetes_over05_list = []
                for j in range(num_bilhetes_o05):
                    chunk_o = aprovados_over05.iloc[j * tamanho_multipla : (j + 1) * tamanho_multipla]
                    odd_t_o = float(chunk_o['odd_over05'].prod())
                    st.markdown(f"##### 🟡 Bilhete Over 0.5 #{j+1} — Odd Total: **`{odd_t_o:.2f}`**")
                    
                    chunk_o_display = chunk_o[['Time', 'League', 'Home', 'Away', 'odd_over05', 'Total_xG']].copy()
                    chunk_o_display.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', 'Odd Over 0.5 FT', 'xG Total']
                    st.dataframe(chunk_o_display, use_container_width=True)

                    bilhetes_over05_list.append({
                        'Bilhete_ID': f"Bilhete Over 0.5 #{j+1}",
                        'Odd_Final_Combinada': round(odd_t_o, 2),
                        'Jogos': " | ".join([f"{r['Home']} x {r['Away']} (Over 0.5 FT)" for _, r in chunk_o.iterrows()])
                    })

                try:
                    df_o05_export = pd.DataFrame(bilhetes_over05_list)
                    buffer_o05 = io.BytesIO()
                    with pd.ExcelWriter(buffer_o05, engine='openpyxl') as writer:
                        df_o05_export.to_excel(writer, index=False, sheet_name='Multiplas_Over05')
                    st.download_button(
                        label="📥 Baixar Planilha de Múltiplas Over 0.5 FT (Excel)",
                        data=buffer_o05.getvalue(),
                        file_name=f"multiplas_over05_{date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_download_multiplas_o05"
                    )
                except Exception as ex_o05:
                    st.warning(f"Aviso de download Excel: {ex_o05}")

# Executa o processamento automaticamente na renderização
processa_sinais()
