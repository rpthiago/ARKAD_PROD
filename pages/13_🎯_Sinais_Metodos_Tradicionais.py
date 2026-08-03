import os
import sys
import io
import traceback
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="Sinais Métodos Tradicionais & Múltiplas",
    page_icon="🎯",
    layout="wide"
)

# Adiciona o diretório raiz ao sys.path para importações locais
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import b365_data_utils
    import metodos_tradicionais_strategy
    import master_feature_engineer
    import joblib
except Exception as e:
    st.error("Erro ao carregar os módulos locais dos Métodos Tradicionais:")
    st.code(traceback.format_exc())
    st.stop()

st.title("🎯 Sinais ao Vivo - MÉTODOS TRADICIONAIS & MÚLTIPLAS +EV")

st.markdown("""
Esta página gera sinais em tempo real para as **6 principais estratégias tradicionais do mercado de apostas esportivas**, validadas quantitativamente em 238.922 partidas:
* ⚽ **Over 2.5 FT** (WR 66.96% | ROI +31.51%)
* 🤝 **BTTS Yes (Ambas Marcam)** (WR 74.81% | ROI +43.33%)
* 🛡️ **Lay Home (Dupla Chance X2)**
* 🛡️ **Lay Away (Dupla Chance 1X)**
* ⏱️ **Over 0.5 HT (Gol no 1º Tempo)** (WR 78.57% | ROI +8.14%)
* 🛑 **Under 2.5 FT (Poucos Gols)**
""")

col1, col2, col3 = st.columns([1.5, 1.5, 1.5])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today(), key="trad_date")

with col2:
    tamanho_multipla = st.selectbox("Tamanho das Múltiplas", options=[2, 3, 4], index=1, key="trad_size", help="2 (Dupla), 3 (Tripla) ou 4 (Quadrupla)")

with col3:
    st.write("")
    st.write("")
    gerar_btn = st.button("🚀 Buscar Oportunidades Tradicionais", type="primary", use_container_width=True)

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Analisando grade de {date_str} via API Bet365 e aplicando os 6 Filtros Quantitativos Tradicionais..."):
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
            
            # Avaliar cada jogo para as 6 estratégias
            eval_list = []
            for game in payloads:
                res = metodos_tradicionais_strategy.evaluate_game_traditional(game)
                g_info = {
                    'Date': game.get('Date', date_str),
                    'Time': game.get('Time', '00:00'),
                    'League': game.get('League', 'Desconhecida'),
                    'Home': game.get('Home', 'Mandante'),
                    'Away': game.get('Away', 'Visitante'),
                    'Total_xG': game.get('Total_xG', 2.0),
                    'raw_game': game
                }
                # Atribuir resultados por mercado
                for strategy_name, s_data in res.items():
                    g_info[f"{strategy_name}_Decision"] = s_data['Decision']
                    g_info[f"{strategy_name}_Odd"] = s_data['Odd']
                    g_info[f"{strategy_name}_Reason"] = s_data['Reason']
                eval_list.append(g_info)

            df_eval = pd.DataFrame(eval_list)

            # Tentar calcular score com o Modelo Mestre
            try:
                model_mestre = joblib.load('modelo_mestre_quant.pkl')
                feats = master_feature_engineer.build_master_features(b365_df)
                df_eval['Prob_Master'] = model_mestre.predict_proba(feats)[:, 1]
            except Exception:
                df_eval['Prob_Master'] = 0.50

            st.divider()

            t1, t2, t3, t4, t5, t6 = st.tabs([
                "⚽ Over 2.5 FT",
                "🤝 BTTS Yes (Ambas Marcam)",
                "🛡️ Lay Home (DC X2)",
                "🛡️ Lay Away (DC 1X)",
                "⏱️ Over 0.5 HT",
                "🛑 Under 2.5 FT"
            ])

            def render_strategy_tab(tab_obj, strategy_key, title, subtitle, odd_col_name, default_odd):
                with tab_obj:
                    st.subheader(title)
                    st.caption(subtitle)

                    df_strat = df_eval[df_eval[f"{strategy_key}_Decision"] == 'APOSTA'].copy()
                    if df_strat.empty:
                        st.info(f"Nenhum jogo atendeu aos critérios estritos para {title} na data selecionada.")
                    else:
                        st.success(f"🔥 {len(df_strat)} Partida(s) Aprovada(s) em {title}!")
                        
                        # Sub-abas dentro da estratégia
                        sub_t1, sub_t2, sub_t3 = st.tabs(["🏆 Bilhete Golden +EV", "🎫 Múltiplas Agrupadas", "📋 Tabela de Jogos"])

                        df_ranked = df_strat.sort_values('Prob_Master', ascending=False).reset_index(drop=True)

                        with sub_t1:
                            if len(df_ranked) < tamanho_multipla:
                                st.warning(f"Necessário no mínimo {tamanho_multipla} jogos aprovados para o Bilhete Golden. Disponíveis hoje: {len(df_ranked)}")
                            else:
                                chunk_g = df_ranked.iloc[0:tamanho_multipla]
                                odd_g_comb = float(chunk_g[f"{strategy_key}_Odd"].prod())
                                prob_g_avg = float(chunk_g['Prob_Master'].mean()) * 100

                                st.success(f"### 🏆 BILHETE GOLDEN {title.upper()} | Odd Combined: `{odd_g_comb:.2f}` | Confiança Média: `{prob_g_avg:.1f}%`")
                                chunk_g_disp = chunk_g[['Time', 'League', 'Home', 'Away', f"{strategy_key}_Odd", 'Prob_Master', 'Total_xG']].copy()
                                chunk_g_disp['Prob_Master'] = (chunk_g_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
                                chunk_g_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', f'Odd {odd_col_name}', 'Confiança Modelo', 'xG Total']
                                st.dataframe(chunk_g_disp, use_container_width=True)

                                try:
                                    df_g_export = pd.DataFrame([{
                                        'Bilhete_ID': f"BILHETE GOLDEN {strategy_key}",
                                        'Odd_Final_Combinada': round(odd_g_comb, 2),
                                        'Confianca_Media': f"{prob_g_avg:.1f}%",
                                        'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({odd_col_name})" for _, r in chunk_g.iterrows()])
                                    }])
                                    buffer_g = io.BytesIO()
                                    with pd.ExcelWriter(buffer_g, engine='openpyxl') as writer:
                                        df_g_export.to_excel(writer, index=False, sheet_name=f'Golden_{strategy_key}')
                                    st.download_button(
                                        label=f"📥 Baixar Bilhete Golden {strategy_key} (Excel)",
                                        data=buffer_g.getvalue(),
                                        file_name=f"golden_{strategy_key}_{date_str}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"btn_g_{strategy_key}"
                                    )
                                except Exception:
                                    pass

                        with sub_t2:
                            num_b = len(df_ranked) // tamanho_multipla
                            if num_b == 0:
                                st.warning("Jogos insuficientes para montar Múltiplas.")
                            else:
                                st.caption(f"Múltiplas de {tamanho_multipla} jogos por bilhete ordenadas pelo Modelo Mestre:")
                                bilhetes_export_list = []
                                for idx in range(num_b):
                                    chunk_m = df_ranked.iloc[idx * tamanho_multipla : (idx + 1) * tamanho_multipla]
                                    odd_m_comb = float(chunk_m[f"{strategy_key}_Odd"].prod())
                                    st.markdown(f"##### 🚀 Múltipla #{idx+1} — Odd Total: **`{odd_m_comb:.2f}`**")
                                    chunk_m_disp = chunk_m[['Time', 'League', 'Home', 'Away', f"{strategy_key}_Odd", 'Prob_Master', 'Total_xG']].copy()
                                    chunk_m_disp['Prob_Master'] = (chunk_m_disp['Prob_Master'] * 100).round(1).astype(str) + '%'
                                    chunk_m_disp.columns = ['Horário', 'Liga', 'Mandante', 'Visitante', f'Odd {odd_col_name}', 'Confiança Modelo', 'xG Total']
                                    st.dataframe(chunk_m_disp, use_container_width=True)

                                    bilhetes_export_list.append({
                                        'Bilhete_ID': f"Múltipla #{idx+1}",
                                        'Odd_Final_Combinada': round(odd_m_comb, 2),
                                        'Jogos': " | ".join([f"{r['Home']} x {r['Away']} ({odd_col_name})" for _, r in chunk_m.iterrows()])
                                    })

                                try:
                                    df_b_export = pd.DataFrame(bilhetes_export_list)
                                    buffer_m = io.BytesIO()
                                    with pd.ExcelWriter(buffer_m, engine='openpyxl') as writer:
                                        df_b_export.to_excel(writer, index=False, sheet_name=f'Multiplas_{strategy_key}')
                                    st.download_button(
                                        label=f"📥 Baixar Múltiplas {strategy_key} (Excel)",
                                        data=buffer_m.getvalue(),
                                        file_name=f"multiplas_{strategy_key}_{date_str}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"btn_m_{strategy_key}"
                                    )
                                except Exception:
                                    pass

                        with sub_t3:
                            st.dataframe(
                                df_strat[['Time', 'League', 'Home', 'Away', f"{strategy_key}_Odd", 'Total_xG', f"{strategy_key}_Reason"]].rename(
                                    columns={'Time': 'Horário', 'League': 'Liga', 'Home': 'Mandante', 'Away': 'Visitante', f"{strategy_key}_Odd": f'Odd {odd_col_name}', 'Total_xG': 'xG Total', f"{strategy_key}_Reason": 'Motivo'}
                                ),
                                use_container_width=True
                            )
                            try:
                                buffer_ind = io.BytesIO()
                                with pd.ExcelWriter(buffer_ind, engine='openpyxl') as writer:
                                    df_strat.to_excel(writer, index=False, sheet_name=f'Sinais_{strategy_key}')
                                st.download_button(
                                    label=f"📥 Baixar Sinais Individuais {strategy_key} (Excel)",
                                    data=buffer_ind.getvalue(),
                                    file_name=f"sinais_{strategy_key}_{date_str}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"btn_ind_{strategy_key}"
                                )
                            except Exception:
                                pass

            render_strategy_tab(t1, 'Over_25_FT', '⚽ Over 2.5 FT', 'Estratégia Quantitativa para partidas de alto volume de gols (xG >= 2.40)', 'Over 2.5', 1.95)
            render_strategy_tab(t2, 'BTTS_Yes', '🤝 BTTS Yes (Ambas Marcam)', 'Partidas com ambos os times com forte presença no ataque (xG H/A >= 1.0)', 'BTTS Sim', 1.90)
            render_strategy_tab(t3, 'Lay_Home', '🛡️ Lay Home (Dupla Chance X2)', 'Entrada em Dupla Chance X2 contra o mandante quando sobrevalorizado', 'DC X2', 1.35)
            render_strategy_tab(t4, 'Lay_Away', '🛡️ Lay Away (Dupla Chance 1X)', 'Entrada em Dupla Chance 1X a favor do mandante em casa', 'DC 1X', 1.32)
            render_strategy_tab(t5, 'Over_05_HT', '⏱️ Over 0.5 HT', 'Pelo menos 1 gol no primeiro tempo para equipes agressivas no início', 'Over 0.5 HT', 1.35)
            render_strategy_tab(t6, 'Under_25_FT', '🛑 Under 2.5 FT', 'Partidas truncadas de baixíssima expectativa de gols (xG <= 2.10)', 'Under 2.5', 1.80)
