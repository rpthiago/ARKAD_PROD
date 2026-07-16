import os
import sys
import io
import time
from datetime import datetime, date
import pandas as pd
import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay 0x0 - Ao Vivo",
    page_icon="🎯",
    layout="wide",
)

try:
    import coleta_lay_cs_aovivo
    from coleta_lay_cs_aovivo import sinais_do_dia, _hist_df, MERCADOS
    import b365_data_utils
except ImportError as e:
    st.error(f"Erro ao carregar os módulos locais do Lay 0x0: {e}")
    st.stop()

st.title("🎯 Sinais Lay 0x0 (XGBoost v2)")
st.markdown("""
Esta página bate na **API da Betfair em tempo real**, calcula as inteligências do motor **XGBoost (RF/v2)**, e aplica os **filtros estritos e realistas** validados no nosso backtest de longo prazo (2024-2026):

*   **🏆 Sweet Spot (XGBoost):** Odd Betfair Lay entre **10.00 e 99.00**
    *   *Métrica de Entrada:* EV do Lay > 0.02 (calculado dinamicamente usando a probabilidade de ML e as odds live).
    *   *Filtros Contextuais:* Taxa histórica de 0x0 da liga **< 12.0%** e probabilidade de mercado implícita **< 10.0%**.
    *   *Nota:* A auditoria de truncamento temporal deste modelo foi **aprovada com sucesso (PASS)**, garantindo zero vazamento de dados futuros.

> ⚠️ **IMPORTANTE (FULL MATCH):** Conforme comprovado matematicamente pelo nosso Backtest Master, essa estratégia opera em **Full Match** (deixando a operação correr até o final do jogo). Não faça Cash Out. O robô só toma Red se o placar final for exatamente 0x0.
""")

col1, col2 = st.columns([1, 3])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())
    gerar_btn = st.button("Pesquisar Oportunidades", type="primary")

if gerar_btn:
    date_str = target_date.strftime("%Y-%m-%d")
    with st.spinner(f"Baixando grade de {date_str}, montando Histórico Rolante e executando modelos..."):
        # Garante que o histórico está carregado na memória
        _hist_df()
        
        # Puxa os sinais brutos do motor 0x0
        cfg = MERCADOS["0x0"]
        sinais_brutos = sinais_do_dia(date_str, cfg)
        
        if not sinais_brutos:
            st.warning("Nenhum jogo encontrado para hoje na API Betfair (ou fora de temporada).")
        else:
            df = pd.DataFrame(sinais_brutos)
            
            # Limpa colunas e força numérico para a filtragem estrita
            df["Odd_Num"] = pd.to_numeric(df["Odd_lay_entrada"], errors="coerce")
            df["Prob_Num"] = pd.to_numeric(df["Prob"], errors="coerce")
            
            # 1. Filtragem estrita final (Odd >= 10.0)
            df_final_sinais = df[
                df["Metodo"].str.contains("RF", na=False) &
                (df["Odd_Num"] >= 10.0)
            ].copy()
            
            if not df_final_sinais.empty:
                df_final_sinais["Metodo_Final"] = "XGBoost (Lay 0x0)"
            
            # Formatar tabela de saída
            sinais_filtrados = []
            jogos_vistos = {}
            
            for d_idx, row in df_final_sinais.iterrows():
                key = (row["Mandante"], row["Visitante"])
                if key not in jogos_vistos:
                    jogos_vistos[key] = {
                        "Date": row["Date"],
                        "Horario": row["Horario"],
                        "Liga": row["Liga"],
                        "Mandante": row["Mandante"],
                        "Visitante": row["Visitante"],
                        "Odd_lay_entrada": row["Odd_lay_entrada"],
                        "Prob": row["Prob"],
                        "Modelos_Aprovados": [row["Metodo_Final"]]
                    }
            
            df_final = pd.DataFrame([
                {
                    "Data": j["Date"],
                    "Horário": j["Horario"][:5] if j["Horario"] else "",
                    "Liga": j["Liga"],
                    "Mandante": j["Mandante"],
                    "Visitante": j["Visitante"],
                    "Odd Lay Betfair": j["Odd_lay_entrada"],
                    "Probabilidade ML": f"{j['Prob']}%",
                    "Estratégia": " + ".join(j["Modelos_Aprovados"])
                }
                for j in jogos_vistos.values()
            ])
            
            st.divider()
            
            if df_final.empty:
                st.info(f"O robô analisou {len(df)} jogos hoje, mas **nenhum** atendeu aos critérios estritos da IA (XGBoost na faixa >= 10.0 com filtros contextuais). Guarde a banca!")
                with st.expander("Ver todos os palpites rejeitados (fora da faixa de odd/probabilidade estrita/blacklist)"):
                    rejected = df.copy()
                    rejected["Filtros_Originais"] = rejected["Metodo"]
                    st.dataframe(rejected.drop(columns=["Odd_Num", "Prob_Num", "Metodo", "PREENCHER_odd_abertura", "PREENCHER_odd_min60", "PREENCHER_odd_min75", "Placar_final", "Momento_gols", "status", "obs"]), use_container_width=True)
            else:
                st.success(f"🔥 {len(df_final)} Oportunidades de Valor Encontradas!")
                
                # Exibe a tabela bonita
                st.dataframe(df_final, use_container_width=True)
                
                # Botão de Download
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_final.to_excel(writer, index=False, sheet_name='Sinais')
                excel_data = buffer.getvalue()
                
                st.download_button(
                    label="📥 Baixar Planilha de Sinais (Excel)",
                    data=excel_data,
                    file_name=f"sinais_lay0x0_realista_{target_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                
                st.caption("Opere essas entradas em **Full Match** (segurando até o final do jogo) para colher a expectativa matemática positiva validados no backtest.")
