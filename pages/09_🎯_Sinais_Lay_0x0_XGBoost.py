# -*- coding: utf-8 -*-
"""
pages/09_🎯_Sinais_Lay_0x0_XGBoost.py — Painel e Monitor em Tempo Real do Lay 0x0 XGBoost
Modelo Quantitativo de Machine Learning (Sweet Spot [10.0, 20.0], EV > 2%, Liga < 8%, Mkt < 10%)
"""

import os
import sys
import io
import time
import traceback
from datetime import datetime, date
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Sinais Lay 0x0 XGBoost - Ao Vivo",
    page_icon="🎯",
    layout="wide",
)

ROOT = Path(__file__).resolve().parent.parent
FORWARD_0X0_DIR = ROOT / "forward_0x0"
LEDGER_FILE = FORWARD_0X0_DIR / "ledger_forward_0x0.csv"
CLV_FILE = FORWARD_0X0_DIR / "clv_0x0_log.csv"

st.title("🎯 Sinais Lay 0x0 (Modelo Quantitativo XGBoost)")
st.markdown("""
O **Lay 0x0 XGBoost** é o único método quantitativo do sistema fundamentado em **Machine Learning independente do preço**. 
Enquanto métodos de Match Odds dependem de distorções em odds cegas, o XGBoost estima a probabilidade intrínseca de 0x0 a partir de **features temporais sem vazamento** (*as-of rolling stats*, xG acumulado, médias móveis e contexto de liga).

### 🛡️ Regra Base Congelada (Sweet Spot de Elite):
1. **Faixa de Odd Lay Betfair:** Entre **10.00 e 20.00** (Liability controlada).
2. **Valor Esperado Positivo ($EV$):** $EV > +2.0\%$ calculado sobre a probabilidade prevista pelo XGBoost e a odd real de Lay.
3. **Filtro Contextual de Liga:** Taxa histórica de 0x0 da liga **$< 8.0\%$** (elimina ligas amarradas de placar magro).
4. **Probabilidade Implícita de Mercado:** $\text{mkt\_prob} < 10.0\%$ (o mercado concorda que o 0x0 é improvável).
5. **Execução:** **Full Match** (segurar até o apito final; green garantido se sair qualquer gol).
""")

# ── Tabs Principais ──
tab1, tab2, tab3 = st.tabs([
    "🎯 Sinais do Dia (Ao Vivo)",
    "📊 Livro-Razão & Performance Forward",
    "📐 Engenharia do Modelo & Pré-Registro"
])

# =========================================================================
# TAB 1: SINAIS DO DIA
# =========================================================================
with tab1:
    col_ctrl, col_main = st.columns([1, 3])
    
    with col_ctrl:
        st.markdown("### ⚙️ Parâmetros")
        target_date = st.date_input("Data dos Jogos", value=date.today(), key="date_input_0x0")
        target_str = target_date.strftime("%Y-%m-%d")
        
        st.markdown("### 💰 Gestão de Banca")
        banca_val = st.number_input("Banca Atual (R$)", min_value=100.0, value=2000.0, step=100.0)
        gestao_mode = st.selectbox(
            "Dimensionamento de Risco",
            [
                "Kelly 0.25 (Recomendado - Máx 2.5% Liability)",
                "Liability Fixa 2.5% (Conservador)",
                "Liability Fixa 5.0% (Equilibrado)"
            ]
        )
        
        btn_buscar = st.button("🔄 Pesquisar Sinais do Dia", type="primary", use_container_width=True)
        
    with col_main:
        picks_file = FORWARD_0X0_DIR / f"picks_0x0_{target_str}.csv"
        df_picks = pd.DataFrame()
        
        # 1. Carrega do cache diário se já existir
        if picks_file.exists():
            try:
                df_picks = pd.read_csv(picks_file)
            except Exception:
                df_picks = pd.DataFrame()
                
        if btn_buscar or df_picks.empty:
            import subprocess
            _script_0x0 = FORWARD_0X0_DIR / "gerar_picks_dia.py"
            # No Streamlit Cloud (1 GB de RAM) rodar o motor mata o container: a base b365 sozinha ocupa
            # ~590 MB em memoria. Na nuvem a pagina so LE o arquivo de picks gerado pelo robo local.
            _na_nuvem = str(ROOT).startswith("/mount/src") or bool(os.environ.get("STREAMLIT_SHARING_MODE"))
            if _na_nuvem:
                st.info("Os picks do dia sao gerados pelo robo local (rodar_0x0.bat). "
                        "Aqui na nuvem esta pagina apenas mostra o arquivo do dia — rodar o modelo XGBoost "
                        "exigiria ~600 MB de RAM e derrubaria o app.")
            if (not _na_nuvem) and _script_0x0.exists() and (btn_buscar or target_str >= date.today().strftime("%Y-%m-%d")):
                with st.spinner(f"Rodando motor XGBoost Lay 0x0 para {target_str}..."):
                    try:
                        subprocess.run([sys.executable, str(_script_0x0), target_str], cwd=str(ROOT), timeout=60, check=False)
                    except Exception:
                        pass
            if picks_file.exists():
                try:
                    df_picks = pd.read_csv(picks_file)
                except Exception:
                    df_picks = pd.DataFrame()
            if df_picks.empty and LEDGER_FILE.exists():
                try:
                    df_l = pd.read_csv(LEDGER_FILE)
                    df_l_dia = df_l[df_l["Data"] == target_str].copy()
                    if not df_l_dia.empty:
                        df_picks = df_l_dia
                except Exception:
                    pass

        if not df_picks.empty:
            st.success(f"🔥 **{len(df_picks)} Oportunidade(s) Encontrada(s) para {target_str}!**")
            
            tabela_exibir = []
            for _, r in df_picks.iterrows():
                jogo_nome = r.get("jogo") or f"{r.get('Home')} x {r.get('Away')}"
                odd_lay = float(r.get("odd_lay") or r.get("odd_lay_entrada") or 0.0)
                prob_ml = float(r.get("p") or 0.0)
                ev_val = float(r.get("ev") or 0.0)
                link_bf = r.get("link", "")
                ko_hora = str(r.get("ko", "15:00"))[:5]
                liga_nome = r.get("liga") or r.get("Liga", "")
                
                # Cálculo de Sizing
                if "Kelly" in gestao_mode:
                    kelly = prob_ml - (1.0 - prob_ml) * (odd_lay - 1.0) / 0.95 if odd_lay > 1 else 0.0
                    frac = min(0.025, max(0.0, 0.25 * kelly))
                elif "2.5%" in gestao_mode:
                    frac = 0.025
                else:
                    frac = 0.05
                    
                risk_rs = banca_val * frac
                stake_bf = risk_rs / (odd_lay - 1.0) if odd_lay > 1.0 else 0.0
                
                tabela_exibir.append({
                    "Horário": ko_hora,
                    "Liga": liga_nome,
                    "Partida": jogo_nome,
                    "Odd Lay Betfair": f"{odd_lay:.2f}",
                    "Prob. ML (XGBoost)": f"{prob_ml*100:.1f}%",
                    "EV Estimado": f"{ev_val*100:+.1f}%",
                    "Taxa 0x0 Liga": f"{float(r.get('liga_0x0', 0))*100:.1f}%",
                    "Prob. Mercado": f"{float(r.get('mkt_prob', 0))*100:.1f}%",
                    "Liability (Risco)": f"R$ {risk_rs:.2f}",
                    "Stake Back Betfair": f"R$ {stake_bf:.2f}",
                    "Link Betfair": link_bf if link_bf else "https://www.betfair.bet.br/exchange/plus/football"
                })
                
            df_view = pd.DataFrame(tabela_exibir)
            
            st.dataframe(
                df_view,
                column_config={
                    "Link Betfair": st.column_config.LinkColumn("Mercado Betfair", display_text="Abrir Exchange ↗")
                },
                use_container_width=True,
                hide_index=True
            )
            
            st.caption("ℹ️ **Execução:** Manter a posição em Full Match até o fim da partida. Não efetuar cash out.")
        else:
            st.info(f"ℹ️ **Nenhum sinal aprovado para {target_str}.**")
            st.markdown(f"""
            O filtro do **Lay 0x0 XGBoost** é hiper-seletivo. Para aprovar um jogo, ele exige simultaneamente:
            * Odd Lay da Betfair rigorosamente entre **10.00 e 20.00**.
            * Taxa histórica de 0x0 da liga **< 8.0%**.
            * Probabilidade de mercado implícita **< 10.0%**.
            * Vantagem matemática estatística ($EV > +2\%$).
            
            > 💡 **Frequência Típica:** O modelo gera cerca de **1 a 3 jogos por semana**, priorizando alta taxa de acerto e preservação estrita de capital. Quando não há jogos, **guarde a banca**.
            """)

# =========================================================================
# TAB 2: LIVRO-RAZÃO & HISTÓRICO FORWARD
# =========================================================================
with tab2:
    st.subheader("📊 Livro-Razão Forward (Sinais Auditados e Liquidados)")
    
    if LEDGER_FILE.exists():
        try:
            df_ledger = pd.read_csv(LEDGER_FILE)
        except Exception:
            df_ledger = pd.DataFrame()
    else:
        df_ledger = pd.DataFrame()
        
    if df_ledger.empty:
        st.warning("O arquivo de ledger forward (`forward_0x0/ledger_forward_0x0.csv`) ainda não possui registros locais.")
    else:
        df_liq = df_ledger[df_ledger["status"] == "LIQUIDADO"].copy()
        
        n_total = len(df_ledger)
        n_liq = len(df_liq)
        greens = (df_liq["target"] == 1).sum() if "target" in df_liq.columns else 0
        reds = (df_liq["target"] == 0).sum() if "target" in df_liq.columns else 0
        wr_real = (greens / n_liq * 100) if n_liq > 0 else 0.0
        pnl_u_total = df_liq["pnl_liab"].astype(float).sum() if "pnl_liab" in df_liq.columns else 0.0
        
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        col_m1.metric("Total Sinais Registrados", f"{n_total} jogos", f"{n_liq} liquidados")
        col_m2.metric("Win Rate Real (Forward)", f"{wr_real:.1f}%", f"{greens}G / {reds}R")
        col_m3.metric("P&L Acumulado (Liability)", f"{pnl_u_total:+.2f} u", "Em risco")
        
        # Leitura de CLV se existir
        if CLV_FILE.exists():
            try:
                df_clv = pd.read_csv(CLV_FILE)
                clv_vals = pd.to_numeric(df_clv["clv_lay0x0"], errors="coerce").dropna()
                med_clv = clv_vals.median() if not clv_vals.empty else 0.0
                col_m4.metric("CLV Mediano vs Fechamento", f"{med_clv:+.2f}%", "Closing Line Value")
            except Exception:
                col_m4.metric("CLV Mediano", "N/D")
        else:
            col_m4.metric("CLV Mediano", "N/D")
            
        st.divider()
        
        st.markdown("#### Detalhamento de Partidas do Forward")
        
        def _cor_status(val):
            if str(val) == "1" or str(val).upper() == "GREEN":
                return "background-color: #1e3d2f; color: #4ade80; font-weight: bold;"
            elif str(val) == "0" or str(val).upper() == "RED":
                return "background-color: #4c1d24; color: #f87171; font-weight: bold;"
            return ""

        colunas_exibir = [c for c in [
            "Data", "Home", "Away", "liga", "odd_lay_entrada", "p", "ev",
            "status", "placar", "target", "pnl_liab", "break_even"
        ] if c in df_ledger.columns]
        
        df_display = df_ledger[colunas_exibir].copy()
        df_display.rename(columns={
            "liga": "Liga", "odd_lay_entrada": "Odd Lay", "p": "Prob ML", "ev": "EV",
            "status": "Status", "placar": "Placar FT", "target": "Green/Red",
            "pnl_liab": "PnL (u)", "break_even": "Break-Even"
        }, inplace=True)
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)

# =========================================================================
# TAB 3: ENGENHARIA DO MODELO & PRÉ-REGISTRO
# =========================================================================
with tab3:
    st.subheader("📐 Especificação Técnica e Pré-Registro")
    
    st.markdown("""
    ### 🔬 Por que o Lay 0x0 XGBoost é Diferente?
    A esmagadora maioria dos métodos de Match Odds (como Lay Draw ou Lay Home) perde para a comissão da Betfair no longo prazo porque tenta adivinhar distorções em odds eficientes sem informação nova.
    
    O **Lay 0x0 XGBoost** ataca uma ineficiência estrutural comprovada:
    1. **Preço vs Risco Físico:** Mercados com odd Lay entre 10.0 e 20.0 pagam prêmio desproporcional quando a dinâmica ofensiva das equipes e da liga aponta para baixíssima probabilidade de 0x0.
    2. **Features Leak-Free:** O modelo utiliza médias móveis com `shift(1)`, decomposição de mando (*Home* vs *Away* estritamente separados), e decaimento exponencial ponderado.
    3. **Pre-requisito para Produção Real:** O modelo só será liberado para capital integral após atingir $N \ge 300$ jogos no paper trading com **CLV mediano positivo e $\%$CLV+ $> 50\%$** (provando que ele antecipa a linha de fechamento da Betfair Exchange).
    
    > 📄 Documento de Pré-Registro Completo: Consulte `PREREGISTRO_lay0x0_xgb_forward.md` no repositório.
    """)
