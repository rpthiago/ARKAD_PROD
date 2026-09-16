# -*- coding: utf-8 -*-
"""
pages/03_⚡_Radar_Trader_InPlay.py — Cockpit Operacional e Calculadora dos Métodos Trader In-Play.

Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md & GEMINI.md
Status: OBSERVACAO_STAKE_ZERO (stake: 0.0)

Os 4 Métodos de Trading In-Play:
  1. LTD Trader Clássico (15'-25' 0-0, saída no 1º gol ou stop aos 68')
  2. Swing Trade: Fav em Desvantagem (20'-45' 0-1, saída no empate ou stop aos 70')
  3. Scalping de Janela Morta (33'-38' HT ou 55'-62' FT, saída em 4-6 ticks)
  4. Late Goal Trader (78'-84' diff 1 gol, Back Over limite até os 90')
"""

import sys
from datetime import date, datetime
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st

# Configuração da página Streamlit
st.set_page_config(
    page_title="Radar Trader In-Play — ARKAD",
    page_icon="⚡",
    layout="wide",
)

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trader_inplay_engine import (
    calcular_cashout_ltd,
    calcular_cashout_back,
    calcular_freebet_back,
    calcular_stop_loss_tempo,
    escanear_oportunidades_trader,
)

try:
    from inplay_telemetry_engine import InPlayTelemetryEngine
except Exception:
    InPlayTelemetryEngine = None

try:
    from futpythontrader_client import get_daily_dataframe
except Exception:
    get_daily_dataframe = None

try:
    import b365_data_utils
except Exception:
    b365_data_utils = None


# Estilo customizado
st.markdown("""
<style>
    .trader-header {
        background: linear-gradient(135deg, #101726 0%, #1a233a 100%);
        padding: 20px;
        border-radius: 12px;
        border-left: 6px solid #ff9800;
        margin-bottom: 20px;
    }
    .card-trader {
        background-color: #161e2e;
        border-radius: 12px;
        padding: 18px;
        margin-bottom: 16px;
        border: 1px solid #283548;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    }
    .card-ltd {
        border-left: 6px solid #2196f3;
    }
    .card-fav {
        border-left: 6px solid #ff5722;
    }
    .card-scalp {
        border-left: 6px solid #00bcd4;
    }
    .card-late {
        border-left: 6px solid #4caf50;
    }
    .badge-metodo {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.85rem;
        margin-right: 8px;
    }
    .badge-ltd { background-color: #1976d2; color: #ffffff; }
    .badge-fav { background-color: #e64a19; color: #ffffff; }
    .badge-scalp { background-color: #0097a7; color: #ffffff; }
    .badge-late { background-color: #388e3c; color: #ffffff; }
    .badge-odd-ok { background-color: #2e7d32; color: #ffffff; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }
    .badge-odd-wait { background-color: #f57f17; color: #ffffff; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }
    .metric-box {
        background-color: #1e293b;
        padding: 12px;
        border-radius: 8px;
        text-align: center;
        border: 1px solid #334155;
    }
    .metric-val {
        font-size: 1.3rem;
        font-weight: bold;
    }
    .metric-lbl {
        font-size: 0.75rem;
        color: #94a3b8;
        text-transform: uppercase;
    }
    .btn-betfair {
        background-color: #ffb80c;
        color: #000000 !important;
        font-weight: bold;
        padding: 8px 14px;
        border-radius: 6px;
        text-decoration: none;
        display: inline-block;
        margin-top: 8px;
    }
</style>
""", unsafe_allow_html=True)


# Cabeçalho Principal
st.markdown("""
<div class="trader-header">
    <h2 style="margin:0; color:#ffb80c;">⚡ Suíte de Métodos Trader In-Play — ARKAD</h2>
    <p style="margin:5px 0 0 0; color:#cbd5e1; font-size: 0.95rem;">
        Cockpit de Trading ao vivo na Betfair Exchange com Hedging real, cálculo de Cashout com 5% de comissão e Stop Loss disciplinado.
    </p>
    <div style="margin-top: 10px;">
        <span style="background-color: #334155; color: #f1f5f9; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
            🛡️ PROTOCOLO: STAKE-ZERO (stake: 0.0) | PRÉ-REGISTRO OFICIAL CONGELADO (N ≥ 200)
        </span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Tabs Principais ──
tab1, tab2, tab3 = st.tabs([
    "⚡ Radar Ao Vivo (Cockpit de Trading)",
    "🧮 Calculadora Dinâmica de Cashout & Freebet",
    "📜 Regras Congeladas & Validação"
])


# =========================================================================
# TAB 1: RADAR AO VIVO (COCKPIT DE TRADING)
# =========================================================================
with tab1:
    col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 2, 1.5, 1.5])

    with col_f1:
        target_date = st.date_input("Data dos Jogos", value=date.today(), key="trader_date")
        target_str = target_date.strftime("%Y-%m-%d")

    with col_f2:
        metodo_filtro = st.selectbox(
            "Filtrar por Método",
            [
                "Todos os Métodos",
                "LTD Trader Clássico",
                "Swing Trade: Fav em Desvantagem",
                "Scalping de Janela Morta",
                "Late Goal Trader (Over Limite)"
            ]
        )

    with col_f3:
        status_filtro = st.selectbox(
            "Filtrar por Status",
            ["Todos", "Ao Vivo (Janela Ativa)", "Pré-Jogo / Preparar", "Aguardando Odd"]
        )

    with col_f4:
        st.write("")
        st.write("")
        btn_atualizar = st.button("🔄 Atualizar Radar", type="primary", use_container_width=True)

    # Loader de Jogos e Telemetria
    @st.cache_data(ttl=60)
    def carregar_dados_trader(dt_str: str):
        df = None
        # 1. Tenta API Betfair oficial
        if get_daily_dataframe is not None:
            try:
                df = get_daily_dataframe(source="betfair", date_str=dt_str)
            except Exception:
                df = None

        # 2. Fallback b365 utils
        if (df is None or df.empty) and b365_data_utils is not None:
            try:
                games_list = b365_data_utils.fetch_betfair_daily(dt_str)
                if games_list:
                    df = pd.DataFrame(games_list)
            except Exception:
                df = None

        # 3. Telemetria In-Play
        mapa_live = {}
        if InPlayTelemetryEngine is not None:
            try:
                tele_engine = InPlayTelemetryEngine()
                mapa_live = tele_engine._mapa_live
            except Exception:
                mapa_live = {}

        return df, mapa_live

    df_jogos, mapa_telemetria = carregar_dados_trader(target_str)

    oportunidades = []
    if df_jogos is not None and not df_jogos.empty:
        oportunidades = escanear_oportunidades_trader(df_jogos, mapa_telemetria)

    # Filtros
    ops_filtradas = []
    for op in oportunidades:
        # Filtro método
        if metodo_filtro != "Todos os Métodos" and metodo_filtro.lower() not in op["nome_metodo"].lower():
            continue
        # Filtro status
        if status_filtro == "Ao Vivo (Janela Ativa)" and "AO VIVO" not in op["status_tempo"]:
            continue
        if status_filtro == "Pré-Jogo / Preparar" and "PREPARAR" not in op["status_tempo"]:
            continue
        if status_filtro == "Aguardando Odd" and op.get("status_odd") != "AGUARDANDO_ODD":
            continue
        ops_filtradas.append(op)

    # KPI Bar
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown("""
        <div class="metric-box">
            <div class="metric-val" style="color:#60a5fa;">{}</div>
            <div class="metric-lbl">Total Oportunidades</div>
        </div>
        """.format(len(ops_filtradas)), unsafe_allow_html=True)
    with k2:
        qtd_live = sum(1 for o in ops_filtradas if "AO VIVO" in o["status_tempo"] or "GATILHO" in o["status_tempo"])
        st.markdown("""
        <div class="metric-box">
            <div class="metric-val" style="color:#4ade80;">{}</div>
            <div class="metric-lbl">Em Janela Ao Vivo</div>
        </div>
        """.format(qtd_live), unsafe_allow_html=True)
    with k3:
        qtd_prep = sum(1 for o in ops_filtradas if "PREPARAR" in o["status_tempo"])
        st.markdown("""
        <div class="metric-box">
            <div class="metric-val" style="color:#facc15;">{}</div>
            <div class="metric-lbl">Radar Preparatório</div>
        </div>
        """.format(qtd_prep), unsafe_allow_html=True)
    with k4:
        qtd_wait = sum(1 for o in ops_filtradas if o.get("status_odd") == "AGUARDANDO_ODD")
        st.markdown("""
        <div class="metric-box">
            <div class="metric-val" style="color:#f87171;">{}</div>
            <div class="metric-lbl">Aguardando Odd Real</div>
        </div>
        """.format(qtd_wait), unsafe_allow_html=True)

    st.markdown("---")

    if not ops_filtradas:
        st.info(f"Nenhuma oportunidade ativa encontrada para os filtros selecionados na data {target_str}.")
        
        # Opção de demonstrador didático
        with st.expander("👁️ Ver Demonstração Didática dos 4 Sinais Operacionais"):
            st.caption("Exemplo simulado de como cada um dos 4 métodos se apresenta durante uma partida ao vivo:")
            
            exemplo_ops = [
                {
                    "id_metodo": "LTD_TRADER",
                    "nome_metodo": "LTD Trader Clássico",
                    "icone": "🤝",
                    "jogo": "Arsenal x Luton Town",
                    "liga": "Premier League",
                    "data": target_str,
                    "hora": "16:00",
                    "minuto_atual": "21'",
                    "placar_atual": "0 - 0",
                    "status_tempo": "AO VIVO (Janela Ativa)",
                    "status_odd": "ODD_DISPONIVEL",
                    "mercado": "Match Odds (The Draw)",
                    "lado": "LAY",
                    "runner": "Empate",
                    "faixa_odd_entrada": "3.00 a 4.20",
                    "odd_atual": 3.45,
                    "placar_gatilho": "0 - 0",
                    "minuto_janela": "15' a 25'",
                    "take_profit": "Gol do Favorito (1-0) -> Back Empate @ 7.50 (+55% PnL)",
                    "stop_loss": "0-0 aos 68' -> Cashout @ ~2.00 (-58% PnL) ou se Zebra fizer 0-1",
                    "tipo_gestao": "Liability Fixa",
                    "risco_sugerido_pct": 5.0,
                    "stake": 0.0,
                    "tipo_registro": "OBSERVACAO_STAKE_ZERO",
                    "link_betfair": "https://www.betfair.com/exchange/plus/football",
                    "observacao": "Sair no gol do favorito. Não segurar até os 90'."
                },
                {
                    "id_metodo": "FAV_DESVANTAGEM",
                    "nome_metodo": "Swing Trade: Fav em Desvantagem",
                    "icone": "🔥",
                    "jogo": "Manchester City x Bournemouth",
                    "liga": "Premier League",
                    "data": target_str,
                    "hora": "17:30",
                    "minuto_atual": "28'",
                    "placar_atual": "0 - 1",
                    "status_tempo": "GATILHO ATIVO (0-1 In-Play)",
                    "status_odd": "ODD_DISPONIVEL",
                    "mercado": "Match Odds (Mandante)",
                    "lado": "BACK",
                    "runner": "Manchester City",
                    "faixa_odd_entrada": "2.10 a 3.10",
                    "odd_atual": 2.40,
                    "placar_gatilho": "0 - 1",
                    "minuto_janela": "20' a 45'",
                    "take_profit": "Empate (1-1) -> Cashout Lay Mandante @ ~1.48 (+50% PnL) ou Freebet",
                    "stop_loss": "Minuto 70' se persistir 0-1 e pressão esfriar, ou stop imediato em 0-2",
                    "tipo_gestao": "Stake Fixa",
                    "risco_sugerido_pct": 5.0,
                    "stake": 0.0,
                    "tipo_registro": "OBSERVACAO_STAKE_ZERO",
                    "link_betfair": "https://www.betfair.com/exchange/plus/football",
                    "observacao": "Excelente relação Risco x Retorno. Fechar na igualdade sem precisar da virada."
                },
                {
                    "id_metodo": "SCALPING_UNDER",
                    "nome_metodo": "Scalping de Janela Morta",
                    "icone": "⏱️",
                    "jogo": "Getafe x Valencia",
                    "liga": "La Liga",
                    "data": target_str,
                    "hora": "19:00",
                    "minuto_atual": "35'",
                    "placar_atual": "0 - 0",
                    "status_tempo": "OPERAÇÃO RÁPIDA (35')",
                    "status_odd": "ODD_DISPONIVEL",
                    "mercado": "Under 1.5 HT",
                    "lado": "BACK",
                    "runner": "Under 1.5 HT",
                    "faixa_odd_entrada": "1.30 a 1.65",
                    "odd_atual": 1.42,
                    "placar_gatilho": "0 - 0",
                    "minuto_janela": "33'-38' HT ou 55'-62' FT",
                    "take_profit": "Permanecer 5 a 8 minutos (queda 4-6 ticks) -> Lay Under (+12% PnL)",
                    "stop_loss": "Se sair gol durante a janela -> Fechar imediatamente no repique",
                    "tipo_gestao": "Stake Fixa",
                    "risco_sugerido_pct": 3.0,
                    "stake": 0.0,
                    "tipo_registro": "OBSERVACAO_STAKE_ZERO",
                    "link_betfair": "https://www.betfair.com/exchange/plus/football",
                    "observacao": "Operação cirúrgica. Não prolongar além da janela estipulada."
                },
                {
                    "id_metodo": "LATE_GOAL",
                    "nome_metodo": "Late Goal Trader (Over Limite)",
                    "icone": "⚡",
                    "jogo": "Leverkusen x Wolfsburg",
                    "liga": "Bundesliga",
                    "data": target_str,
                    "hora": "20:30",
                    "minuto_atual": "81'",
                    "placar_atual": "2 - 1",
                    "status_tempo": "PRESSÃO FINAL (81')",
                    "status_odd": "ODD_DISPONIVEL",
                    "mercado": "Over 3.5 FT",
                    "lado": "BACK",
                    "runner": "Mais de 3.5 Gols",
                    "faixa_odd_entrada": "1.80 a 2.50",
                    "odd_atual": 2.15,
                    "placar_gatilho": "2 - 1",
                    "minuto_janela": "78' a 84'",
                    "take_profit": "Gol nos minutos finais -> 100% Green Automático (sem necessidade de cashout)",
                    "stop_loss": "Apito final sem gols (perda da stake de 1u)",
                    "tipo_gestao": "Stake Fixa (1u)",
                    "risco_sugerido_pct": 5.0,
                    "stake": 0.0,
                    "tipo_registro": "OBSERVACAO_STAKE_ZERO",
                    "link_betfair": "https://www.betfair.com/exchange/plus/football",
                    "observacao": "Jogo totalmente quebrado taticamente. Explora contra-ataques e desespero."
                }
            ]
            ops_para_exibir = exemplo_ops
    else:
        ops_para_exibir = ops_filtradas

    # Renderização dos Cards Operacionais
    for op in ops_para_exibir:
        metodo_id = op["id_metodo"]
        card_class = "card-ltd" if "LTD" in metodo_id else ("card-fav" if "FAV" in metodo_id else ("card-scalp" if "SCALPING" in metodo_id else "card-late"))
        badge_class = "badge-ltd" if "LTD" in metodo_id else ("badge-fav" if "FAV" in metodo_id else ("badge-scalp" if "SCALPING" in metodo_id else "badge-late"))
        odd_badge = '<span class="badge-odd-ok">🟢 ODD BETFAIR</span>' if op.get("status_odd") == "ODD_DISPONIVEL" else '<span class="badge-odd-wait">🟡 AGUARDANDO ODD</span>'

        # Minuto numérico para barra
        m_str = str(op.get("minuto_atual", "0")).replace("'", "").strip()
        try:
            m_val = int(m_str)
        except Exception:
            m_val = 0
        pct_tempo = min(100, int((m_val / 90.0) * 100))

        minuto_disp = op.get("minuto_atual", "0'")
        placar_disp = op.get("placar_atual", "0-0")
        link_bf = op.get("link_betfair", "https://www.betfair.com/exchange/plus/football")
        obs_disp = op.get("observacao", "")
        liga_disp = op.get("liga", "")
        hora_disp = op.get("hora", "")

        st.markdown(f"""
        <div class="card-trader {card_class}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <span class="badge-metodo {badge_class}">{op['icone']} {op['nome_metodo']}</span>
                    {odd_badge}
                    <span style="font-weight:bold; font-size:1.1rem; margin-left:8px; color:#ffffff;">{op['jogo']}</span>
                    <span style="color:#94a3b8; font-size:0.85rem; margin-left:8px;">({liga_disp} • {hora_disp})</span>
                </div>
                <div>
                    <span style="background-color:#1e293b; color:#38bdf8; padding:4px 10px; border-radius:6px; font-weight:bold; font-size:0.95rem;">
                        ⏱️ {minuto_disp} | Placar: {placar_disp}
                    </span>
                </div>
            </div>
            <div style="background-color:#334155; border-radius:4px; height:6px; width:100%; margin: 10px 0 14px 0; overflow:hidden;">
                <div style="background-color:#38bdf8; height:100%; width:{pct_tempo}%;"></div>
            </div>
            <div style="display:grid; grid-template-columns: 1fr 1fr 1fr 1.2fr; gap: 12px; margin-top:8px;">
                <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                    <div style="font-size:0.75rem; color:#94a3b8;">MERCADO & LADO</div>
                    <div style="font-weight:bold; color:#f8fafc;">{op['lado']} {op['runner']}</div>
                    <div style="font-size:0.8rem; color:#cbd5e1;">{op['mercado']}</div>
                </div>
                <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                    <div style="font-size:0.75rem; color:#94a3b8;">PREÇO DE ENTRADA</div>
                    <div style="font-weight:bold; color:#ffb80c;">Odd Atual: {op['odd_atual']}</div>
                    <div style="font-size:0.8rem; color:#cbd5e1;">Faixa Alvo: {op['faixa_odd_entrada']}</div>
                </div>
                <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                    <div style="font-size:0.75rem; color:#94a3b8;">TAKE PROFIT (GREEN)</div>
                    <div style="font-size:0.85rem; color:#4ade80; font-weight:600;">{op['take_profit']}</div>
                </div>
                <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                    <div style="font-size:0.75rem; color:#94a3b8;">STOP LOSS (RED)</div>
                    <div style="font-size:0.85rem; color:#f87171; font-weight:600;">{op['stop_loss']}</div>
                </div>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:10px;">
                <div style="font-size:0.8rem; color:#94a3b8;">
                    💡 <b>Observação:</b> {obs_disp}
                </div>
                <div>
                    <a href="{link_bf}" target="_blank" class="btn-betfair">
                        🔗 Abrir Mercado Betfair Exchange
                    </a>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# =========================================================================
# TAB 2: CALCULADORA DINÂMICA DE CASHOUT & FREEBET
# =========================================================================
with tab2:
    st.markdown("### 🧮 Simulador de Cashout, Hedging e Freebet (Betfair Real)")
    st.markdown("""
    Esta calculadora aplica a **fórmula matemática exata do livro de ofertas da Betfair Exchange** com o desconto da taxa de comissão de **5% sobre o ganho líquido**.
    Simule saídas em Green (Take Profit) ou contenção de perdas em Stop Loss.
    """)

    col_tipo, col_comissao = st.columns([2, 1])
    with col_tipo:
        tipo_operacao = st.radio(
            "Selecione o Tipo de Entrada Original:",
            ["LAY Inicial (ex: LTD Trader)", "BACK Inicial (ex: Fav em Desvantagem ou Scalping Under)"],
            horizontal=True
        )
    with col_comissao:
        comissao_pct = st.number_input("Comissão Betfair (%)", min_value=0.0, max_value=10.0, value=5.0, step=0.5)
        taxa_comissao = comissao_pct / 100.0

    st.markdown("---")

    if "LAY" in tipo_operacao:
        st.subheader("🤝 Simulação de Cashout para Posição LAY (LTD Trader)")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            odd_lay_in = st.number_input("Odd de Entrada (LAY)", min_value=1.05, max_value=20.0, value=3.50, step=0.05)
        with c2:
            liability_in = st.number_input("Capital em Risco / Liability (R$)", min_value=10.0, value=100.0, step=10.0)
        with c3:
            odd_back_out = st.number_input("Odd Atual de Saída (BACK)", min_value=1.05, max_value=100.0, value=7.50, step=0.10)
        with c4:
            minuto_simulado = st.number_input("Minuto do Jogo", min_value=1, max_value=95, value=25, step=1)

        # Executa o cálculo
        res_calc = calcular_cashout_ltd(odd_lay_entrada=odd_lay_in, odd_draw_atual=odd_back_out, liability=liability_in, commission=taxa_comissao)
        res_stop = calcular_stop_loss_tempo(odd_entrada=odd_lay_in, odd_atual=odd_back_out, liability_ou_stake=liability_in, tipo="LAY", minuto_atual=minuto_simulado, minuto_limite=68, commission=taxa_comissao)

        if res_calc.get("valido"):
            pnl = res_calc["pnl_liquido"]
            roi = res_calc["roi_pct"]
            cor_pnl = "#4ade80" if pnl > 0 else ("#f87171" if pnl < 0 else "#facc15")

            r1, r2, r3, r4 = st.columns(4)
            with r1:
                st.metric("Stake Back de Fechamento", f"R$ {res_calc['stake_fechamento']:.2f}")
            with r2:
                st.metric("PnL Líquido (R$)", f"R$ {pnl:+.2f}")
            with r3:
                st.metric("ROI s/ Capital em Risco", f"{roi:+.1f}%")
            with r4:
                st.metric("Recomendação Operacional", res_stop["recomendacao"])

            if res_stop["recomendacao"] == "CASHOUT_GREEN":
                st.success(f"🎉 **Take Profit Ativo!** {res_stop['descricao']}")
            elif res_stop["recomendacao"] == "STOP_LOSS":
                st.error(f"🚨 **Alerta de Stop Loss!** {res_stop['descricao']}")
            else:
                st.info(f"⏳ **Operação em Andamento:** {res_stop['descricao']}")

            with st.expander("🔍 Memória de Cálculo Detalhada da Betfair"):
                st.write(f"""
                - **Stake Lay Inicial correspondente à Liability:** R$ {res_calc['stake_in']:.2f}
                - **Stake necessária no Back @ {odd_back_out:.2f}:** R$ {res_calc['stake_fechamento']:.2f}
                - **Resultado Bruto Uniforme:** R$ {res_calc['pnl_bruto']:.2f}
                - **Comissão Betfair ({comissao_pct:.1f}% sobre ganhos):** R$ {max(0.0, res_calc['pnl_bruto'] - pnl):.2f}
                - **Lucro Líquido Final Travado:** R$ {pnl:.2f}
                """)

    else:
        st.subheader("🔥 Simulação de Cashout / Freebet para Posição BACK")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            odd_back_in = st.number_input("Odd de Entrada (BACK)", min_value=1.05, max_value=20.0, value=2.50, step=0.05)
        with c2:
            stake_in = st.number_input("Stake de Entrada (R$)", min_value=10.0, value=100.0, step=10.0)
        with c3:
            odd_lay_out = st.number_input("Odd Atual de Saída (LAY)", min_value=1.05, max_value=100.0, value=1.50, step=0.05)
        with c4:
            minuto_simulado = st.number_input("Minuto do Jogo", min_value=1, max_value=95, value=45, step=1)

        res_cash = calcular_cashout_back(odd_back_in=odd_back_in, odd_lay_out=odd_lay_out, stake_in=stake_in, commission=taxa_comissao)
        res_free = calcular_freebet_back(odd_back_in=odd_back_in, odd_lay_out=odd_lay_out, stake_in=stake_in, commission=taxa_comissao)
        res_stop = calcular_stop_loss_tempo(odd_entrada=odd_back_in, odd_atual=odd_lay_out, liability_ou_stake=stake_in, tipo="BACK", minuto_atual=minuto_simulado, minuto_limite=70, commission=taxa_comissao)

        if res_cash.get("valido"):
            pnl = res_cash["pnl_liquido"]
            roi = res_cash["roi_pct"]

            r1, r2, r3, r4 = st.columns(4)
            with r1:
                st.metric("Stake Lay de Fechamento", f"R$ {res_cash['stake_fechamento']:.2f}")
            with r2:
                st.metric("Cashout Lucro Líquido (R$)", f"R$ {pnl:+.2f}")
            with r3:
                st.metric("ROI s/ Stake", f"{roi:+.1f}%")
            with r4:
                st.metric("Recomendação", res_stop["recomendacao"])

            if res_stop["recomendacao"] == "CASHOUT_GREEN":
                st.success(f"🎉 **Take Profit Ativo!** {res_stop['descricao']}")
            elif res_stop["recomendacao"] == "STOP_LOSS":
                st.error(f"🚨 **Alerta de Stop Loss!** {res_stop['descricao']}")
            else:
                st.info(f"⏳ **Operação em Andamento:** {res_stop['descricao']}")

            st.markdown("#### ⚖️ Comparativo Operacional: Cashout Total vs Freebet")
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                st.markdown(f"""
                <div style="background-color:#1e293b; padding:14px; border-radius:8px; border-left:4px solid #4ade80;">
                    <h4 style="margin:0 0 6px 0; color:#4ade80;">Opção A: Cashout Equilibrado (Hedge Total)</h4>
                    <p style="margin:0; font-size:0.9rem; color:#cbd5e1;">
                        Trava lucro líquido idêntico em qualquer resultado final:
                    </p>
                    <ul style="margin:6px 0 0 0; padding-left:20px; font-size:0.9rem;">
                        <li>Fazer <b>LAY de R$ {res_cash['stake_fechamento']:.2f} @ {odd_lay_out:.2f}</b></li>
                        <li>Lucro Líquido Travado: <b>R$ {pnl:.2f} ({roi:+.1f}%)</b></li>
                        <li>Risco restante: <b>R$ 0,00</b></li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            with col_opt2:
                st.markdown(f"""
                <div style="background-color:#1e293b; padding:14px; border-radius:8px; border-left:4px solid #38bdf8;">
                    <h4 style="margin:0 0 6px 0; color:#38bdf8;">Opção B: Freebet (Deixar Lucro no Vencedor)</h4>
                    <p style="margin:0; font-size:0.9rem; color:#cbd5e1;">
                        Devolve a stake original e mantém todo o lucro no time favorito:
                    </p>
                    <ul style="margin:6px 0 0 0; padding-left:20px; font-size:0.9rem;">
                        <li>Fazer <b>LAY de R$ {res_free['stake_fechamento']:.2f} @ {odd_lay_out:.2f}</b></li>
                        <li>Perda se o time NÃO vencer: <b>R$ 0,00 (Risco Zero)</b></li>
                        <li>Lucro Líquido se o time virar/vencer: <b>R$ {res_free['lucro_se_vencer']:.2f}</b></li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)


# =========================================================================
# TAB 3: REGRAS CONGELADAS & VALIDAÇÃO
# =========================================================================
with tab3:
    st.markdown("### 📜 Governança e Regras Congeladas (PREREGISTRO_SUITE_TRADER_INPLAY.md)")
    st.markdown("""
    Todo método operado no ARKAD obedece às **5 Leis Inegociáveis do GEMINI.md** e ao protocolo de validação honesta.
    """)

    st.markdown("""
    | # | Método Trader | Mercado | Janela | Gatilho de Entrada | Saída Green (Take Profit) | Saída Red (Stop Loss) |
    |---|---|---|---|---|---|---|
    | **1** | **LTD Trader (Clássico)** | Match Odds (Draw) | 15' a 25' | 0-0, Super Fav Mandante ($\le 1.45$), Lay Draw @ [3.00, 4.20] | Gol do favorito: Back Draw @ ~7.50 (+50% a +70% PnL) | 0-0 aos 68' (cashout aceitando -50% a -60%) ou zebra marcar |
    | **2** | **Swing Trade: Fav em Desvantagem** | Match Odds (Fav) | 20' a 45' | Super Fav Mandante ($\le 1.35$) perdendo de 0-1, Back Fav @ [2.10, 3.10] | Gol de empate do fav (1-1): Cashout Lay Fav @ ~1.50 (+40% a +60% PnL) | Minuto 70' se persistir 0-1 sem pressão, ou se zebra fizer 0-2 |
    | **3** | **Scalping de Janela Morta** | Under 1.5 HT / 2.5 FT | 33' a 38' HT ou 55' a 62' FT | Jogo truncado (sem finalização no alvo), Back Under @ [1.30, 1.65] | Permanecer 5-8 min no mercado (queda de 4-6 ticks), Lay Under com +8% a +15% | Gol durante a janela (red de mercado) |
    | **4** | **Late Goal Trader** | Over Limite (+0.5) | 78' a 84' | Diferença de 1 gol (1-0, 0-1, 2-1) ou empate com fav buscando vitória, Back Over @ [1.80, 2.50] | Gol após os 80' (Green total de 100% automático) | Apito final aos 90' sem gols (perda da stake de 1u) |
    """)

    st.markdown("---")

    col_gov1, col_gov2 = st.columns(2)
    with col_gov1:
        st.markdown("#### 🛡️ Critérios de Aprovação Estatística (3 Vias)")
        st.markdown("""
        1. **APROVADO para Capital Real:**
           - Amostra mínima: $N \ge 200$ sinais executáveis ao vivo com odds reais da Betfair.
           - Piso do Intervalo de Confiança (IC95% bloco-dia) **estritamente maior que 0.0%**.
           - Mínimo de 10 Reds observados no período (garantindo robustez à cauda adversa).
           - Sobrevivência ao controle de falsas descobertas (Benjamini-Hochberg FDR).
        2. **REPROVADO / ARQUIVADO:**
           - Teto do IC95% $< 0.0\%$, OU
           - ROI acumulado $< -10.0\%$ após $N \ge 80$ operações.
        3. **INCONCLUSIVO:**
           - Permanece em observação no ledger com `stake: 0.0` até atingir o volume estipulado.
        """)

    with col_gov2:
        st.markdown("#### ⚠️ Leis de Execução In-Play (Lições do GEMINI.md)")
        st.markdown("""
        - **Proibido fabricar dados:** Sem odd real capturada no instante $\\rightarrow$ status `AGUARDANDO_ODD`. Proibido inventar odd média ou teórica.
        - **Mercados Líquidos Exclusivos:** Operações de trading rápido ocorrem **apenas** em Match Odds 1X2 e Over/Under principal. Mercados rasos de Correct Score foram expurgados porque o spread duplo elimina o lucro.
        - **Disciplina de Stop Loss:** Não transformar trade em aposta punter torcedora. Atingido o minuto limite (68' ou 70'), o encerramento da posição é obrigatório.
        - **Matemática do Cashout:** Dedução de 5% de comissão da Betfair em todos os fechamentos verdes.
        """)
