# -*- coding: utf-8 -*-
"""
pages/03_⚡_Radar_Trader_InPlay.py — Cockpit Operacional e Calculadora dos Métodos Trader In-Play.

Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md (Revisão v2) & GEMINI.md
Status: OBSERVACAO_STAKE_ZERO (stake: 0.0)

Os 4 Métodos de Trading In-Play:
  1. LTD Trader Clássico (15'-25' 0-0, Lay Draw [3.50, 10.00], saída no 1º gol ou stop aos 68')
  2. Swing Trade: Fav em Desvantagem (20'-45' 0-1, Back Fav [2.00, 3.20], saída no 1-1 ou stop aos 70')
  3. Scalping de Janela Morta (33'-38' HT ou 55'-62' FT, Back Under, saída em 5-8 min)
  4. Late Goal Trader v2 (82'-86' diff 1 gol, Back Over limite [1.50, 2.60], liquidação oficial)
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
    _canon
)

from tracker_trader_inplay import carregar_log, LOG_CSV

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
    .card-ltd { border-left: 6px solid #2196f3; }
    .card-fav { border-left: 6px solid #ff5722; }
    .card-scalp { border-left: 6px solid #00bcd4; }
    .card-late { border-left: 6px solid #4caf50; }
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
    .badge-green { background-color: #2e7d32; color: #ffffff; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 0.85rem; }
    .badge-red { background-color: #c62828; color: #ffffff; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 0.85rem; }
    .badge-pendente { background-color: #f57f17; color: #ffffff; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 0.85rem; }
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
            🛡️ PROTOCOLO: STAKE-ZERO (stake: 0.0) | COLETOR OFICIAL VPS: betfair_live_odds.csv
        </span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Tabs Principais ──
tab1, tab2, tab3 = st.tabs([
    "⚡ Cockpit & Trades Ao Vivo",
    "🧮 Calculadora Dinâmica de Cashout & Freebet",
    "📜 Regras Congeladas & Governança (Revisão v2)"
])


# =========================================================================
# TAB 1: COCKPIT & TRADES AO VIVO
# =========================================================================
with tab1:
    df_log = carregar_log()

    col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 2, 1.5, 1.5])
    with col_f1:
        datas_disp = sorted(df_log["ko"].str[:10].dropna().unique().tolist(), reverse=True) if not df_log.empty else [str(date.today())]
        if not datas_disp:
            datas_disp = [str(date.today())]
        data_sel = st.selectbox("Data de Referência", ["Todas"] + datas_disp)

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
            "Status Operacional",
            ["Todos", "LIQUIDADO", "PENDENTE", "FORA_DA_FAIXA", "SEM_ODD_SAIDA"]
        )

    with col_f4:
        st.write("")
        st.write("")
        btn_refresh = st.button("🔄 Atualizar Log da VPS", type="primary", use_container_width=True)

    df_filtrado = df_log.copy()
    if not df_filtrado.empty:
        if data_sel != "Todas":
            df_filtrado = df_filtrado[df_filtrado["ko"].str[:10] == data_sel]
        if metodo_filtro != "Todos os Métodos":
            df_filtrado = df_filtrado[df_filtrado["nome_metodo"] == metodo_filtro]
        if status_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado["status"] == status_filtro]

    # KPIs superiores
    total_trades = len(df_filtrado)
    liq_trades = df_filtrado[df_filtrado["status"] == "LIQUIDADO"] if not df_filtrado.empty else pd.DataFrame()
    qtd_liq = len(liq_trades)
    qtd_green = len(liq_trades[liq_trades["resultado"] == "GREEN"]) if not liq_trades.empty else 0
    qtd_red = len(liq_trades[liq_trades["resultado"] == "RED"]) if not liq_trades.empty else 0
    wr_real = (qtd_green / qtd_liq * 100.0) if qtd_liq > 0 else 0.0
    pnl_acum = liq_trades["pnl_liquido"].sum() if not liq_trades.empty else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-val" style="color:#60a5fa;">{total_trades}</div>
            <div class="metric-lbl">Total de Trades Registrados</div>
        </div>
        """, unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-val" style="color:#4ade80;">{qtd_green} G / {qtd_red} R</div>
            <div class="metric-lbl">Taxa de Acerto: {wr_real:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    with k3:
        cor_pnl = "#4ade80" if pnl_acum >= 0 else "#f87171"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-val" style="color:{cor_pnl};">R$ {pnl_acum:+.2f}</div>
            <div class="metric-lbl">P&L Líquido Simulado (100u)</div>
        </div>
        """, unsafe_allow_html=True)
    with k4:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-val" style="color:#facc15;">{total_trades - qtd_liq}</div>
            <div class="metric-lbl">Trades em Andamento (Pendentes)</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    if df_filtrado.empty:
        st.info("Nenhum trade encontrado para os filtros selecionados.")
    else:
        # Renderização dos cards de cada trade
        for _, op in df_filtrado.iterrows():
            metodo_id = str(op["id_metodo"])
            card_class = "card-ltd" if "LTD" in metodo_id else ("card-fav" if "FAV" in metodo_id else ("card-scalp" if "SCALPING" in metodo_id else "card-late"))
            badge_class = "badge-ltd" if "LTD" in metodo_id else ("badge-fav" if "FAV" in metodo_id else ("badge-scalp" if "SCALPING" in metodo_id else "badge-late"))

            st_op = str(op.get("status", "")).upper()
            pnl_val = op.get("pnl_liquido")
            roi_val = op.get("roi_pct")
            pnl_str = f"{float(pnl_val):+.2f}" if pd.notna(pnl_val) else "0.00"
            roi_str = f"{float(roi_val):+.1f}%" if pd.notna(roi_val) else "0.0%"

            if st_op == "LIQUIDADO":
                if op.get("resultado") == "GREEN":
                    res_badge = f'<span class="badge-green">✅ GREEN ({pnl_str} / {roi_str})</span>'
                else:
                    res_badge = f'<span class="badge-red">❌ RED ({pnl_str} / {roi_str})</span>'
            elif st_op == "FORA_DA_FAIXA":
                res_badge = '<span style="background-color:#475569; color:#ffffff; padding:3px 8px; border-radius:4px; font-weight:bold; font-size:0.85rem;">⚠️ FORA DA FAIXA</span>'
            elif st_op == "SEM_ODD_SAIDA":
                res_badge = '<span style="background-color:#64748b; color:#ffffff; padding:3px 8px; border-radius:4px; font-weight:bold; font-size:0.85rem;">⏹️ SEM ODD SAÍDA</span>'
            else:
                res_badge = '<span class="badge-pendente">⏳ PENDENTE</span>'

            min_in = op.get("minuto_entrada", "")
            min_out = op.get("minuto_saida", "")
            mn_display = f"{min_in}'" if not min_out or pd.isna(min_out) else f"{min_in}' ➔ {min_out}'"

            st.markdown(f"""
            <div class="card-trader {card_class}">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <span class="badge-metodo {badge_class}">{op['nome_metodo']}</span>
                        {res_badge}
                        <span style="font-weight:bold; font-size:1.1rem; margin-left:8px; color:#ffffff;">{op['home']} x {op['away']}</span>
                        <span style="color:#94a3b8; font-size:0.85rem; margin-left:8px;">({op.get('competicao', '')} • KO: {op.get('ko', '')})</span>
                    </div>
                    <div>
                        <span style="background-color:#1e293b; color:#38bdf8; padding:4px 10px; border-radius:6px; font-weight:bold; font-size:0.95rem;">
                            ⏱️ Minuto: {mn_display} | Placar: {op.get('placar_entrada', '')} ➔ {op.get('placar_saida', '...') }
                        </span>
                    </div>
                </div>
                <div style="display:grid; grid-template-columns: 1fr 1fr 1.2fr 1.2fr; gap: 12px; margin-top:12px;">
                    <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                        <div style="font-size:0.75rem; color:#94a3b8;">MERCADO & OPERAÇÃO</div>
                        <div style="font-weight:bold; color:#f8fafc;">{op['lado']} {op['runner']}</div>
                        <div style="font-size:0.8rem; color:#cbd5e1;">{op['mercado']}</div>
                    </div>
                    <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                        <div style="font-size:0.75rem; color:#94a3b8;">ODDS REAL BETFAIR</div>
                        <div style="font-weight:bold; color:#ffb80c;">Entrada: {op['odd_entrada']}</div>
                        <div style="font-size:0.8rem; color:#cbd5e1;">Saída: {op.get('odd_saida', 'Aguardando')}</div>
                    </div>
                    <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                        <div style="font-size:0.75rem; color:#94a3b8;">AUDITORIA DE ENTRADA</div>
                        <div style="font-size:0.8rem; color:#94a3b8;">Capture TS In: {op.get('capture_ts_in', '')}</div>
                        <div style="font-size:0.8rem; color:#94a3b8;">Capture TS Out: {op.get('capture_ts_out', 'Pendente')}</div>
                    </div>
                    <div style="background-color:#0f172a; padding:8px 12px; border-radius:6px;">
                        <div style="font-size:0.75rem; color:#94a3b8;">MOTIVO DE SAÍDA / LIQUIDAÇÃO</div>
                        <div style="font-size:0.85rem; color:#e2e8f0;">{op.get('motivo_saida', 'Operação em andamento')}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with st.expander("📥 Ver Tabela Completa do Log (CSV da VPS)"):
            st.dataframe(df_filtrado, use_container_width=True)


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
            odd_lay_in = st.number_input("Odd de Entrada (LAY)", min_value=1.05, max_value=20.0, value=5.50, step=0.10)
        with c2:
            liability_in = st.number_input("Capital em Risco / Liability (R$)", min_value=10.0, value=100.0, step=10.0)
        with c3:
            odd_back_out = st.number_input("Odd Atual de Saída (BACK)", min_value=1.05, max_value=100.0, value=12.00, step=0.50)
        with c4:
            minuto_simulado = st.number_input("Minuto do Jogo", min_value=1, max_value=95, value=25, step=1)

        res_calc = calcular_cashout_ltd(odd_lay_entrada=odd_lay_in, odd_draw_atual=odd_back_out, liability=liability_in, commission=taxa_comissao)
        res_stop = calcular_stop_loss_tempo(odd_entrada=odd_lay_in, odd_atual=odd_back_out, liability_ou_stake=liability_in, tipo="LAY", minuto_atual=minuto_simulado, minuto_limite=68, commission=taxa_comissao)

        if res_calc.get("valido"):
            pnl = res_calc["pnl_liquido"]
            roi = res_calc["roi_pct"]

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

    else:
        st.subheader("🔥 Simulação de Cashout / Freebet para Posição BACK")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            odd_back_in = st.number_input("Odd de Entrada (BACK)", min_value=1.05, max_value=20.0, value=2.40, step=0.05)
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

            st.markdown("#### ⚖️ Comparativo Operacional: Cashout Total vs Freebet")
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                st.markdown(f"""
                <div style="background-color:#1e293b; padding:14px; border-radius:8px; border-left:4px solid #4ade80;">
                    <h4 style="margin:0 0 6px 0; color:#4ade80;">Opção A: Cashout Equilibrado (Hedge Total)</h4>
                    <p style="margin:0; font-size:0.9rem; color:#cbd5e1;">Trava lucro líquido idêntico em qualquer resultado final:</p>
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
                    <p style="margin:0; font-size:0.9rem; color:#cbd5e1;">Devolve a stake original e mantém todo o lucro no favorito:</p>
                    <ul style="margin:6px 0 0 0; padding-left:20px; font-size:0.9rem;">
                        <li>Fazer <b>LAY de R$ {res_free['stake_fechamento']:.2f} @ {odd_lay_out:.2f}</b></li>
                        <li>Perda se o time NÃO vencer: <b>R$ 0,00 (Risco Zero)</b></li>
                        <li>Lucro Líquido se o time virar/vencer: <b>R$ {res_free['lucro_se_vencer']:.2f}</b></li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)


# =========================================================================
# TAB 3: REGRAS CONGELADAS & GOVERNANÇA (REVISÃO V2)
# =========================================================================
with tab3:
    st.markdown("### 📜 Governança e Regras Congeladas (PREREGISTRO_SUITE_TRADER_INPLAY.md)")
    st.markdown("""
    Todo método operado no ARKAD obedece às **5 Leis Inegociáveis do GEMINI.md** e ao protocolo de validação honesta.
    """)

    st.markdown("#### 📊 Medições Empíricas de Odds no Coletor da VPS (16/08 → 13/09/2026)")
    st.markdown("""
    | Método | Mercado & Posição | Janela & Estado | p5 | p25 | Mediana | p75 | p95 | Faixa Medida Congelada |
    |---|---|---|---|---|---|---|---|---|
    | **M1: LTD Clássico** | Lay Draw (Match Odds) | 15'–25' (0-0, Fav $\le 1.45$) | 3.51 | 5.12 | **7.80** | 12.50 | 18.00 | **[3.50, 10.00]** |
    | **M2: Fav em Desvantagem** | Back Fav (Match Odds) | 20'–45' (0-1, Fav $\le 1.35$) | 1.85 | 2.10 | **2.40** | 2.85 | 3.40 | **[2.00, 3.20]** |
    | **M3: Scalping Janela Morta** | Back Under 1.5 HT | 33'–38' HT (0-0) | 1.32 | 1.60 | **1.82** | 2.25 | 3.35 | **[1.35, 2.20]** |
    | **M4: Late Goal v2** | Back Over Limite (+0.5) | 82'–86' (diff = 1) | 1.56 | 1.87 | **2.10** | 2.38 | 2.84 | **[1.50, 2.60]** |
    """)

    st.markdown("---")

    col_gov1, col_gov2 = st.columns(2)
    with col_gov1:
        st.markdown("#### 🛡️ Critérios de Decisão Estatística (3 Vias)")
        st.markdown("""
        1. **APROVADO para Capital Real:**
           - Amostra mínima: $N \ge 200$ operações reais na VPS.
           - Piso do Intervalo de Confiança (IC95% bloco-dia) **estritamente maior que 0.0%**.
           - Mínimo de 10 Reds observados no período.
           - Sobrevivência ao controle FDR (Benjamini-Hochberg).
        2. **REPROVADO / ARQUIVADO:**
           - Teto do IC95% $< 0.0\%$, OU
           - ROI acumulado $< -10.0\%$ após $N \ge 80$ operações.
        3. **INCONCLUSIVO:**
           - Permanece em observação no ledger com `stake: 0.0`.
        """)

    with col_gov2:
        st.markdown("#### ⚠️ Leis de Execução In-Play (Lições do GEMINI.md)")
        st.markdown("""
        - **Placar Factual por O/U:** O estado é obtido via `gols_por_ou`, nunca pelo menor lay do Correct Score (que reflete o placar final mais provável).
        - **Minuto Canônico:** `-min_to_ko - 15`. Proibido usar `abs(min_to_ko)`.
        - **Proibido fabricar dados:** Sem odd real capturada no instante $\\rightarrow$ aposta descartada. Proibido inventar odd média ou teórica.
        - **Disciplina de Stop Loss:** Não transformar trade em torcida. Atingido o minuto limite (68' ou 70'), o encerramento da posição é obrigatório.
        - **Comissão Real:** 5% descontados em todos os fechamentos no verde.
        """)
