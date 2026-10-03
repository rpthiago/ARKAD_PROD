# -*- coding: utf-8 -*-
"""
05_📈_Resultados_Jogos_do_Dia.py — Painel Executivo de Resultados dos Jogos do Dia
no estilo oficial "Resultados dos Métodos em Validação Forward — ARKAD".

Acompanha a liquidação oficial em tempo real dos sinais gerados nos 5 Horários Estratégicos,
com placares autoritativos da Betfair Exchange, métricas de risco (Liability real) e curva de equity intraday.
"""
import os, io, sys
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(
    page_title="ARKAD — Resultados dos Jogos do Dia",
    page_icon="📈",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from liquidar_sinais_dia import liquidar_planilha_dia
from sinais_dia_coletor import sincronizar_ledger_vps

# ── Estilização CSS Moderna (Dark Theme ARKAD) ──
st.markdown("""
<style>
    .kpi-container {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border-radius: 12px;
        padding: 16px 20px;
        border: 1px solid #334155;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.25);
        text-align: center;
    }
    .kpi-title {
        font-size: 0.85rem;
        color: #94a3b8;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 800;
        margin-bottom: 4px;
    }
    .kpi-sub {
        font-size: 0.8rem;
        color: #64748b;
    }
    .game-card-res {
        background-color: #1a2234;
        border-radius: 10px;
        padding: 14px 18px;
        border-left: 6px solid #3b82f6;
        margin-bottom: 12px;
        border-top: 1px solid #2d3748;
        border-right: 1px solid #2d3748;
        border-bottom: 1px solid #2d3748;
    }
    .game-card-green {
        border-left-color: #10b981 !important;
        background: linear-gradient(90deg, rgba(16, 185, 129, 0.08) 0%, #1a2234 100%);
    }
    .game-card-red {
        border-left-color: #ef4444 !important;
        background: linear-gradient(90deg, rgba(239, 68, 68, 0.08) 0%, #1a2234 100%);
    }
    .game-card-pendente {
        border-left-color: #f59e0b !important;
    }
    .badge-hora {
        background-color: #2563eb;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.85em;
    }
    .badge-metodo {
        background-color: #0f766e;
        color: #f0fdf4;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.8em;
        font-weight: 600;
    }
    .badge-placar {
        background-color: #334155;
        color: #f8fafc;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: 800;
        font-size: 0.9em;
    }
    .badge-green {
        background-color: #059669;
        color: white;
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 800;
    }
    .badge-red {
        background-color: #dc2626;
        color: white;
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 800;
    }
    .badge-pend {
        background-color: #d97706;
        color: white;
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 800;
    }
</style>
""", unsafe_allow_html=True)

# ── Header Oficial ──
st.title("📈 Resultados dos Métodos em Validação Forward — ARKAD")
st.warning("""
🚨 **STATUS DE AUDITORIA & LIQUIDAÇÃO OFICIAL (Betfair Exchange)**
* **Governança:** Liquidação desacoplada pelo status oficial da Betfair Exchange (`placares_ft.csv` / `WINNER`).
* **Matemática do LAY (Lei 4):** Lucro no Green `= +Stake × (1 - Comissão)` · Prejuízo no Red `= -Liability (Risco)` · Break-even WR `= (Odd - 1) / (Odd - 0.05)`.
* **Desacoplamento Rigoroso:** Sinais gerados como `PENDENTE` antes do apito inicial; liquidação realizada após o encerramento do evento.
""")

# ── Sidebar: Parâmetros de Gestão & Filtros ──
st.sidebar.header("⚙️ Configurações de Banca & Risco")
banca_total = st.sidebar.number_input("Banca Total (R$)", min_value=100.0, value=4000.0, step=100.0)

tipo_gestao = st.sidebar.selectbox(
    "Modelo de Gestão de Risco (Liability)",
    options=[
        "🎯 Diferenciada B7 (15% Over 4.5 | 10% em 2x2, 0x3, Away 1X | 5.0% em Home, Draw)",
        "⚖️ Uniforme (5.0% Fixa - R$ 200 por Jogo)",
        "🛡️ Conservadora (2.0% Fixa - R$ 80 por Jogo)",
        "🔒 Protegida (1.0% Fixa - R$ 40 por Jogo)"
    ],
    index=0
)

taxa_comissao = st.sidebar.selectbox(
    "Taxa de Comissão Betfair",
    options=[5.0, 3.5],
    format_func=lambda x: f"{x}% ({'Protocolo Conservador Institucional' if x == 5.0 else 'Fórmula VIP do Usuário (0.965)'})",
    index=0
)
fator_comissao = 1.0 - (taxa_comissao / 100.0)

st.sidebar.markdown("---")
st.sidebar.subheader("📅 Data & Atualização")
data_sel = st.sidebar.date_input("Data da Grade", value=date.today())
ds_str = data_sel.strftime("%Y-%m-%d")

btn_liquidar = st.sidebar.button("🔄 Atualizar Placares & Liquidar Agora", type="primary", use_container_width=True)

# ── Carregamento e Liquidação dos Dados ──
@st.cache_data(ttl=60)
def carregar_e_processar_resultados(data_iso, forcar_sync=False):
    if forcar_sync:
        try:
            sincronizar_ledger_vps()
            liquidar_planilha_dia(data_iso)
        except Exception:
            pass
            
    planilha_path = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_iso}.xlsx"
    if not planilha_path.exists():
        # Tenta carregar coletor se nao existir a principal
        planilha_alt = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_iso}_coletor.xlsx"
        if planilha_alt.exists():
            planilha_path = planilha_alt
        else:
            return pd.DataFrame()
            
    try:
        df = pd.read_excel(planilha_path)
    except Exception:
        return pd.DataFrame()
        
    if df.empty:
        return pd.DataFrame()
        
    # Exclusão definitiva do Lay 0x3
    df = df[~df["Método"].astype(str).str.contains("0x3", case=False, na=False)].copy()
        
    # Dimensionamento de Risco por Método
    def _calc_liab(m):
        m_str = str(m)
        if "Zebra" in m_str or "Micro-Liability" in m_str:
            return min(banca_total * 0.05, 50.0)
        if "Diferenciada" in tipo_gestao:
            if "Over 4.5" in m_str:
                return round(banca_total * 0.15, 2)
            elif "2x2" in m_str or "0x3" in m_str or "3x0" in m_str or "Away" in m_str or "1X" in m_str:
                return round(banca_total * 0.10, 2)
            return round(banca_total * 0.05, 2)
        elif "2.0%" in tipo_gestao:
            return round(banca_total * 0.02, 2)
        elif "1.0%" in tipo_gestao:
            return round(banca_total * 0.01, 2)
        else:
            return round(banca_total * 0.05, 2)
            
    df["Odd_Entrada"] = pd.to_numeric(df.get("Odd_Entrada"), errors="coerce").fillna(5.0)
    df["Risco_Liability_R$"] = df["Método"].apply(_calc_liab)
    df["Stake_Sugerida_R$"] = (df["Risco_Liability_R$"] / (df["Odd_Entrada"] - 1.0)).round(2)
    df["Break_Even_WR%"] = (((df["Odd_Entrada"] - 1.0) / (df["Odd_Entrada"] - (1.0 - fator_comissao))) * 100).round(1)
    
    # PnL em Reais e Unidades
    def _calcular_pnl(row):
        status = str(row.get("Status", "")).upper()
        res = str(row.get("Resultado", "")).upper()
        odd = float(row.get("Odd_Entrada", 5.0))
        stk = float(row.get("Stake_Sugerida_R$", 0.0))
        liab = float(row.get("Risco_Liability_R$", 0.0))
        
        if "GREEN" in res or "GREEN" in status:
            lucro_rs = round(stk * fator_comissao, 2)
            pnl_u = round(fator_comissao / (odd - 1.0), 4)
            return lucro_rs, pnl_u, "🟢 GREEN"
        elif "RED" in res or "RED" in status:
            prej_rs = round(-liab, 2)
            pnl_u = -1.0
            return prej_rs, pnl_u, "🔴 RED"
        else:
            return 0.0, 0.0, "⏳ PENDENTE"
            
    res_list = [ _calcular_pnl(r) for _, r in df.iterrows() ]
    df["PnL_R$"] = [ x[0] for x in res_list ]
    df["PnL_u"] = [ x[1] for x in res_list ]
    df["Status_Formatado"] = [ x[2] for x in res_list ]
    
    # Bloco de Horário
    def _bloco_do_jogo(h):
        h_str = str(h)[:5]
        if h_str < "10:30":
            return "🌅 Bloco 1 (06:00) — Matinal"
        elif h_str < "13:00":
            return "🏆 Bloco 2 (10:30) — Europa 1"
        elif h_str < "15:30":
            return "⚽ Bloco 3 (13:00) — Europa 2"
        elif h_str < "18:00":
            return "⚡ Bloco 4 (15:30) — Tarde / Clássicos"
        else:
            return "🌙 Bloco 5 (18:30) — Noite Américas"
            
    df["Bloco"] = df["Hora"].apply(_bloco_do_jogo)
    return df

if btn_liquidar:
    with st.spinner("Sincronizando com a VPS e liquidando jogos finalizados pelo placar oficial Betfair..."):
        carregar_e_processar_resultados.clear()
        df_dados = carregar_e_processar_resultados(ds_str, forcar_sync=True)
        st.sidebar.success("✅ Placares atualizados e jogos liquidados!")
else:
    df_dados = carregar_e_processar_resultados(ds_str, forcar_sync=False)

if df_dados.empty:
    st.info(f"Nenhum sinal ou planilha encontrada para a data **{ds_str}**. Gere os sinais na Página 04 ou aguarde o próximo bloco.")
    st.stop()

# ── Filtros Secundários na Sidebar ──
st.sidebar.markdown("---")
st.sidebar.subheader("🔍 Filtros de Visualização")
blocos_opts = ["Todos"] + sorted(df_dados["Bloco"].unique().tolist())
filtro_bloco = st.sidebar.selectbox("Filtrar por Bloco", blocos_opts)

metodos_opts = ["Todos"] + sorted(df_dados["Método"].unique().tolist())
filtro_metodo = st.sidebar.selectbox("Filtrar por Método", metodos_opts)

status_opts = ["Todos", "Apenas Liquidados (Greens & Reds)", "Apenas Greens", "Apenas Reds", "Apenas Pendentes"]
filtro_status = st.sidebar.selectbox("Filtrar por Status", status_opts)

# Aplicação dos Filtros
df_view = df_dados.copy()
if filtro_bloco != "Todos":
    df_view = df_view[df_view["Bloco"] == filtro_bloco]
if filtro_metodo != "Todos":
    df_view = df_view[df_view["Método"] == filtro_metodo]
if filtro_status == "Apenas Liquidados (Greens & Reds)":
    df_view = df_view[df_view["Status_Formatado"].isin(["🟢 GREEN", "🔴 RED"])]
elif filtro_status == "Apenas Greens":
    df_view = df_view[df_view["Status_Formatado"] == "🟢 GREEN"]
elif filtro_status == "Apenas Reds":
    df_view = df_view[df_view["Status_Formatado"] == "🔴 RED"]
elif filtro_status == "Apenas Pendentes":
    df_view = df_view[df_view["Status_Formatado"] == "⏳ PENDENTE"]

# ── KPIs Consolidados do Topo ──
df_liq = df_dados[df_dados["Status_Formatado"].isin(["🟢 GREEN", "🔴 RED"])].copy()

total_jogos = len(df_dados)
jogos_liq = len(df_liq)
greens = (df_dados["Status_Formatado"] == "🟢 GREEN").sum()
reds = (df_dados["Status_Formatado"] == "🔴 RED").sum()
pendentes = (df_dados["Status_Formatado"] == "⏳ PENDENTE").sum()

win_rate = (greens / jogos_liq * 100) if jogos_liq > 0 else 0.0
pnl_total_rs = df_liq["PnL_R$"].sum()
pnl_total_u = df_liq["PnL_u"].sum()
risco_total_exposto = df_liq["Risco_Liability_R$"].sum()
roi_yield = (pnl_total_rs / risco_total_exposto * 100) if risco_total_exposto > 0 else 0.0

col_k1, col_k2, col_k3, col_k4, col_k5, col_k6, col_k7 = st.columns(7)

with col_k1:
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Total Jogos</div>
        <div class="kpi-value" style="color: #60a5fa;">{total_jogos}</div>
        <div class="kpi-sub">Grade de {ds_str}</div>
    </div>
    """, unsafe_allow_html=True)

with col_k2:
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Liquidados</div>
        <div class="kpi-value" style="color: #e2e8f0;">{jogos_liq}</div>
        <div class="kpi-sub">{pendentes} pendentes/ao vivo</div>
    </div>
    """, unsafe_allow_html=True)

with col_k3:
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Greens</div>
        <div class="kpi-value" style="color: #10b981;">{greens}</div>
        <div class="kpi-sub">Acertos confirmados</div>
    </div>
    """, unsafe_allow_html=True)

with col_k4:
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Reds</div>
        <div class="kpi-value" style="color: {'#ef4444' if reds > 0 else '#94a3b8'};">{reds}</div>
        <div class="kpi-sub">Erros registrados</div>
    </div>
    """, unsafe_allow_html=True)

with col_k5:
    wr_color = "#10b981" if win_rate >= 85.0 else ("#f59e0b" if win_rate >= 75.0 else "#ef4444")
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Win Rate</div>
        <div class="kpi-value" style="color: {wr_color};">{win_rate:.1f}%</div>
        <div class="kpi-sub">Taxa de Acerto Real</div>
    </div>
    """, unsafe_allow_html=True)

with col_k6:
    pnl_color = "#10b981" if pnl_total_rs > 0 else ("#ef4444" if pnl_total_rs < 0 else "#94a3b8")
    sinal_pnl = "+" if pnl_total_rs > 0 else ""
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Resultado Líquido</div>
        <div class="kpi-value" style="color: {pnl_color};">{sinal_pnl}R$ {pnl_total_rs:,.2f}</div>
        <div class="kpi-sub">{sinal_pnl}{pnl_total_u:.2f} unidades (u)</div>
    </div>
    """, unsafe_allow_html=True)

with col_k7:
    roi_color = "#10b981" if roi_yield > 0 else ("#ef4444" if roi_yield < 0 else "#94a3b8")
    sinal_roi = "+" if roi_yield > 0 else ""
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-title">Yield / ROI Real</div>
        <div class="kpi-value" style="color: {roi_color};">{sinal_roi}{roi_yield:.2f}%</div>
        <div class="kpi-sub">Sobre Liability em Risco</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ── Tabs de Visualização ──
tab_blocos, tab_metodos, tab_equity, tab_tabela = st.tabs([
    "⚡ Visão por Horário & Blocos",
    "📊 Desempenho por Método",
    "📈 Curva de Equity Intraday",
    "📋 Tabela Analítica Completa"
])

# ── TAB 1: VISÃO POR HORÁRIO & BLOCOS ──
with tab_blocos:
    st.subheader(f"⚡ Monitoramento dos Jogos por Horário — Grade de {ds_str}")
    
    # Agrupa por Bloco
    blocos_presentes = sorted(df_view["Bloco"].unique().tolist())
    for blk in blocos_presentes:
        df_b = df_view[df_view["Bloco"] == blk].copy()
        if df_b.empty:
            continue
            
        b_liq = df_b[df_b["Status_Formatado"].isin(["🟢 GREEN", "🔴 RED"])]
        b_gr = (df_b["Status_Formatado"] == "🟢 GREEN").sum()
        b_rd = (df_b["Status_Formatado"] == "🔴 RED").sum()
        b_pnl = b_liq["PnL_R$"].sum()
        
        cor_pnl_b = "#10b981" if b_pnl > 0 else ("#ef4444" if b_pnl < 0 else "#94a3b8")
        sinal_b = "+" if b_pnl > 0 else ""
        
        with st.expander(f"**{blk}** — {len(df_b)} jogos ({b_gr} Greens | {b_rd} Reds) | P&L: **{sinal_b}R$ {b_pnl:,.2f}**", expanded=True):
            for _, j in df_b.iterrows():
                st_format = j.get("Status_Formatado", "⏳ PENDENTE")
                card_cls = "game-card-green" if "GREEN" in st_format else ("game-card-red" if "RED" in st_format else "game-card-pendente")
                badge_res = '<span class="badge-green">🟢 GREEN</span>' if "GREEN" in st_format else ('<span class="badge-red">🔴 RED</span>' if "RED" in st_format else '<span class="badge-pend">⏳ EM ANDAMENTO</span>')
                
                placar_txt = str(j.get("Placar", "")).strip()
                if not placar_txt or placar_txt == "nan":
                    placar_badge = '<span class="badge-placar">Ao Vivo / Pendente</span>'
                else:
                    placar_badge = f'<span class="badge-placar">Placar Final: {placar_txt}</span>'
                    
                odd_ent = float(j.get("Odd_Entrada", 0.0))
                stk_val = float(j.get("Stake_Sugerida_R$", 0.0))
                liab_val = float(j.get("Risco_Liability_R$", 0.0))
                pnl_j = float(j.get("PnL_R$", 0.0))
                pnl_txt = f"+R$ {pnl_j:,.2f}" if pnl_j > 0 else (f"-R$ {abs(pnl_j):,.2f}" if pnl_j < 0 else "R$ 0,00")
                pnl_cor = "#10b981" if pnl_j > 0 else ("#ef4444" if pnl_j < 0 else "#94a3b8")
                
                st.markdown(f"""
                <div class="game-card-res {card_cls}">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <div>
                            <span class="badge-hora">{j.get('Hora', '')}</span>
                            <span style="color: #94a3b8; font-size: 0.9em; margin-left: 8px;">🏆 <b>{j.get('Liga', '')}</b></span>
                        </div>
                        <div>
                            {placar_badge}
                            {badge_res}
                        </div>
                    </div>
                    <div style="font-size: 1.15rem; font-weight: 700; color: #f8fafc; margin-bottom: 6px;">
                        ⚽ {j.get('Jogo', '')}
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 0.88rem; color: #cbd5e1; flex-wrap: wrap; gap: 8px;">
                        <div>
                            <span class="badge-metodo">📌 {j.get('Método', '')}</span>
                            <span style="margin-left: 6px;">Odd Lay: <b>{odd_ent:.2f}</b></span>
                            <span style="color: #64748b; margin-left: 6px;">(Break-even: {j.get('Break_Even_WR%', 0.0):.1f}%)</span>
                        </div>
                        <div>
                            <span>Risco: <b>R$ {liab_val:.2f}</b></span>
                            <span style="margin-left: 8px;">Stake: <b>R$ {stk_val:.2f}</b></span>
                            <span style="margin-left: 10px; font-weight: 800; color: {pnl_cor}; font-size: 1.0rem;">➔ P&L: {pnl_txt}</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

# ── TAB 2: DESEMPENHO POR MÉTODO ──
with tab_metodos:
    st.subheader("📊 Desempenho Segmentado por Método")
    
    linhas_met = []
    for met, g in df_dados.groupby("Método"):
        g_liq = g[g["Status_Formatado"].isin(["🟢 GREEN", "🔴 RED"])]
        n_tot = len(g)
        n_liq = len(g_liq)
        gr = (g["Status_Formatado"] == "🟢 GREEN").sum()
        rd = (g["Status_Formatado"] == "🔴 RED").sum()
        pnd = (g["Status_Formatado"] == "⏳ PENDENTE").sum()
        wr = (gr / n_liq * 100) if n_liq > 0 else 0.0
        be_wr = g["Break_Even_WR%"].median()
        pnl_rs = g_liq["PnL_R$"].sum()
        pnl_u = g_liq["PnL_u"].sum()
        risco_tot = g_liq["Risco_Liability_R$"].sum()
        roi_m = (pnl_rs / risco_tot * 100) if risco_tot > 0 else 0.0
        
        linhas_met.append({
            "Método": met,
            "Total Jogos": n_tot,
            "Liquidados": n_liq,
            "Greens": gr,
            "Reds": rd,
            "Pendentes": pnd,
            "Win Rate %": f"{wr:.1f}%",
            "Break-Even WR %": f"{be_wr:.1f}%",
            "P&L (Unidades)": f"{'+' if pnl_u > 0 else ''}{pnl_u:.2f} u",
            "P&L (R$)": f"{'+' if pnl_rs > 0 else ''}R$ {pnl_rs:,.2f}",
            "ROI / Yield %": f"{'+' if roi_m > 0 else ''}{roi_m:.2f}%"
        })
        
    df_met_res = pd.DataFrame(linhas_met)
    st.dataframe(df_met_res, use_container_width=True, hide_index=True)

# ── TAB 3: CURVA DE EQUITY INTRADAY ──
with tab_equity:
    st.subheader("📈 Curva de Equity Intraday (Evolução Acumulada do Dia)")
    
    df_eq = df_dados.sort_values("Hora").copy()
    df_eq_liq = df_eq[df_eq["Status_Formatado"].isin(["🟢 GREEN", "🔴 RED"])].copy()
    
    if not df_eq_liq.empty:
        df_eq_liq["P&L_Acumulado_R$"] = df_eq_liq["PnL_R$"].cumsum()
        df_eq_liq["Jogos_Finalizados"] = range(1, len(df_eq_liq) + 1)
        
        st.line_chart(df_eq_liq.set_index("Jogos_Finalizados")["P&L_Acumulado_R$"])
        
        col_eq1, col_eq2, col_eq3 = st.columns(3)
        with col_eq1:
            st.metric("Pico Máximo de Lucro do Dia", f"R$ {df_eq_liq['P&L_Acumulado_R$'].max():,.2f}")
        with col_eq2:
            st.metric("Drawdown Máximo Intraday", f"R$ {df_eq_liq['P&L_Acumulado_R$'].min():,.2f}")
        with col_eq3:
            st.metric("Fechamento Atual", f"R$ {df_eq_liq['P&L_Acumulado_R$'].iloc[-1]:,.2f}")
    else:
        st.info("Aguardando finalização do primeiro jogo do dia para gerar a curva de equity.")

# ── TAB 4: TABELA ANALÍTICA COMPLETA ──
with tab_tabela:
    st.subheader("📋 Tabela Analítica Completa com Exportação")
    
    cols_tab = [
        "Hora", "Liga", "Jogo", "Método", "Odd_Entrada", "Break_Even_WR%",
        "Risco_Liability_R$", "Stake_Sugerida_R$", "Placar", "Resultado", "PnL_R$", "Status_Formatado"
    ]
    cols_existentes = [ c for c in cols_tab if c in df_view.columns ]
    df_export = df_view[cols_existentes].copy()
    
    st.dataframe(df_export, use_container_width=True, hide_index=True)
    
    # Download Excel
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Resultados_Forward")
    st.download_button(
        label="📥 Baixar Tabela Consolidada em Excel",
        data=buffer.getvalue(),
        file_name=f"Resultados_ARKAD_{ds_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
