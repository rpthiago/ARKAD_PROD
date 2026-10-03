# -*- coding: utf-8 -*-
"""
04_⚡_Radar_Jogos_do_Dia.py — Radar de Jogos do Dia em 5 Horários Estratégicos (Betfair Exchange Oficial)

Central de Monitoramento e Operação dos Sinais Oficiais:
- Conexão direta com o Coletor da VPS (odds de Lay reais e liquidez da Betfair).
- Segmentação em 5 Blocos de Horário para respeitar o tempo de consolidação da liquidez.
- Dimensionamento automático de stakes com base na banca e gestão de risco do usuário.
- Acompanhamento ao vivo de status (Pendente / Liquidado / Green / Red).
- Disparo sob demanda de boletins formatados no Telegram.
"""
import io
import os
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# Configuração da Página
st.set_page_config(
    page_title="ARKAD — Radar de Jogos do Dia",
    page_icon="⚡",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sinais_dia_coletor import gerar_planilha_do_coletor, sincronizar_ledger_vps
from telegram_notifier import enviar_mensagem_telegram, enviar_documento_telegram

# ── Estilização Visual ──
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border-radius: 10px;
        padding: 16px 20px;
        border: 1px solid #334155;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .game-card {
        background-color: #1a2234;
        border-radius: 10px;
        padding: 16px;
        border-left: 5px solid #3b82f6;
        margin-bottom: 14px;
        border-top: 1px solid #2d3748;
        border-right: 1px solid #2d3748;
        border-bottom: 1px solid #2d3748;
    }
    .game-card-green {
        border-left-color: #10b981;
    }
    .game-card-red {
        border-left-color: #ef4444;
    }
    .badge-hora {
        background-color: #2563eb;
        color: white;
        padding: 4px 10px;
        border-radius: 6px;
        font-weight: bold;
        font-size: 0.9em;
    }
    .badge-metodo {
        background-color: #0f766e;
        color: #f0fdf4;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.85em;
        font-weight: 600;
    }
    .badge-status-pendente {
        background-color: #d97706;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.8em;
    }
    .badge-status-green {
        background-color: #059669;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.8em;
        font-weight: bold;
    }
    .badge-status-red {
        background-color: #dc2626;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.8em;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ── Definição dos 5 Blocos de Horários ──
BLOCOS = {
    1: {"nome": "🌅 Bloco 1 (06:00) — Matinal", "inicio": "06:00", "fim": "10:30", "desc": "Ásia, Austrália e Copas Matutinas"},
    2: {"nome": "🏆 Bloco 2 (10:30) — Europa 1", "inicio": "10:30", "fim": "13:00", "desc": "Premier League, Championship, Escócia"},
    3: {"nome": "⚽ Bloco 3 (13:00) — Europa 2", "inicio": "13:00", "fim": "15:30", "desc": "Bundesliga, Serie A, La Liga, Ligue 1"},
    4: {"nome": "⚡ Bloco 4 (15:30) — Tarde / Clássicos", "inicio": "15:30", "fim": "18:00", "desc": "Grandes jogos europeus e Séries B/C"},
    5: {"nome": "🌙 Bloco 5 (18:30) — Noite Américas", "inicio": "18:00", "fim": "23:59", "desc": "Brasileirão Série A, Libertadores, MLS, Liga MX"}
}

def obter_bloco_atual():
    agora = datetime.now().strftime("%H:%M")
    if agora < "10:30":
        return 1
    elif agora < "13:00":
        return 2
    elif agora < "15:30":
        return 3
    elif agora < "18:30":
        return 4
    else:
        return 5

# ── Sidebar: Configuração de Banca e Filtros ──
st.sidebar.header("⚙️ Gestão de Banca & Perfil")
banca_total = st.sidebar.number_input("Banca Total (R$)", min_value=100.0, value=4000.0, step=100.0)
perfil_stake = st.sidebar.selectbox("Risco Máx por Entrada (Liability)", [
    "🎯 Diferenciada B7 (15% Over 4.5 | 10% em 2x2, 0x3, 3x0, Away 1X | 5% em Draw, Home)",
    "Firme (5.0% da banca - R$ 200)",
    "Moderado (2.0% da banca - R$ 80)",
    "Conservador (1.0% da banca - R$ 40)",
    "Protegido (0.5% da banca - R$ 20)"
], index=0)

if "0.5%" in perfil_stake:
    pct_risco = 0.005
elif "1.0%" in perfil_stake:
    pct_risco = 0.010
elif "2.0%" in perfil_stake:
    pct_risco = 0.020
else:
    pct_risco = 0.050

liability_base = banca_total * pct_risco

st.sidebar.markdown("---")
st.sidebar.subheader("📅 Parâmetros de Consulta")
data_selecionada = st.sidebar.date_input("Data dos Jogos", value=date.today())
ds_iso = data_selecionada.strftime("%Y-%m-%d")

btn_sincronizar = st.sidebar.button("🔄 Sincronizar Odds da VPS Agora", type="primary", use_container_width=True)

# ── Carregamento de Dados com Cache & Sincronização ──
@st.cache_data(ttl=60)
def carregar_dados_jogos(data_str, force_sync=False):
    # Tenta sempre buscar as odds mais frescas da VPS (executa em 2s via SCP)
    try:
        sincronizar_ledger_vps()
    except Exception:
        pass
    df = gerar_planilha_do_coletor(data_str, sync_vps=False)
    return df

if btn_sincronizar:
    with st.spinner("Puxando últimos registros de odds da Betfair direto da VPS..."):
        carregar_dados_jogos.clear()
        df_raw = carregar_dados_jogos(ds_iso, force_sync=True)
        st.sidebar.success("✅ Odds e sinais sincronizados da VPS!")
else:
    df_raw = carregar_dados_jogos(ds_iso, force_sync=False)

# ── Título & Cabeçalho ──
bloco_atual_id = obter_bloco_atual()
bloco_atual_info = BLOCOS[bloco_atual_id]
hora_agora = datetime.now().strftime("%H:%M:%S")

st.title("⚡ Radar de Jogos do Dia — Betfair Exchange")
st.markdown(f"""
Monitoramento oficial de entradas nos **5 Horários Estratégicos**. As odds são capturadas diretamente na **API oficial da Betfair (VPS KO−10)**, 
garantindo que cada jogo seja avaliado no momento em que a liquidez se consolida.
""")

col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.info(f"🕒 **Horário Atual:** `{hora_agora}` (Brasília) | 🟢 **Janela Ativa:** **{bloco_atual_info['nome']}** (`{bloco_atual_info['inicio']}` às `{bloco_atual_info['fim']}`)")
with col_h2:
    st.caption("📡 **Fonte de Odds:** `Betfair Exchange Oficial (VPS)`\n\n🛡️ **Liquidação:** `Runner Status Betfair (WINNER/LOSER)`")

# ── Processamento de Stakes & Risco ──
if not df_raw.empty:
    df = df_raw.copy()
    # Exclusão definitiva do Lay 0x3
    df = df[~df["Método"].astype(str).str.contains("0x3", case=False, na=False)].copy()
    
    def _calcular_liability(metodo):
        m = str(metodo)
        if "Zebra" in m or "Micro-Liability" in m:
            return min(liability_base, 50.0)
        if "Diferenciada" in perfil_stake:
            if "Over 4.5" in m:
                return round(banca_total * 0.15, 2)
            elif "0x3" in m or "3x0" in m or "2x2" in m or "Away" in m or "1X" in m:
                return round(banca_total * 0.10, 2)
            else:
                return round(banca_total * 0.05, 2)
        return liability_base

    df["Odd_Entrada"] = pd.to_numeric(df["Odd_Entrada"], errors="coerce")
    df["Risco_Red_R$"] = df["Método"].apply(_calcular_liability)
    df["Stake_Sugerida_R$"] = np.where(df["Odd_Entrada"] > 1.0, (df["Risco_Red_R$"] / (df["Odd_Entrada"] - 1.0)).round(2), 0.0)
    df["Lucro_Green_R$"] = (df["Stake_Sugerida_R$"] * 0.955).round(2)
    df["Hora_Str"] = df["Hora"].astype(str).str[:5]
    
    # Filtros da Sidebar
    metodos_unicos = sorted(df["Método"].dropna().unique().tolist())
    filtro_metodos = st.sidebar.multiselect("Filtrar por Método", metodos_unicos, default=metodos_unicos)
    
    status_opcoes = ["Todos", "⏳ Apenas Pendentes", "✅ Apenas Liquidados"]
    filtro_status = st.sidebar.selectbox("Filtrar por Status", status_opcoes, index=0)
    
    df_filtrado = df[df["Método"].isin(filtro_metodos)].copy()
    if filtro_status == "⏳ Apenas Pendentes":
        df_filtrado = df_filtrado[df_filtrado["Resultado"] == "PENDENTE"]
    elif filtro_status == "✅ Apenas Liquidados":
        df_filtrado = df_filtrado[df_filtrado["Resultado"] != "PENDENTE"]
        
    # ── Cards de Métricas Consolidadas ──
    total_jogos = len(df)
    total_pendentes = (df["Resultado"] == "PENDENTE").sum()
    total_liquidados = total_jogos - total_pendentes
    greens = (df["Resultado"] == "GREEN").sum()
    reds = (df["Resultado"] == "RED").sum()
    
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total de Entradas Hoje", f"{total_jogos} jogos", f"Data: {ds_iso}")
    with m2:
        st.metric("Entradas Pendentes", f"{total_pendentes} jogos", "Aguardando Apito")
    with m3:
        st.metric("Entradas Concluídas", f"{total_liquidados} jogos", f"{greens} Green / {reds} Red")
    with m4:
        wr_dia = (greens / total_liquidados * 100) if total_liquidados > 0 else 0.0
        st.metric("Taxa de Acerto (Hoje)", f"{wr_dia:.1f}%", f"{greens} de {total_liquidados} resolvidos")

    st.markdown("---")

    # ── Tabs por Bloco de Horários ──
    tab_atual, tab_b1, tab_b2, tab_b3, tab_b4, tab_b5, tab_todos = st.tabs([
        f"⚡ Bloco Atual ({bloco_atual_info['inicio']}-{bloco_atual_info['fim']})",
        "🌅 1. Matinal (06h)",
        "🏆 2. Europa 1 (10h30)",
        "⚽ 3. Europa 2 (13h)",
        "⚡ 4. Tarde (15h30)",
        "🌙 5. Noite (18h30)",
        f"📋 Todos do Dia ({total_jogos})"
    ])

    def renderizar_jogos_bloco(df_subset, titulo_bloco, desc_bloco, show_telegram_btn=True):
        if df_subset.empty:
            st.info(f"Nenhum jogo qualificado registrado para o **{titulo_bloco}** nesta data.")
            return

        st.subheader(f"{titulo_bloco} — {len(df_subset)} entradas")
        st.caption(desc_bloco)
        
        # Modo de Visualização: Cards ou Tabela
        col_vis1, col_vis2 = st.columns([1, 1])
        with col_vis1:
            modo_view = st.radio("Visualização", ["🗂️ Cards Detalhados", "📊 Tabela Completa"], horizontal=True, key=f"view_{titulo_bloco}")
            
        with col_vis2:
            if show_telegram_btn:
                if st.button(f"📲 Disparar {len(df_subset)} jogos no Telegram", key=f"tg_{titulo_bloco}", use_container_width=True):
                    msg_linhas = [
                        f"🎯 *ARKAD — RADAR DE JOGOS ({ds_iso})*",
                        f"⚡ *{titulo_bloco}*",
                        f"💰 *Banca:* R$ {banca_total:,.2f} | 🛡️ *Risco:* R$ {liability_base:.2f}",
                        f"📊 *Entradas:* {len(df_subset)} jogos\n",
                        "━━━━━━━━━━━━━━━━━━━━━━━"
                    ]
                    for _, s in df_subset.iterrows():
                        msg_linhas.append(
                            f"⏰ `{s.get('Hora', '')}` | 🏆 *{s.get('Liga', '')}*\n"
                            f"⚽ *{s.get('Jogo', '')}*\n"
                            f"📌 *{s.get('Método', '')}* (Odd Lay: `{float(s.get('Odd_Entrada') or 0):.2f}`)\n"
                            f"💵 *Stake:* `R$ {float(s.get('Stake_Sugerida_R$') or 0):.2f}` ➔ *Lucro:* `+R$ {float(s.get('Lucro_Green_R$') or 0):.2f}`\n"
                            f"Status: {s.get('Status', '⏳ PENDENTE')}\n"
                            "───────────────────────"
                        )
                    ok_m, res_m = enviar_mensagem_telegram("\n".join(msg_linhas), force=True)
                    if ok_m:
                        st.success("✅ Boletim do bloco disparado no seu Telegram!")
                    else:
                        st.error(f"Erro Telegram: {res_m}")

        if modo_view == "🗂️ Cards Detalhados":
            for _, r in df_subset.iterrows():
                res = str(r.get("Resultado", "PENDENTE"))
                card_class = "game-card"
                badge_res = '<span class="badge-status-pendente">⏳ PENDENTE</span>'
                if res == "GREEN":
                    card_class = "game-card game-card-green"
                    badge_res = '<span class="badge-status-green">🟢 GREEN</span>'
                elif res == "RED":
                    card_class = "game-card game-card-red"
                    badge_res = '<span class="badge-status-red">🔴 RED</span>'

                liq = float(r.get("Liquidez_Lay") or 0.0)
                liq_txt = f"R$ {liq:,.0f}" if liq > 0 else "Profunda"
                fav_odd = float(r.get("Odd_Fav") or 0.0)
                fav_txt = f" · Fav @{fav_odd:.2f}" if fav_odd > 0 else ""

                st.markdown(f"""
                <div class="{card_class}">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <div>
                            <span class="badge-hora">⏰ {r.get('Hora', '')}</span>
                            <span style="font-weight: 600; color: #94a3b8; margin-left: 8px;">🏆 {r.get('Liga', '')}</span>
                        </div>
                        <div>
                            {badge_res}
                        </div>
                    </div>
                    <div style="font-size: 1.15em; font-weight: bold; color: #f8fafc; margin-bottom: 6px;">
                        ⚽ {r.get('Jogo', '')}
                    </div>
                    <div style="display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 10px;">
                        <span class="badge-metodo">📌 {r.get('Método', '')}</span>
                        <span style="color: #cbd5e1;">Odd Lay: <b>{float(r.get('Odd_Entrada') or 0):.2f}</b>{fav_txt}</span>
                        <span style="color: #64748b;">|</span>
                        <span style="color: #94a3b8;">Fila Betfair: <b>{liq_txt}</b></span>
                    </div>
                    <div style="background-color: #0f172a; padding: 10px 14px; border-radius: 6px; display: flex; justify-content: space-between; align-items: center;">
                        <span style="color: #94a3b8;">💵 Stake Sugerida: <b style="color: #38bdf8;">R$ {float(r.get('Stake_Sugerida_R$') or 0):.2f}</b></span>
                        <span style="color: #94a3b8;">🛡️ Risco Máx (Red): <b style="color: #f87171;">R$ {float(r.get('Risco_Red_R$') or 0):.2f}</b></span>
                        <span style="color: #94a3b8;"> Lucro Potencial (Green): <b style="color: #4ade80;">+R$ {float(r.get('Lucro_Green_R$') or 0):.2f}</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            cols_tabela = [
                "Hora", "Liga", "Jogo", "Método", "Odd_Entrada", "Odd_Fav", 
                "Stake_Sugerida_R$", "Lucro_Green_R$", "Risco_Red_R$", "Liquidez_Lay", "Status", "Placar"
            ]
            cols_show = [c for c in cols_tabela if c in df_subset.columns]
            st.dataframe(df_subset[cols_show], use_container_width=True, hide_index=True)

    # 1. Tab Bloco Atual
    with tab_atual:
        df_atual = df_filtrado[
            (df_filtrado["Hora_Str"] >= bloco_atual_info["inicio"]) & 
            (df_filtrado["Hora_Str"] <= bloco_atual_info["fim"])
        ]
        renderizar_jogos_bloco(df_atual, bloco_atual_info["nome"], bloco_atual_info["desc"])

    # 2. Tab Bloco 1 (Matinal)
    with tab_b1:
        b1_info = BLOCOS[1]
        df_b1 = df_filtrado[(df_filtrado["Hora_Str"] >= b1_info["inicio"]) & (df_filtrado["Hora_Str"] <= b1_info["fim"])]
        renderizar_jogos_bloco(df_b1, b1_info["nome"], b1_info["desc"])

    # 3. Tab Bloco 2 (Europa 1)
    with tab_b2:
        b2_info = BLOCOS[2]
        df_b2 = df_filtrado[(df_filtrado["Hora_Str"] >= b2_info["inicio"]) & (df_filtrado["Hora_Str"] <= b2_info["fim"])]
        renderizar_jogos_bloco(df_b2, b2_info["nome"], b2_info["desc"])

    # 4. Tab Bloco 3 (Europa 2)
    with tab_b3:
        b3_info = BLOCOS[3]
        df_b3 = df_filtrado[(df_filtrado["Hora_Str"] >= b3_info["inicio"]) & (df_filtrado["Hora_Str"] <= b3_info["fim"])]
        renderizar_jogos_bloco(df_b3, b3_info["nome"], b3_info["desc"])

    # 5. Tab Bloco 4 (Tarde / Clássicos)
    with tab_b4:
        b4_info = BLOCOS[4]
        df_b4 = df_filtrado[(df_filtrado["Hora_Str"] >= b4_info["inicio"]) & (df_filtrado["Hora_Str"] <= b4_info["fim"])]
        renderizar_jogos_bloco(df_b4, b4_info["nome"], b4_info["desc"])

    # 6. Tab Bloco 5 (Noite Américas)
    with tab_b5:
        b5_info = BLOCOS[5]
        df_b5 = df_filtrado[(df_filtrado["Hora_Str"] >= b5_info["inicio"]) & (df_filtrado["Hora_Str"] <= b5_info["fim"])]
        renderizar_jogos_bloco(df_b5, b5_info["nome"], b5_info["desc"])

    # 7. Tab Todos do Dia
    with tab_todos:
        renderizar_jogos_bloco(df_filtrado, f"Todos os Jogos do Dia ({ds_iso})", "Grade consolidada completa de todas as janelas horárias", show_telegram_btn=True)

    # ── Exportação Excel ──
    st.markdown("---")
    col_exp1, col_exp2 = st.columns([2, 1])
    with col_exp1:
        st.subheader("📥 Exportar Grade Completa do Dia")
        st.caption("Planilha formatada pronta com todas as entradas, stakes dimensionadas e resultados.")
    with col_exp2:
        _buf = io.BytesIO()
        with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
            df.to_excel(_writer, index=False, sheet_name="Sinais_Oficiais")
        st.download_button(
            label="📥 Baixar Planilha Consolidada (.xlsx)",
            data=_buf.getvalue(),
            file_name=f"Sinais_Metodos_Aprovados_{ds_iso}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True
        )

else:
    st.warning(f"Nenhum sinal encontrado para {ds_iso}. Clique no botão abaixo para puxar as odds da VPS agora.")
    if st.button("🔄 Puxar Jogos da VPS Agora", type="primary"):
        sincronizar_ledger_vps()
        st.rerun()
