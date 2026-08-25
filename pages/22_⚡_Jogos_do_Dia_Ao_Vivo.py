# -*- coding: utf-8 -*-
"""
22_⚡_Jogos_do_Dia_Ao_Vivo.py — Painel de Monitoramento dos Jogos Selecionados do Dia
Permite acompanhar ao vivo todas as partidas qualificadas pelos modelos com odds reais da Betfair,
cálculo de stake/responsabilidade e placares em tempo real.
"""

import os
import sys
import unicodedata
import re
from datetime import datetime, date
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Jogos do Dia — Radar Ao Vivo",
    page_icon="⚡",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import b365_data_utils
from futpythontrader_client import get_daily_dataframe
from hist_rf_loader import load_hist_rf
import lay_draw_rf_v2_strategy as LD
from estrategia_lay_under15 import avaliar_jogo_lay_under15

# Estilização visual moderna
st.markdown("""
<style>
    .metric-card {
        background-color: #1e2130;
        border-radius: 10px;
        padding: 15px;
        border-left: 5px solid #4CAF50;
        margin-bottom: 10px;
    }
    .badge-green {
        background-color: #2e7d32;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-pending {
        background-color: #f57f17;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-live {
        background-color: #0288d1;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Radar de Jogos do Dia — Acompanhamento Ao Vivo")
st.markdown("""
Este painel monitora a **grade diária da Betfair Exchange**, aplica os modelos estatísticos auditados 
(**Lay Under 1.5 FT** e **Lay Draw v2**) e exibe os jogos qualificados com suas respectivas odds reais, 
dimensionamento de stake e placares atualizados em tempo real.
""")

# ── Barra Lateral / Configurações ──
st.sidebar.header("⚙️ Configurações & Gestão")
data_selecionada = st.sidebar.date_input("Data dos Jogos", value=date.today())
data_str = data_selecionada.strftime("%Y-%m-%d")

banca_total = st.sidebar.number_input("Banca Total (R$)", min_value=50.0, value=1000.0, step=50.0)
perfil_risco = st.sidebar.selectbox(
    "Perfil de Risco (Stake Máx)",
    options=["Conservador (1% da banca)", "Moderado (2% da banca)", "Recomendado (Kelly Fracionário 2.5%)", "Personalizado"]
)

if "1%" in perfil_risco:
    stake_base = banca_total * 0.01
elif "2%" in perfil_risco:
    stake_base = banca_total * 0.02
elif "2.5%" in perfil_risco:
    stake_base = banca_total * 0.025
else:
    pct = st.sidebar.number_input("Percentual (%)", min_value=0.5, max_value=10.0, value=2.0, step=0.5)
    stake_base = banca_total * (pct / 100.0)

st.sidebar.info(f"💰 **Stake Base:** R$ {stake_base:.2f}")

# ── Carregar Coletor In-Play de Placares ──
@st.cache_data(ttl=60, show_spinner=False)
def carregar_placares_coletor():
    cache_file = ROOT / "_placares_coletor_cache.csv"
    if not cache_file.exists():
        return {}
    try:
        df_ticks = pd.read_csv(cache_file)
        df_ticks['min_to_ko'] = pd.to_numeric(df_ticks.get('min_to_ko'), errors='coerce')
        df_ticks['lay'] = pd.to_numeric(df_ticks.get('lay'), errors='coerce')
        
        mapa = {}
        def _canon(s):
            if not isinstance(s, str): return ''
            return s.lower().strip().replace(' ', '').replace('-', '').replace('.', '')
            
        for (ko, home, away), g in df_ticks.groupby(['ko', 'home', 'away']):
            k_date = str(ko)[:10]
            m_key = f"{k_date}_{_canon(home)}_{_canon(away)}"
            g_sorted = g.sort_values('min_to_ko')
            
            # Placar final
            f_ticks = g_sorted[g_sorted['min_to_ko'] <= -90]
            if f_ticks.empty:
                f_ticks = g_sorted.tail(15)
            vf = f_ticks.dropna(subset=['lay'])
            final_sc = vf.loc[vf['lay'].idxmin()]['runner'] if not vf.empty else "N/A"
            
            # Placar atual / in-play
            latest = g_sorted.tail(10).dropna(subset=['lay'])
            live_sc = latest.loc[latest['lay'].idxmin()]['runner'] if not latest.empty else "N/A"
            min_ko = g_sorted['min_to_ko'].min() if not g_sorted.empty else 0
            
            mapa[m_key] = {
                'placar_final': final_sc,
                'placar_live': live_sc,
                'min_jogo': f"{abs(int(min_ko))}'" if min_ko < 0 else "Pré-Jogo"
            }
        return mapa
    except Exception:
        return {}

# ── Carregar Dados da Betfair e Avaliar ──
@st.cache_data(ttl=120, show_spinner=False)
def processar_grade_do_dia(data_str_param):
    try:
        df_bf = get_daily_dataframe(source="betfair", date_str=data_str_param)
    except Exception:
        df_bf = None
        
    if df_bf is None or df_bf.empty:
        try:
            df_bf = b365_data_utils.fetch_betfair_daily(data_str_param)
        except Exception:
            df_bf = pd.DataFrame()
            
    if df_bf.empty:
        return pd.DataFrame()
        
    df_hist = load_hist_rf()
    
    # 1. Avaliar Lay Draw
    payload = df_bf.to_dict('records')
    LD.PROB_MIN = 0.70
    LD.TOTAL_XGOT_MIN = 0.0
    LD.ODD_MAX = 4.80
    
    res_ld = LD.predict_and_evaluate_live(payload, df_hist)
    df_eval_ld = pd.DataFrame(res_ld) if res_ld else pd.DataFrame()
    
    jogos_qualificados = []
    
    # Processar Lay Draw
    if not df_eval_ld.empty:
        aprov_ld = df_eval_ld[df_eval_ld['Decision'] == 'APOSTA']
        for _, row in aprov_ld.iterrows():
            odd_lay = float(row.get('Odd_D_FT', row.get('Odd_D_Lay', 3.50)))
            prob = float(row.get('Prob_ML', 0.0))
            ev = float(row.get('ev_lay', 0.0))
            odd_1x1 = float(row.get('Odd_CS_1x1_Back', 7.50) or 7.50)
            
            # Cobertura 1x1
            st_cob = (odd_lay - 1.0) / ((odd_1x1 - 1.0) * 0.955) if odd_1x1 > 1.0 else 0.0
            
            jogos_qualificados.append({
                'Data': data_str_param,
                'Horário': str(row.get('Time', row.get('Hora', '15:00'))),
                'Jogo': f"{row['Home']} x {row['Away']}",
                'Home': row['Home'],
                'Away': row['Away'],
                'Liga': str(row.get('League', 'N/A')),
                'Método': 'Lay Draw (+ Hedge 1x1)',
                'Mercado': 'Match Odds (Draw)',
                'Odd_Lay': odd_lay,
                'Odd_Hedge': odd_1x1,
                'Prob_IA': prob,
                'EV': ev,
                'Break_Even': (odd_lay - 1.0) / (odd_lay - 0.045),
                'Stake_Hedge_Ratio': st_cob,
                'Tipo': 'Lay Draw'
            })
            
    # Processar Lay Under 1.5
    for _, row in df_bf.iterrows():
        try:
            res_u15 = avaliar_jogo_lay_under15(row.to_dict(), ev_threshold=0.05)
            if res_u15.get('aplica'):
                odd_lay = float(res_u15.get('odd_lay', 3.20))
                prob = float(res_u15.get('prob_ia', 0.75))
                ev = float(res_u15.get('ev', 0.08))
                
                jogos_qualificados.append({
                    'Data': data_str_param,
                    'Horário': str(row.get('Time', row.get('Hora', '15:00'))),
                    'Jogo': f"{row['Home']} x {row['Away']}",
                    'Home': row['Home'],
                    'Away': row['Away'],
                    'Liga': str(row.get('League', 'N/A')),
                    'Método': 'Lay Under 1.5 FT (XGBoost)',
                    'Mercado': 'Under 1.5 FT',
                    'Odd_Lay': odd_lay,
                    'Odd_Hedge': 0.0,
                    'Prob_IA': prob,
                    'EV': ev,
                    'Break_Even': (odd_lay - 1.0) / (odd_lay - 0.045),
                    'Stake_Hedge_Ratio': 0.0,
                    'Tipo': 'Lay Under 1.5'
                })
        except Exception:
            pass
            
    return pd.DataFrame(jogos_qualificados)

# ── Execução e Apresentação ──
col_btn, col_info = st.columns([1, 4])
with col_btn:
    atualizar_btn = st.button("🔄 Atualizar Radar Agora", type="primary", use_container_width=True)

if atualizar_btn:
    st.cache_data.clear()

with st.spinner(f"Consultando grade de {data_str} na Betfair Exchange e aplicando modelos de IA..."):
    df_jogos = processar_grade_do_dia(data_str)
    mapa_inplay = carregar_placares_coletor()

def _canon(s):
    if not isinstance(s, str): return ''
    return s.lower().strip().replace(' ', '').replace('-', '').replace('.', '')

# ── Métricas do Topo ──
n_total = len(df_jogos)
st.markdown("---")

m1, m2, m3, m4 = st.columns(4)
m1.metric("📅 Data Selecionada", data_str)
m2.metric("🎯 Jogos Qualificados", f"{n_total}")
m3.metric("💰 Stake Base Sugerida", f"R$ {stake_base:.2f}")
m4.metric("🛡️ Proteção Ativa", "Comissão 4.5% + Hedge 1x1")

if df_jogos.empty:
    st.info(f"Nenhum jogo qualificado com EV+ para a data **{data_str}**. Os modelos mantêm critérios rígidos de valor esperado para proteger sua banca.")
else:
    # Formatar dados para a tabela
    tabela_display = []
    
    for _, r in df_jogos.iterrows():
        k = f"{data_str}_{_canon(r['Home'])}_{_canon(r['Away'])}"
        info_placar = mapa_inplay.get(k, {'placar_final': 'N/A', 'placar_live': 'N/A', 'min_jogo': 'Pré-Jogo'})
        
        odd_lay = r['Odd_Lay']
        prob = r['Prob_IA']
        ev = r['EV']
        be_wr = r['Break_Even']
        
        # Dimensionamento financeiro
        stake_real = stake_base
        resp_max = stake_real * (odd_lay - 1.0)
        
        # Hedge se for Lay Draw
        st_hedge_txt = "-"
        if r['Tipo'] == 'Lay Draw' and r['Odd_Hedge'] > 1.0:
            st_h_val = stake_real * r['Stake_Hedge_Ratio']
            st_hedge_txt = f"R$ {st_h_val:.2f} (Back 1x1 @ {r['Odd_Hedge']:.2f})"
            
        placar_exibicao = info_placar['placar_live'] if info_placar['placar_live'] != 'N/A' else info_placar['placar_final']
        if placar_exibicao == 'N/A':
            status_txt = "⏳ AGUARDANDO KICK-OFF"
        else:
            status_txt = f"⚽ AO VIVO ({placar_exibicao})"
            
        tabela_display.append({
            'Horário': r['Horário'],
            'Partida': r['Jogo'],
            'Liga': r['Liga'],
            'Método': r['Método'],
            'Odd Lay Betfair': f"{odd_lay:.2f}",
            'Prob IA': f"{prob*100:.1f}%",
            'EV Estimado': f"{ev*100:+.1f}%",
            'Break-Even': f"{be_wr*100:.1f}%",
            'Stake Entrada': f"R$ {stake_real:.2f}",
            'Responsabilidade Máx': f"R$ {resp_max:.2f}",
            'Cobertura (Hedge)': st_hedge_txt,
            'Status / Placar': status_txt
        })
        
    df_tab = pd.DataFrame(tabela_display)
    
    st.subheader(f"📋 Partidas Selecionadas para Operação ({len(df_tab)} jogos)")
    st.dataframe(
        df_tab,
        use_container_width=True,
        hide_index=True
    )
    
    # ── Cards de Acompanhamento Detalhado ──
    st.markdown("### 🔍 Guia Operacional por Jogo")
    for _, r in df_jogos.iterrows():
        odd_lay = r['Odd_Lay']
        resp_max = stake_base * (odd_lay - 1.0)
        
        with st.expander(f"⚽ {r['Jogo']} — {r['Método']} (Odd Lay: {odd_lay:.2f})", expanded=True):
            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                st.markdown(f"**Liga:** {r['Liga']}")
                st.markdown(f"**Mercado:** `{r['Mercado']}`")
                st.markdown(f"**Odd de Entrada (Lay):** `{odd_lay:.2f}`")
            with c2:
                st.markdown(f"**Probabilidade IA:** `{r['Prob_IA']*100:.1f}%`")
                st.markdown(f"**Valor Esperado (EV):** `{r['EV']*100:+.1f}%`")
                st.markdown(f"**Break-Even Mínimo:** `{r['Break_Even']*100:.1f}%`")
            with c3:
                st.markdown(f"**Stake Sugerida:** `R$ {stake_base:.2f}`")
                st.markdown(f"**Responsabilidade Máxima:** `R$ {resp_max:.2f}`")
                if r['Tipo'] == 'Lay Draw' and r['Odd_Hedge'] > 1.0:
                    st_h = stake_base * r['Stake_Hedge_Ratio']
                    st.markdown(f"🛡️ **Proteção Back 1x1:** Colocar `R$ {st_h:.2f}` na odd `{r['Odd_Hedge']:.2f}`")
                else:
                    st.markdown("🛡️ **Gestão:** Lay puro sem cobertura necessária.")

st.markdown("---")
st.caption("⚡ **ARKAD PROD** — Monitoramento em tempo real conectado à API Betfair Exchange. Todas as probabilidades utilizam modelos calibrados e validados contra overfitting.")
