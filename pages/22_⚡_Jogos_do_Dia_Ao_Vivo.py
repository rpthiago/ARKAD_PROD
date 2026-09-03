# -*- coding: utf-8 -*-
"""
22_⚡_Jogos_do_Dia_Ao_Vivo.py — Radar de Jogos do Dia: Tríade Aprovada & Gestão Dinâmica
Monitoramento em tempo real dos 3 métodos validados no forward com odds de LAY reais da Betfair Exchange:
1. Lay Draw Base (Fav <= 1.40)
2. Lay Home Base (Fav Visitante <= 1.65)
3. Lay Over 4.5 FT (Under Pesado)
Gestão de Risco: 5% de Liability Dinâmica por entrada.
"""

import os
import sys
from datetime import datetime, date
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Radar de Jogos do Dia — Tríade Aprovada",
    page_icon="⚡",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from futpythontrader_client import get_daily_dataframe

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
    .badge-red {
        background-color: #c62828;
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

st.title("⚡ Radar de Jogos do Dia — Tríade Aprovada & Gestão Dinâmica")
st.markdown("""
Este radar monitora a **grade diária da Betfair Exchange** e filtra **exclusivamente os 3 métodos sobreviventes e validados no forward real**:
* 👑 **Lay Draw Base:** Super Favorito (`Fav <= 1.40` | `Lay 4.5 a 10.0`) $\\rightarrow$ *+5,2% ROI no forward*
* 👑 **Lay Home Base:** Favorito Visitante (`Visitante <= 1.65` | `Lay 2.0 a 10.0`) $\\rightarrow$ *+7,7% ROI no forward*
* ⚠️ **Lay Over 4.5 FT:** Defesa Pesada (`Under 2.5 <= 1.50` | `Lay 4.0 a 20.0`) $\\rightarrow$ *+2,9% a +5,6% ROI*

Todos os cálculos utilizam **odds executáveis de LAY da Betfair** e a **Gestão Dinâmica de 5% de Risco por aposta**.
""")

# ── Barra Lateral / Gestão de Banca ──
st.sidebar.header("💰 Gestão de Risco Dinâmica")
banca_total = st.sidebar.number_input(
    "Banca Atual (R$)",
    min_value=100.0,
    max_value=1_000_000.0,
    value=4000.0,
    step=200.0,
    help="O valor atual da sua banca. A cada entrada, o risco máximo em caso de Red é proporcional a este valor."
)

risco_pct = st.sidebar.slider(
    "Risco por Entrada (% da Banca)",
    min_value=1.0,
    max_value=10.0,
    value=5.0,
    step=0.5,
    help="Risco máximo em caso de Red (Liability Fixa Dinâmica). Recomendado: 5.0%."
)

liability_alvo = banca_total * (risco_pct / 100.0)

st.sidebar.markdown(f"""
**Resumo de Gestão:**
* **Banca Operacional:** R$ {banca_total:,.2f}
* **Risco Máximo por Red (5%):** **R$ {liability_alvo:,.2f}**
* **Comissão Betfair:** 4,5% nos greens
""")

st.sidebar.markdown("---")
st.sidebar.header("📅 Seleção de Grade")
data_selecionada = st.sidebar.date_input("Data dos Jogos", value=date.today())
data_str = data_selecionada.strftime("%Y-%m-%d")

metodos_ativos = st.sidebar.multiselect(
    "Filtrar Métodos no Radar",
    ["Lay Draw (Fav <= 1.40)", "Lay Home / DC X2 (Fav Visitante <= 1.65)", "Lay Over 4.5 FT (Under Pesado)"],
    default=["Lay Draw (Fav <= 1.40)", "Lay Home / DC X2 (Fav Visitante <= 1.65)", "Lay Over 4.5 FT (Under Pesado)"]
)

st.sidebar.markdown("---")
st.sidebar.header("🔬 Nível de Filtragem")
modo_filtro = st.sidebar.radio(
    "Critério de Filtros",
    options=[
        "🎯 Filtros Novos Refinados (Over 3.5 >= 2.54 | Visitante [1.54, 1.65])",
        "👑 Regra Base Ampla (Sem os novos filtros)"
    ],
    index=0
)
usar_filtros_novos = "Refinados" in modo_filtro

COMM = 0.045

# ── Carregamento da Grade Betfair ──
@st.cache_data(ttl=180, show_spinner=False)
def escanear_triade_betfair(ds_iso, filtros_novos=True):
    try:
        df = get_daily_dataframe(source="betfair", date_str=ds_iso)
    except Exception:
        return pd.DataFrame()
        
    if df is None or df.empty:
        return pd.DataFrame()
        
    sinais = []
    
    # Conversões numéricas seguras
    for col in [
        'Odd_H_Back', 'Odd_A_Back', 'Odd_D_Back',
        'Odd_H_Lay', 'Odd_A_Lay', 'Odd_D_Lay',
        'Odd_Over45_FT_Lay', 'Odd_Under25_FT_Back', 'Odd_Over35_FT_Back',
        'Goals_H_FT', 'Goals_A_FT'
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    for _, r in df.iterrows():
        liga = str(r.get("League", "N/A"))
        hora = str(r.get("Time", r.get("Hora", "15:00")))[:5]
        home = str(r.get("Home", "Home"))
        away = str(r.get("Away", "Away"))
        
        oh_back = r.get("Odd_H_Back")
        oa_back = r.get("Odd_A_Back")
        od_back = r.get("Odd_D_Back")
        o35_back = r.get("Odd_Over35_FT_Back")
        
        h_lay = r.get("Odd_H_Lay")
        d_lay = r.get("Odd_D_Lay")
        o45_lay = r.get("Odd_Over45_FT_Lay")
        u25_back = r.get("Odd_Under25_FT_Back")
        
        gh = r.get("Goals_H_FT")
        ga = r.get("Goals_A_FT")
        tem_placar = pd.notna(gh) and pd.notna(ga)
        placar_str = f"{int(gh)}x{int(ga)}" if tem_placar else "vs"
        
        base_sinal = {
            "Data": ds_iso,
            "Horário": hora,
            "Liga": liga,
            "Partida": f"{home} x {away}",
            "Home": home,
            "Away": away,
            "Goals_H": gh,
            "Goals_A": ga,
            "Tem_Placar": tem_placar,
            "Placar": placar_str
        }
        
        # 1. LAY DRAW: min(Back_H, Back_A) <= 1.40 e Lay_D entre 4.5 e 10.0
        # Se filtros_novos: exige Odd_Over35_FT_Back >= 2.54
        fav_back = None
        if pd.notna(oh_back) and pd.notna(oa_back):
            fav_back = min(oh_back, oa_back)
        elif pd.notna(oh_back):
            fav_back = oh_back
        elif pd.notna(oa_back):
            fav_back = oa_back
            
        filtro_d_extra = (pd.isna(o35_back) or o35_back >= 2.54) if filtros_novos else True
        if fav_back is not None and fav_back <= 1.40 and pd.notna(d_lay) and (4.5 <= d_lay <= 10.0) and filtro_d_extra:
            is_green = (gh != ga) if tem_placar else None
            info_filtro = "Over3.5 >= 2.54" if filtros_novos else "Regra Base"
            sinais.append({
                **base_sinal,
                "Método": "Lay Draw (Fav <= 1.40)",
                "Filtro_Aplicado": info_filtro,
                "Mercado": "Match Odds (Draw)",
                "Odd_Fav": fav_back,
                "Odd_Lay": d_lay,
                "Green": is_green,
                "Tipo": "LAY_DRAW"
            })
            
        # 2. LAY HOME: Se filtros_novos: 1.54 <= Away <= 1.65; Se base: Away <= 1.65
        cond_home_fav = (1.54 <= oa_back <= 1.65) if filtros_novos else (oa_back <= 1.65)
        if pd.notna(oa_back) and cond_home_fav and pd.notna(h_lay) and (2.0 <= h_lay <= 10.0):
            is_green = (ga >= gh) if tem_placar else None
            info_filtro = "Away [1.54, 1.65]" if filtros_novos else "Regra Base"
            sinais.append({
                **base_sinal,
                "Método": "Lay Home / DC X2 (Fav Visitante <= 1.65)",
                "Filtro_Aplicado": info_filtro,
                "Mercado": "Match Odds (Home)",
                "Odd_Fav": oa_back,
                "Odd_Lay": h_lay,
                "Green": is_green,
                "Tipo": "LAY_HOME"
            })
            
        # 3. LAY OVER 4.5 FT: Under 2.5 <= 1.50 e Lay_O45 entre 4.0 e 20.0
        if pd.notna(u25_back) and u25_back <= 1.50 and pd.notna(o45_lay) and (4.0 <= o45_lay <= 20.0):
            is_green = ((gh + ga) <= 4) if tem_placar else None
            sinais.append({
                **base_sinal,
                "Método": "Lay Over 4.5 FT (Under Pesado)",
                "Filtro_Aplicado": "Under <= 1.50",
                "Mercado": "Over/Under 4.5 FT",
                "Odd_Fav": u25_back,
                "Odd_Lay": o45_lay,
                "Green": is_green,
                "Tipo": "LAY_OVER45"
            })
            
    return pd.DataFrame(sinais)

col_top1, col_top2 = st.columns([1, 4])
with col_top1:
    btn_atualizar = st.button("🔄 Atualizar Grade Agora", type="primary", use_container_width=True)

if btn_atualizar:
    st.cache_data.clear()

with st.spinner(f"Consultando grade de {data_str} na Betfair Exchange..."):
    df_radar = escanear_triade_betfair(data_str, filtros_novos=usar_filtros_novos)

if df_radar.empty:
    st.info(f"Nenhum jogo qualificado para a tríade aprovada na grade de **{data_str}** na Betfair.")
else:
    # Filtrar métodos selecionados na barra lateral
    if metodos_ativos:
        df_radar = df_radar[df_radar['Método'].isin(metodos_ativos)].copy().reset_index(drop=True)
        
    # Calcular Dimensionamento Dinâmico de Risco
    df_radar['Liability_R$'] = liability_alvo
    df_radar['Stake_R$'] = df_radar['Liability_R$'] / (df_radar['Odd_Lay'] - 1.0)
    df_radar['Lucro_Potencial_R$'] = df_radar['Stake_R$'] * (1.0 - COMM)
    df_radar['Break_Even_WR'] = (df_radar['Odd_Lay'] - 1.0) / (df_radar['Odd_Lay'] - COMM) * 100.0
    
    # Calcular PnL Realizado para jogos finalizados
    def calc_pnl(row):
        if row['Green'] is None:
            return None
        return row['Lucro_Potencial_R$'] if row['Green'] else -row['Liability_R$']
        
    df_radar['PnL_R$'] = df_radar.apply(calc_pnl, axis=1)
    
    # ── Métricas do Topo ──
    n_jogos = len(df_radar)
    risco_total_dia = df_radar['Liability_R$'].sum()
    lucro_potencial_dia = df_radar['Lucro_Potencial_R$'].sum()
    
    finalizados = df_radar[df_radar['Green'].notna()]
    n_finalizados = len(finalizados)
    pnl_realizado_dia = finalizados['PnL_R$'].sum() if n_finalizados > 0 else 0.0
    
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🎯 Jogos Qualificados", f"{n_jogos} jogos", f"{n_finalizados} finalizados")
    c2.metric("🛡️ Risco Máx por Aposta (5%)", f"R$ {liability_alvo:,.2f}", f"Banca: R$ {banca_total:,.2f}")
    c3.metric("💰 Lucro Potencial Dia", f"R$ {lucro_potencial_dia:,.2f}", f"Risco Total: R$ {risco_total_dia:,.2f}")
    if n_finalizados > 0:
        c4.metric("📊 P&L Realizado Hoje", f"R$ {pnl_realizado_dia:+,.2f}", f"Greens: {finalizados['Green'].sum()}/{n_finalizados}")
    else:
        c4.metric("📊 P&L Realizado Hoje", "Aguardando Jogos", "0 finalizados")

    # ── Tabela Principal ──
    st.markdown("---")
    st.subheader(f"📋 Grade de Oportunidades Auditadas ({n_jogos} jogos)")
    
    tabela_visual = []
    for _, r in df_radar.iterrows():
        if r['Green'] is True:
            status_tag = f"✅ GREEN (+R$ {r['Lucro_Potencial_R$']:.2f})"
        elif r['Green'] is False:
            status_tag = f"❌ RED (-R$ {r['Liability_R$']:.2f})"
        else:
            status_tag = "⏳ PENDENTE"
            
        tabela_visual.append({
            "Horário": r['Horário'],
            "Liga": r['Liga'],
            "Partida": r['Partida'],
            "Método Aprovado": r['Método'],
            "Filtro": r.get('Filtro_Aplicado', 'Base'),
            "Odd Fav (Back)": f"{r['Odd_Fav']:.2f}",
            "Odd LAY Real (Betfair)": f"{r['Odd_Lay']:.2f}",
            "Stake Entrada": f"R$ {r['Stake_R$']:.2f}",
            "Risco Máx (5%)": f"R$ {r['Liability_R$']:.2f}",
            "Lucro Potencial": f"R$ {r['Lucro_Potencial_R$']:.2f}",
            "Break-Even Exig.": f"{r['Break_Even_WR']:.1f}%",
            "Placar": r['Placar'],
            "Status": status_tag
        })
        
    st.dataframe(
        pd.DataFrame(tabela_visual),
        use_container_width=True,
        hide_index=True
    )

st.markdown("---")
st.caption("⚡ **ARKAD PROD** — Monitoramento oficial conectado à API Betfair Cloud com odds de LAY reais da Exchange.")
