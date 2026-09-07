# -*- coding: utf-8 -*-
"""
23_⚡_Radar_Pressao_Cruzada.py — Scanner em Tempo Real do Método Pressão Cruzada
ARKAD_PROD

Monitora e filtra as partidas diárias da Betfair Exchange cruzando:
- 1X2 (Odd_H, Odd_Fav)
- Over/Under 2.5 (Odd_Over25)
- BTTS (Odd_BTTS_Yes)

Mapeia:
- Perfil 1: Lay Draw de Alta Pressão (Super Fav + Over)
- Perfil 2: Lay Home Falso Favorito (Fav mandante nominal + Under Pesado)
"""

import os
import io
import sys
from datetime import date, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="ARKAD — Radar Pressão Cruzada",
    page_icon="⚡",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from futpythontrader_client import get_daily_dataframe
from metodo_pressao_cruzada_strategy import (
    calcular_constantes,
    avaliar_jogo_pressao_cruzada,
    escanear_grade_pressao_cruzada
)

# Estilização visual moderna
st.markdown("""
<style>
    .card-destaque {
        background-color: #1a2234;
        border-radius: 10px;
        padding: 16px;
        border-left: 5px solid #00e676;
        margin-bottom: 12px;
    }
    .metric-box {
        background-color: #111827;
        border: 1px solid #374151;
        border-radius: 8px;
        padding: 12px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Radar de Pressão Cruzada (*Cross-Market Index*)")
st.caption("Consistência cruzada entre 1X2, Over/Under 2.5 e BTTS na Betfair Exchange.")

st.error("""
🛑 **STATUS DE GOVERNANÇA: MÉTODO ARQUIVADO (AUDITORIA FORENSE CLAUDE — 07/09/2026)**
* **Veredito:** O conceito de $K_{pressao}$ é matematicamente legítimo, mas **não possui edge pré-jogo** (−2,09% no Lay Draw e −5,01% no Lay Home em 50k jogos).
* **Causa Raiz (Eficiência de Mercado):** O filtro derruba empates para 15%, mas a Betfair derruba a odd junto (o Break-Even sobe para 87,07%). Multiplicar odds públicas não gera informação nova.
* **Uso no Sistema:** Mantido no painel exclusivamente como **Controle Nulo Didático** e laboratório analítico. **NÃO OPERAR COM DINHEIRO REAL**.
""")

# ── Sidebar ──
st.sidebar.header("⚙️ Configurações do Radar")
data_busca = st.sidebar.date_input("Data dos Jogos", value=date.today())

st.sidebar.markdown("---")
st.sidebar.header("💰 Gestão de Risco & Dimensionamento")
modo_gestao = st.sidebar.radio("Modo de Gestão de Risco", ["Liability Fixa (R$)", "Stake Fixa (R$)"], index=0)
if modo_gestao == "Liability Fixa (R$)":
    liability_padrao = st.sidebar.number_input("Risco Máximo por Jogo (R$)", min_value=10.0, value=100.0, step=10.0)
else:
    stake_padrao = st.sidebar.number_input("Stake Base por Jogo (R$)", min_value=10.0, value=100.0, step=10.0)

comm_rate = 0.05
fator_comissao = 1.0 - comm_rate

# ── Carregamento da Grade Diária ──
date_str = data_busca.strftime("%Y-%m-%d")

@st.cache_data(ttl=600, show_spinner=False)
def carregar_grade(ds: str):
    try:
        df = get_daily_dataframe("betfair", ds)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        pass
    # Fallback para base local lean se for data recente
    lean_csv = ROOT / "b365_base_lean.csv"
    if lean_csv.exists():
        df_l = pd.read_csv(lean_csv, low_memory=False)
        df_l = df_l[df_l["Date"].str.startswith(ds)].copy()
        return df_l
    return pd.DataFrame()

with st.spinner(f"Carregando grade da Betfair Exchange para {date_str}..."):
    df_raw = carregar_grade(date_str)

if df_raw.empty:
    st.info(f"Nenhum jogo encontrado para {date_str} na grade da Betfair Exchange.")
    st.stop()

# ── Execução da Estratégia ──
df_sinais = escanear_grade_pressao_cruzada(df_raw)

# KPIs de Topo
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Jogos na Grade", f"{len(df_raw)}")
n_sinais = len(df_sinais)
c2.metric("Sinais Qualificados", f"{n_sinais}")
n_draw = len(df_sinais[df_sinais["Perfil"] == "LAY_DRAW_ALTA_PRESSAO"]) if not df_sinais.empty else 0
c3.metric("Lay Draw Alta Pressão", f"{n_draw}")
n_home = len(df_sinais[df_sinais["Perfil"] == "LAY_HOME_FALSO_FAVORITO"]) if not df_sinais.empty else 0
c4.metric("Lay Home Falso Fav", f"{n_home}")

# Dimensionamento financeiro
if not df_sinais.empty:
    if modo_gestao == "Liability Fixa (R$)":
        df_sinais["Stake_Sugerida_R$"] = (liability_padrao / (df_sinais["Odd_Entrada"] - 1.0)).round(2)
        df_sinais["Risco_Max_R$"] = liability_padrao
        df_sinais["Lucro_Est_R$"] = (df_sinais["Stake_Sugerida_R$"] * fator_comissao).round(2)
    else:
        df_sinais["Stake_Sugerida_R$"] = stake_padrao
        df_sinais["Risco_Max_R$"] = ((df_sinais["Odd_Entrada"] - 1.0) * stake_padrao).round(2)
        df_sinais["Lucro_Est_R$"] = (stake_padrao * fator_comissao).round(2)

# Abas de Navegação
tab1, tab2, tab3 = st.tabs([
    "🎯 Perfil 1: Lay Draw Alta Pressão",
    "🛡️ Perfil 2: Lay Home Falso Fav",
    "📊 Scanner de Todos os Jogos (K_pressao & K_ratio)"
])

# ── TAB 1: LAY DRAW ALTA PRESSÃO ──
with tab1:
    st.subheader("🎯 Lay Draw de Alta Pressão ($K_{pressao} \\le 2.20$)")
    st.caption("Super Favoritos com expectativa alta de gols onde a taxa histórica de empate desaba para ~20%.")
    
    df_p1 = df_sinais[df_sinais["Perfil"] == "LAY_DRAW_ALTA_PRESSAO"].copy() if not df_sinais.empty else pd.DataFrame()
    if df_p1.empty:
        st.info("Nenhuma partida se enquadrou nos critérios estritos de Lay Draw de Alta Pressão para hoje.")
    else:
        cols_show = ["Hora", "Liga", "Confronto", "Odd_Fav", "K_pressao", "Odd_Entrada", "Break_Even_WR", "Stake_Sugerida_R$", "Risco_Max_R$", "Lucro_Est_R$"]
        cols_presentes = [c for c in cols_show if c in df_p1.columns]
        st.dataframe(df_p1[cols_presentes].sort_values("Hora"), use_container_width=True, hide_index=True)

# ── TAB 2: LAY HOME FALSO FAVORITO ──
with tab2:
    st.subheader("🛡️ Lay Home no Falso Favorito ($K_{pressao} \\ge 3.80$)")
    st.caption("Mandantes com odd nominal baixa, mas em jogos truncados/Under com alta chance de zebra ou empate.")
    
    df_p2 = df_sinais[df_sinais["Perfil"] == "LAY_HOME_FALSO_FAVORITO"].copy() if not df_sinais.empty else pd.DataFrame()
    if df_p2.empty:
        st.info("Nenhuma partida se enquadrou no perfil de Falso Favorito para hoje.")
    else:
        cols_show = ["Hora", "Liga", "Confronto", "Odd_Fav", "K_pressao", "Odd_Entrada", "Break_Even_WR", "Stake_Sugerida_R$", "Risco_Max_R$", "Lucro_Est_R$"]
        cols_presentes = [c for c in cols_show if c in df_p2.columns]
        st.dataframe(df_p2[cols_presentes].sort_values("Hora"), use_container_width=True, hide_index=True)

# ── TAB 3: SCANNER DE TODOS OS JOGOS ──
with tab3:
    st.subheader("📊 Mapeamento Geral de Pressão Cruzada na Rodada")
    st.caption("Cálculo em tempo real de $K_{pressao} = \\text{Odd}_H \\times \\text{Odd}_{Over2.5}$ e $K_{ratio} = \\text{Odd}_{Over2.5} / \\text{Odd}_{BTTS}$ para todos os jogos.")
    
    all_rows = []
    for _, r in df_raw.iterrows():
        oh = pd.to_numeric(r.get("Odd_H_Back") or r.get("Odd_H"), errors="coerce")
        oa = pd.to_numeric(r.get("Odd_A_Back") or r.get("Odd_A"), errors="coerce")
        u25 = pd.to_numeric(r.get("Odd_Over25_FT_Back") or r.get("Odd_Over25_FT") or r.get("Odd_Over25"), errors="coerce")
        btts = pd.to_numeric(r.get("Odd_BTTS_Yes_Back") or r.get("Odd_BTTS_Yes"), errors="coerce")
        d_lay = pd.to_numeric(r.get("Odd_D_Lay"), errors="coerce")
        h_lay = pd.to_numeric(r.get("Odd_H_Lay"), errors="coerce")
        
        const = calcular_constantes(oh, u25, btts)
        home = str(r.get("Home") or r.get("Home_Team") or "")
        away = str(r.get("Away") or r.get("Away_Team") or "")
        
        all_rows.append({
            "Hora": str(r.get("Time") or r.get("Hora") or ""),
            "Liga": str(r.get("League") or r.get("Liga") or ""),
            "Confronto": f"{home} x {away}",
            "Odd_H": oh,
            "Odd_A": oa,
            "Odd_Over25": u25,
            "Odd_BTTS": btts,
            "Odd_D_Lay": d_lay,
            "Odd_H_Lay": h_lay,
            "K_pressao": const["k_pressao"],
            "K_ratio": const["k_ratio"]
        })
    df_all = pd.DataFrame(all_rows).dropna(subset=["K_pressao"]).sort_values("K_pressao")
    st.dataframe(df_all, use_container_width=True, hide_index=True)

# ── Download Excel ──
if not df_sinais.empty:
    st.markdown("---")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_sinais.to_excel(w, sheet_name="Sinais_Qualificados", index=False)
        df_all.to_excel(w, sheet_name="Todos_Jogos_K", index=False)
    st.download_button(
        "📥 Baixar Planilha dos Sinais de Hoje (Excel)",
        buf.getvalue(),
        file_name=f"radar_pressao_cruzada_{date_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
