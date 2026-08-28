# -*- coding: utf-8 -*-
"""
02_📊_Resultados_Metodos_Aprovados.py — Painel de Resultados dos Métodos Aprovados do ARKAD.
Consolida automaticamente todas as planilhas diárias da pasta `metodos_aprovados/`:
  - Visão geral com KPIs consolidados (Greens, Reds, WR, P&L e ROI)
  - Curva de Equity e Evolução Acumulada
  - Desempenho desdobrado por Método e por Data
  - Tabela completa jogo a jogo com download em Excel
"""
import os, io, sys, glob
from datetime import date
from pathlib import Path
import numpy as np, pandas as pd, streamlit as st

st.set_page_config(
    page_title="ARKAD — Resultados dos Métodos Aprovados",
    page_icon="📊",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
FOLDER = ROOT / "metodos_aprovados"

# ── Header ──
st.title("📊 Resultados Reais dos Métodos Aprovados — ARKAD")
st.markdown(
    "Acompanhamento e auditoria dos sinais diários gerados e liquidados a partir da pasta "
    "[`metodos_aprovados/`](file:///c:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD/metodos_aprovados)."
)

# ── Sidebar ──
st.sidebar.header("⚙️ Configurações de Banca & Stake")
stake_base = st.sidebar.number_input("Valor da Stake Base (R$)", min_value=10.0, value=100.0, step=10.0)

# ── Carregamento de Dados ──
@st.cache_data(ttl=60, show_spinner=False)
def carregar_dados_aprovados():
    if not FOLDER.exists():
        return pd.DataFrame()
    files = sorted(FOLDER.glob("*.xlsx"))
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for f in files:
        try:
            df = pd.read_excel(f)
            df["_Arquivo"] = f.name
            dfs.append(df)
        except Exception:
            continue
            
    if not dfs:
        return pd.DataFrame()
        
    df_all = pd.concat(dfs, ignore_index=True)
    df_all["Data"] = pd.to_datetime(df_all["Data"], errors="coerce")
    df_all["Hora"] = df_all.get("Hora", "15:00").astype(str).str[:5]
    
    # Padronização de Resultado e P&L
    df_all["Result_Num"] = pd.to_numeric(df_all.get("1/0"), errors="coerce")
    df_all["PnL_u"] = pd.to_numeric(df_all.get("P/L"), errors="coerce")
    
    # Status amigável
    def _status(r):
        if pd.isna(r):
            return "⏳ PENDENTE"
        elif r == 1 or r == 1.0:
            return "🟢 GREEN"
        elif r == 0 or r == 0.0:
            return "🔴 RED"
        return "N/A"
        
    df_all["Status"] = df_all["Result_Num"].apply(_status)
    df_all["PnL_Reais"] = df_all["PnL_u"] * stake_base
    
    return df_all.sort_values(["Data", "Hora"]).reset_index(drop=True)

df_raw = carregar_dados_aprovados()

if df_raw.empty:
    st.warning("Nenhuma planilha encontrada na pasta `metodos_aprovados/`.")
    st.stop()

# ── Filtros no Topo ──
st.markdown("### 🔍 Filtros de Visualização")
f_c1, f_c2, f_c3 = st.columns(3)

metodos_disponiveis = sorted(df_raw["Método"].dropna().unique().tolist())
with f_c1:
    filtro_metodo = st.multiselect("Filtrar por Método", metodos_disponiveis, default=metodos_disponiveis)
with f_c2:
    status_disponiveis = sorted(df_raw["Status"].dropna().unique().tolist())
    filtro_status = st.multiselect("Filtrar por Status", status_disponiveis, default=status_disponiveis)
with f_c3:
    dmin = df_raw["Data"].min().date() if df_raw["Data"].notna().any() else date.today()
    dmax = df_raw["Data"].max().date() if df_raw["Data"].notna().any() else date.today()
    filtro_data = st.date_input("Intervalo de Datas", [dmin, dmax])

# Aplicação dos filtros
df_filt = df_raw.copy()
if filtro_metodo:
    df_filt = df_filt[df_filt["Método"].isin(filtro_metodo)]
if filtro_status:
    df_filt = df_filt[df_filt["Status"].isin(filtro_status)]
if isinstance(filtro_data, (list, tuple)) and len(filtro_data) == 2:
    df_filt = df_filt[(df_filt["Data"].dt.date >= filtro_data[0]) & (df_filt["Data"].dt.date <= filtro_data[1])]

# ── Métricas Consolidadas (KPIs) ──
df_liq = df_filt[df_filt["Status"].isin(["🟢 GREEN", "🔴 RED"])].copy()

total_jogos = len(df_filt)
jogos_liq = len(df_liq)
greens = (df_liq["Status"] == "🟢 GREEN").sum()
reds = (df_liq["Status"] == "🔴 RED").sum()
pendentes = (df_filt["Status"] == "⏳ PENDENTE").sum()

win_rate = (greens / jogos_liq * 100) if jogos_liq > 0 else 0.0
pnl_total_u = df_liq["PnL_u"].sum()
pnl_total_rs = df_liq["PnL_Reais"].sum()

st.markdown("---")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total de Jogos", f"{total_jogos} partidas", f"{jogos_liq} liquidadas")
kpi2.metric("Greens / Taxa de Acerto", f"{greens} ({win_rate:.1f}%)", f"{reds} reds")
kpi3.metric("Lucro Líquido (Unidades)", f"{pnl_total_u:+.2f} u", delta=f"{pnl_total_u:+.2f} u")
kpi4.metric(f"Lucro em R$ (Stake R$ {stake_base:.0f})", f"R$ {pnl_total_rs:+.2f}", delta=f"R$ {pnl_total_rs:+.2f}")
kpi5.metric("Pendentes / Ao Vivo", f"{pendentes} jogos")
st.markdown("---")

# ── Abas de Análise ──
tab_geral, tab_metodo, tab_dia, tab_grafico = st.tabs([
    "📋 Planilha Completa Jogo a Jogo",
    "🎯 Desempenho por Método",
    "📅 Desempenho Dia a Dia",
    "📈 Curva de Lucro Acumulado"
])

# 1. PLANILHA GERAL
with tab_geral:
    st.subheader("📋 Tabela Consolidada de Jogos")
    
    cols_exibir = [
        "Data", "Hora", "Liga", "Jogo", "Método", "Mercado", "Lado", 
        "Odd_Entrada", "Odd_Fav", "Status", "PnL_u", "PnL_Reais"
    ]
    cols_disponiveis = [c for c in cols_exibir if c in df_filt.columns]
    
    df_show = df_filt[cols_disponiveis].copy()
    df_show["Data"] = df_show["Data"].dt.strftime("%d/%m/%Y")
    
    st.dataframe(df_show, use_container_width=True, hide_index=True)
    
    # Download em Excel
    _buf = io.BytesIO()
    with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
        df_show.to_excel(_writer, index=False, sheet_name="Resultados_Aprovados")
    st.download_button(
        label="📥 Baixar Planilha Consolidada (Excel)",
        data=_buf.getvalue(),
        file_name="Resultados_Metodos_Aprovados_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )

# 2. DESEMPENHO POR MÉTODO
with tab_metodo:
    st.subheader("🎯 Comparativo de Desempenho por Método")
    if not df_liq.empty:
        res_m = []
        for m, g in df_liq.groupby("Método"):
            n_m = len(g)
            w_m = (g["Status"] == "🟢 GREEN").sum()
            r_m = (g["Status"] == "🔴 RED").sum()
            wr_m = (w_m / n_m * 100) if n_m > 0 else 0.0
            pnl_m_u = g["PnL_u"].sum()
            pnl_m_rs = g["PnL_Reais"].sum()
            res_m.append({
                "Método": m,
                "Total Jogos": n_m,
                "Greens": w_m,
                "Reds": r_m,
                "Win Rate %": f"{wr_m:.1f}%",
                "Lucro Líquido (u)": round(pnl_m_u, 3),
                "Lucro Líquido (R$)": f"R$ {pnl_m_rs:+.2f}"
            })
        st.dataframe(pd.DataFrame(res_m), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum jogo liquidado no filtro atual.")

# 3. DESEMPENHO DIA A DIA
with tab_dia:
    st.subheader("📅 Desempenho Detalhado por Data")
    if not df_liq.empty:
        res_d = []
        for d, g in df_liq.groupby(df_liq["Data"].dt.strftime("%d/%m/%Y"), sort=False):
            n_d = len(g)
            w_d = (g["Status"] == "🟢 GREEN").sum()
            r_d = (g["Status"] == "🔴 RED").sum()
            wr_d = (w_d / n_d * 100) if n_d > 0 else 0.0
            pnl_d_u = g["PnL_u"].sum()
            pnl_d_rs = g["PnL_Reais"].sum()
            res_d.append({
                "Data": d,
                "Jogos": n_d,
                "Greens": w_d,
                "Reds": r_d,
                "Win Rate %": f"{wr_d:.1f}%",
                "Lucro do Dia (u)": round(pnl_d_u, 3),
                "Lucro do Dia (R$)": f"R$ {pnl_d_rs:+.2f}"
            })
        st.dataframe(pd.DataFrame(res_d), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum jogo liquidado no filtro atual.")

# 4. GRÁFICO DE CURVA DE LUCRO
with tab_grafico:
    st.subheader("📈 Curva de Lucro Acumulado (Equity)")
    if not df_liq.empty:
        df_liq_sorted = df_liq.sort_values(["Data", "Hora"]).reset_index(drop=True)
        df_liq_sorted["PnL_Acum_u"] = df_liq_sorted["PnL_u"].cumsum()
        df_liq_sorted["PnL_Acum_Reais"] = df_liq_sorted["PnL_Reais"].cumsum()
        df_liq_sorted["Num_Jogo"] = range(1, len(df_liq_sorted) + 1)
        
        st.line_chart(df_liq_sorted.set_index("Num_Jogo")["PnL_Acum_Reais"])
        st.caption("Evolução do saldo financeiro acumulado (em R$) ao longo das apostas liquidadas.")
    else:
        st.info("Sem dados suficientes para gerar a curva de equity.")
