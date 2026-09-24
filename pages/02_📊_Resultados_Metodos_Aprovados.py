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
st.title("📊 Resultados dos Métodos em Validação Forward — ARKAD")
st.warning("""
🚨 **STATUS DE GOVERNANÇA (02/09/2026): EM VALIDAÇÃO FORWARD (Stake-Zero)**
* **Portfólio em Validação:** 6 métodos oficiais acompanhados em forward — **Lay 0x0 XGBoost**, **Lay 0x3 Top 3**, **Lay 2x2 Top 3**, **Lay Draw Super Fav**, **Lay Home Fav Visitante** e **Lay Over 4.5 Under Pesado**.
* **Critério Científico:** Odds de LAY reais executáveis na Betfair Exchange e liquidação desacoplada.
* **Governança:** A autoridade canônica é o ledger forward. Status de dinheiro real bloqueado até acumular $N \\ge 400$ apostas ou 5 fins de semana com dados limpos e IC pós-FDR estritamente excluindo zero.
""")
st.markdown(
    "Acompanhamento e auditoria dos sinais diários gerados e liquidados a partir da pasta "
    "[`metodos_aprovados/`](file:///c:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD/metodos_aprovados)."
)

# ── Sidebar ──
st.sidebar.header("⚙️ Configurações de Banca & Gestão")
banca_total = st.sidebar.number_input("Banca Total (R$)", min_value=100.0, value=2000.0, step=100.0)

tipo_gestao = st.sidebar.selectbox(
    "Modelo de Gestão de Risco (Liability)",
    options=[
        "🎯 Diferenciada (15% Over 4.5 | 10% em 0x0, 2x2, 0x3 | 5.0% em Home, Draw)",
        "⚖️ Uniforme (5.0% Fixa para Todos)",
        "🔥 Uniforme (10.0% Fixa para Todos)"
    ],
    index=0
)

if "Diferenciada" in tipo_gestao:
    st.sidebar.markdown(f"""
    **Alocação de Liability por Entrada:**
    * 🟡 **15.0% da Banca (R$ {banca_total * 0.15:,.2f}):** Lay Over 4.5 FT (Under Pesado — 97.8% WR)
    * 🟣 **10.0% da Banca (R$ {banca_total * 0.10:,.2f}):** Lay 0x0 XGBoost, Lay 2x2 Top 3 e Lay 0x3 Top 3
    * 🔵 **5.0% da Banca (R$ {banca_total * 0.05:,.2f}):** Lay Home e Lay Draw
    """)

st.sidebar.markdown("---")
st.sidebar.header("📉 Taxa de Comissão Betfair")
taxa_comissao = st.sidebar.selectbox(
    "Taxa de Comissão",
    options=[3.5, 5.0],
    format_func=lambda x: f"{x}% ({'Fórmula do Usuário - Fator 0.965' if x == 3.5 else 'Protocolo Conservador - Fator 0.950'})",
    index=0
)
fator_comissao = 1.0 - (taxa_comissao / 100.0)

st.sidebar.markdown("---")
st.sidebar.header("📁 Fonte dos Dados")
fonte_dados = st.sidebar.radio(
    "Selecione a Base",
    options=[
        "📁 Todas as Planilhas Diárias (Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx)",
        "👑 Apenas Tríade (Draw, Home, Over 4.5)",
        "🎯 Apenas Top 3 CS (Lay 2x2 e Lay 0x3)"
    ],
    index=0
)

# ── Carregamento de Dados Seguro ──
def _ler_excel_seguro(path):
    import ctypes
    from ctypes import wintypes
    try:
        return pd.read_excel(path)
    except Exception:
        # Fallback Windows: ler arquivo mesmo se aberto no Excel
        try:
            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            GENERIC_READ = 0x80000000
            FILE_SHARE_READ = 1
            FILE_SHARE_WRITE = 2
            FILE_SHARE_DELETE = 4
            OPEN_EXISTING = 3
            FILE_ATTRIBUTE_NORMAL = 0x80
            h = k32.CreateFileW(
                str(path), GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
            )
            if h == -1 or h == 0xFFFFFFFFFFFFFFFF:
                return pd.DataFrame()
            try:
                sz = k32.GetFileSize(h, None)
                buf = ctypes.create_string_buffer(sz)
                br = wintypes.DWORD()
                k32.ReadFile(h, buf, sz, ctypes.byref(br), None)
                return pd.read_excel(io.BytesIO(buf.raw[:br.value]))
            finally:
                k32.CloseHandle(h)
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def carregar_dados_aprovados(
    modo="📁 Todas as Planilhas Diárias (Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx)",
    banca_total=2000.0,
    tipo_gestao="🎯 Diferenciada (15% Over 4.5 | 10% em 0x0, 2x2, 0x3 | 5.0% em Home, Draw)",
    fator_comissao=0.965
):
    if not FOLDER.exists():
        return pd.DataFrame()

    # Carregar EXCLUSIVAMENTE os arquivos no formato Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx da pasta metodos_aprovados/
    planilhas_diarias = sorted([
        f for f in FOLDER.glob("Sinais_Metodos_Aprovados_20*.xlsx")
        if not f.name.startswith("~$") and "Odds_Reais" not in f.name
    ])
    if not planilhas_diarias:
        return pd.DataFrame()

    dfs = []
    for f_novo in planilhas_diarias:
        df_n = _ler_excel_seguro(f_novo)
        if df_n is not None and not df_n.empty:
            for c in list(df_n.columns):
                if "todo" in str(c).lower():
                    df_n.rename(columns={c: "Método"}, inplace=True)
            df_n["_Arquivo"] = f_novo.name
            dfs.append(df_n)

    if not dfs:
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)

    # Filtros opcionais da barra lateral (sempre restritos às planilhas diárias carregadas)
    if "Apenas Tríade" in modo and "Método" in df_all.columns:
        df_all = df_all[~df_all["Método"].astype(str).str.contains("2x2|0x3|Zebra|0x2|2x0|0x0", na=False)].reset_index(drop=True)
    elif "Apenas Top 3 CS" in modo and "Método" in df_all.columns:
        df_all = df_all[df_all["Método"].astype(str).str.contains("2x2|0x3", na=False)].reset_index(drop=True)
        
    # Normalização segura de colunas e deduplicação de nomes
    cols = []
    for c in df_all.columns:
        cols.append("Método" if "todo" in str(c).lower() else c)
    df_all.columns = cols
    df_all = df_all.loc[:, ~df_all.columns.duplicated()].copy()
            
    df_all["Data"] = pd.to_datetime(df_all.get("Data"), format="ISO8601", errors="coerce")
    # Fallback caso alguma planilha tenha data em DD/MM/YYYY
    if df_all["Data"].isna().any():
        _mask_na = df_all["Data"].isna()
        df_all.loc[_mask_na, "Data"] = pd.to_datetime(df_all.loc[_mask_na, "_Arquivo"].str.extract(r"(\d{4}-\d{2}-\d{2})")[0], errors="coerce")
    # Forward comeca em 01/08/2026: nada anterior entra nesta pagina
    df_all = df_all[df_all["Data"] >= pd.Timestamp("2026-08-01")].reset_index(drop=True)
    df_all["Hora"] = df_all.get("Hora", "15:00").astype(str).str[:5]
    
    # 1. Normalização de Odd de Entrada
    if "Odd_Entrada" not in df_all.columns and "Odd_Lay" in df_all.columns:
        df_all["Odd_Entrada"] = df_all["Odd_Lay"]
    elif "Odd_Entrada" in df_all.columns and "Odd_Lay" in df_all.columns:
        df_all["Odd_Entrada"] = df_all["Odd_Entrada"].fillna(df_all["Odd_Lay"])
        
    df_all["Odd_Entrada"] = pd.to_numeric(df_all.get("Odd_Entrada"), errors="coerce").fillna(5.0)
    
    # 2. Normalização de Nomes de Métodos
    def _norm_metodo(m):
        m_str = str(m)
        if "0x0" in m_str:
            return "Lay 0x0 XGBoost (Sweet Spot [10, 20])"
        elif "Away" in m_str or "1X" in m_str:
            return "Lay Away / DC 1X (Fav <= 1.45)"
        elif "Home" in m_str or "X2" in m_str:
            return "Lay Home / DC X2 (Fav Visitante <= 1.65)"
        elif "Over 4.5" in m_str or "Over45" in m_str:
            return "Lay Over 4.5 FT (Under Pesado)"
        elif "Draw" in m_str:
            return "Lay Draw (Fav <= 1.40)"
        elif "2x2" in m_str:
            return "Lay 2x2 Top 3 (Aprovado)"
        elif "0x3" in m_str:
            if "ampla" in m_str.lower() or "sem ranking" in m_str.lower():
                return "Lay 0x3 (Regra Ampla - Paralelo)"
            return "Lay 0x3 Top 3 (Aprovado)"
        elif "Under 0.5" in m_str:
            return "Lay Under 0.5 FT (Fav)"
        elif "0x1" in m_str:
            return "Lay 0x1 Super Fav"
        elif "0x2" in m_str:
            return "Lay 0x2 Zebra (Micro-Liability)"
        elif "2x0" in m_str:
            return "Lay 2x0 Zebra (Micro-Liability)"
        elif "Under 1.5" in m_str:
            return "Lay Under 1.5 FT (XGBoost)"
        return m_str
        
    if "Método" in df_all.columns:
        df_all["Método"] = df_all["Método"].apply(_norm_metodo)
    else:
        df_all["Método"] = "Lay Draw (Fav <= 1.40)"
        
    # 3. Normalização de Status e Resultado com Auto-Settlement Inteligente por Placar
    def _calc_status(r):
        res = str(r.get("Resultado", "")).upper().strip()
        r_num = r.get("1/0")
        gr = r.get("Green")
        st_txt = str(r.get("status", "")).upper()
        
        # 1. Se tem 1/0 explícito preenchido
        if pd.notna(r_num):
            try:
                val = float(r_num)
                if val == 1.0: return "🟢 GREEN"
                elif val == 0.0: return "🔴 RED"
            except Exception:
                pass
                
        # 2. Se tem Green explícito (True/False)
        if gr is True or gr == "True" or gr == 1:
            return "🟢 GREEN"
        elif gr is False or gr == "False" or gr == 0:
            return "🔴 RED"
            
        # 3. Se tem Resultado texto explícito
        if res == "GREEN":
            return "🟢 GREEN"
        elif res == "RED":
            return "🔴 RED"
        elif "FORA_DA_FAIXA" in res or "FORA_DA_FAIXA" in st_txt:
            return "⚪ FORA_DA_FAIXA"
        elif st_txt in ("ADIADO", "DUPLICADO", "SEM_PLACAR") or res in ("ADIADO", "DUPLICADO", "SEM_PLACAR"):
            return "⚪ " + (st_txt if st_txt in ("ADIADO", "DUPLICADO", "SEM_PLACAR") else res)   # fora da conta: nao e pendente
        elif "SKIP" in res or str(r.get("Status_Odd", "")).upper() == "ODD_INVALIDA_SKIP":
            return "⚪ SKIP"
            
        # 4. Auto-Settlement Inteligente por Placar (ex: "2x0", "1x3", "0x0", "2-1")
        plc = str(r.get("Placar", "")).strip().lower().replace("-", "x")
        met = str(r.get("Método", ""))
        if "x" in plc and plc != "vs" and "?" not in plc:
            partes = plc.split("x")
            try:
                gh = int(partes[0].strip())
                ga = int(partes[1].strip())
                if "0x0" in met:
                    return "🔴 RED" if (gh == 0 and ga == 0) else "🟢 GREEN"
                elif "Draw" in met:
                    return "🟢 GREEN" if gh != ga else "🔴 RED"
                elif "Home" in met or "X2" in met:
                    return "🟢 GREEN" if ga >= gh else "🔴 RED"
                elif "Over 4.5" in met:
                    return "🟢 GREEN" if (gh + ga) <= 4 else "🔴 RED"
                elif "2x2" in met:
                    return "🔴 RED" if (gh == 2 and ga == 2) else "🟢 GREEN"
                elif "0x3" in met:
                    return "🔴 RED" if (gh == 0 and ga == 3) else "🟢 GREEN"
                elif "0x1" in met:
                    return "🔴 RED" if (gh == 0 and ga == 1) else "🟢 GREEN"
                elif "Under 0.5" in met:
                    return "🔴 RED" if (gh == 0 and ga == 0) else "🟢 GREEN"
                elif "Away" in met or "1X" in met:
                    return "🔴 RED" if (ga > gh) else "🟢 GREEN"
                elif "0x2" in met:
                    return "🔴 RED" if (gh == 0 and ga == 2) else "🟢 GREEN"
                elif "2x0" in met:
                    return "🔴 RED" if (gh == 2 and ga == 0) else "🟢 GREEN"
            except Exception:
                pass
                
        return "⏳ PENDENTE"
        
    df_all["Status"] = df_all.apply(_calc_status, axis=1)
        
    # 4. Normalização de PnL em Unidades e Reais (Fórmula com comissão dinâmica)
    def _calc_pnl_u(r):
        st_val = str(r.get("Status", ""))
        odd = float(r.get("Odd_Entrada", 5.0))
        if odd <= 1.0:
            odd = 1.05
        if "GREEN" in st_val:
            return round(fator_comissao / (odd - 1.0), 4)
        elif "RED" in st_val:
            return -1.0
        return 0.0
        
    df_all["PnL_u"] = df_all.apply(_calc_pnl_u, axis=1)
    def _calc_liab_rs(met):
        m_str = str(met)
        if "Diferenciada" in tipo_gestao:
            if "Over 4.5" in m_str:
                return round(banca_total * 0.15, 2)
            elif "0x3" in m_str or "2x2" in m_str or "0x0" in m_str:
                return round(banca_total * 0.10, 2)
            elif "Home" in m_str or "X2" in m_str or "Draw" in m_str:
                return round(banca_total * 0.05, 2)
            return round(banca_total * 0.05, 2)
        elif "10.0%" in tipo_gestao:
            return round(banca_total * 0.10, 2)
        else:
            return round(banca_total * 0.05, 2)

    df_all["Liability_R$"] = df_all["Método"].apply(_calc_liab_rs)
    def _calc_pnl_rs(r):
        liab = _calc_liab_rs(r.get("Método", ""))
        return round(float(r.get("PnL_u", 0.0)) * liab, 2)
    df_all["PnL_Reais"] = df_all.apply(_calc_pnl_rs, axis=1)
    
    return df_all.sort_values(["Data", "Método"]).reset_index(drop=True)

df_raw = carregar_dados_aprovados(
    modo=fonte_dados,
    banca_total=banca_total,
    tipo_gestao=tipo_gestao,
    fator_comissao=fator_comissao
)

if df_raw.empty:
    st.warning("Nenhuma planilha encontrada na pasta `metodos_aprovados/`.")
    st.stop()

# ── Filtros no Topo ──
st.markdown("### 🔍 Filtros de Visualização")
f_c1, f_c2, f_c3 = st.columns(3)

metodos_disponiveis = sorted(df_raw["Método"].dropna().unique().tolist()) if "Método" in df_raw.columns else []
with f_c1:
    filtro_metodo = st.multiselect("Filtrar por Método", metodos_disponiveis, default=metodos_disponiveis)
with f_c2:
    status_disponiveis = sorted(df_raw["Status"].dropna().unique().tolist()) if "Status" in df_raw.columns else ["🟢 GREEN", "🔴 RED", "⏳ PENDENTE"]
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
# KPIs do topo: a Ampla (Paralelo) tem os MESMOS jogos do 0x3 Top 3 e as Zebras sao observacao -> fora do total
_fora_total = df_filt["Método"].astype(str).str.contains("Paralelo|Zebra|Micro-Liability", na=False)
if _fora_total.any():
    st.caption("ℹ️ KPIs do topo excluem **Lay 0x3 (Regra Ampla - Paralelo)** — mesmos jogos do 0x3 Top 3 — e as Zebras (observação). Elas seguem nas abas por método.")
df_liq = df_filt[~_fora_total & df_filt["Status"].isin(["🟢 GREEN", "🔴 RED"])].copy()

total_jogos = int((~_fora_total).sum())
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
kpi4.metric(
    f"Lucro em R$ (Banca R$ {banca_total:,.0f})",
    f"R$ {pnl_total_rs:+,.2f}",
    delta=f"{(pnl_total_rs / banca_total) * 100:+.1f}% sobre banca" if banca_total > 0 else ""
)
kpi5.metric("Pendentes / Ao Vivo", f"{pendentes} jogos")
st.markdown("---")

# ── Abas de Análise ──
tab_geral, tab_metodo, tab_dia, tab_grafico, tab_comparativo = st.tabs([
    "📋 Planilha Completa Jogo a Jogo",
    "🎯 Desempenho por Método",
    "📅 Desempenho Dia a Dia",
    "📈 Curva de Lucro Acumulado",
    "⚖️ Comparativo: Regra Base vs Filtros Novos"
])

# 1. PLANILHA GERAL
with tab_geral:
    st.subheader("📋 Tabela Consolidada de Jogos")
    
    cols_exibir = [
        "Data", "Hora", "Liga", "Jogo", "Método", "Mercado", "Lado", 
        "Odd_Entrada", "Odd_Fav", "Placar", "Status", "Liability_R$", "PnL_u", "PnL_Reais"
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
            roi_m = (pnl_m_u / n_m * 100) if n_m > 0 else 0.0
            liab_ex = g["Liability_R$"].iloc[0] if "Liability_R$" in g.columns else (banca_total * 0.05)
            pct_ex = (liab_ex / banca_total * 100) if banca_total > 0 else 5.0
            res_m.append({
                "Método": m,
                "% Banca (Risco)": f"{pct_ex:.1f}%",
                "Liability / Entrada": f"R$ {liab_ex:,.2f}",
                "Total Jogos": n_m,
                "Greens": w_m,
                "Reds": r_m,
                "Win Rate %": f"{wr_m:.1f}%",
                "ROI s/ Liability": f"{roi_m:+.2f}%",
                "Lucro Líquido (u)": round(pnl_m_u, 3),
                "Lucro Líquido (R$)": f"R$ {pnl_m_rs:+,.2f}"
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

        # Simulação do Cenário B7: Gestão Dinâmica (Juros Compostos 5%/10%) + Stop Red (-10%) & Stop Green (+10%)
        def _hm_min(h):
            try:
                p = str(h).strip()[:5].split(":")
                return int(p[0]) * 60 + int(p[1])
            except Exception:
                return 15 * 60

        _df_b7 = df_liq.copy()
        _df_b7["ko_min"] = _df_b7["Hora"].apply(_hm_min)
        _df_b7["end_min"] = _df_b7["ko_min"] + 115
        _df_b7["pct_liab"] = _df_b7["Método"].apply(
            lambda m: 0.15 if "Over 4.5" in str(m) else (0.10 if ("0x3" in str(m) or "2x2" in str(m) or "0x0" in str(m)) else 0.05)
        )
        _df_b7 = _df_b7.sort_values(["Data", "ko_min", "Método"]).reset_index(drop=True)

        _banca_comp = float(banca_total)
        _rows_b7 = []
        for _dt, _g in _df_b7.groupby(_df_b7["Data"].dt.strftime("%Y-%m-%d")):
            _g = _g.sort_values("ko_min").copy()
            _b_dia = _banca_comp
            _g["liab_b7"] = _g["pct_liab"] * _b_dia
            _g["pnl_b7_rs"] = _g["PnL_u"] * _g["liab_b7"]
            _stop_t = 999999
            for _t in sorted(_g["end_min"].unique()):
                _done = _g[(_g["end_min"] <= _t) & (_g["ko_min"] < _stop_t)]
                _pnl_now = _done["pnl_b7_rs"].sum()
                if _pnl_now <= -0.10 * _b_dia or _pnl_now >= 0.10 * _b_dia:
                    _stop_t = _t
                    break
            _kept = _g[_g["ko_min"] < _stop_t].copy()
            _banca_comp += _kept["pnl_b7_rs"].sum()
            _rows_b7.append(_kept)

        if _rows_b7:
            df_b7_res = pd.concat(_rows_b7, ignore_index=True)
            df_b7_res["Num_Jogo"] = range(1, len(df_b7_res) + 1)
            df_b7_res["Lucro_Acumulado_B7_R$"] = df_b7_res["pnl_b7_rs"].cumsum()
            df_b7_res["Banca_Composta_B7_R$"] = float(banca_total) + df_b7_res["Lucro_Acumulado_B7_R$"]

            st.markdown("#### 🏆 Cenário B7: Gestão Dinâmica (Juros Compostos 5%/10%) + Stop Red (-10%) & Stop Green (+10%)")
            c_b7_1, c_b7_2, c_b7_3, c_b7_4 = st.columns(4)
            _lucro_b7 = df_b7_res["Lucro_Acumulado_B7_R$"].iloc[-1]
            _banca_fim_b7 = df_b7_res["Banca_Composta_B7_R$"].iloc[-1]
            _gr_b7 = (df_b7_res["Status"] == "🟢 GREEN").sum()
            _rd_b7 = (df_b7_res["Status"] == "🔴 RED").sum()
            c_b7_1.metric("Banca Final (B7)", f"R$ {_banca_fim_b7:,.2f}", f"Início: R$ {banca_total:,.0f}")
            c_b7_2.metric("Lucro Líquido (B7)", f"R$ {_lucro_b7:+,.2f}", f"{(_lucro_b7 / banca_total * 100):+.1f}% sobre banca")
            c_b7_3.metric("Jogos Executados", f"{len(df_b7_res)} jogos", f"{len(df_liq) - len(df_b7_res)} cortados pelo Stop")
            c_b7_4.metric("Win Rate (B7)", f"{(_gr_b7 / len(df_b7_res) * 100):.1f}%", f"{_gr_b7}G / {_rd_b7}R")

            st.line_chart(df_b7_res.set_index("Num_Jogo")["Lucro_Acumulado_B7_R$"])
            st.caption("Evolução do Lucro Acumulado (em R$) no Cenário B7 (Gestão Dinâmica Composta 5%/10% + Stop Diário -10%/+10%).")

        st.markdown("---")
        st.markdown("#### 📊 Curva de Lucro Acumulado — Banca Fixa Sem Stop (Referência)")
        st.line_chart(df_liq_sorted.set_index("Num_Jogo")["PnL_Acum_Reais"])
        st.caption("Evolução do saldo financeiro acumulado (em R$) com Banca Fixa sem Stop.")
    else:
        st.info("Sem dados suficientes para gerar a curva de equity.")

# 5. COMPARATIVO DIRETO: REGRA BASE vs FILTROS NOVOS
with tab_comparativo:
    st.subheader("⚖️ Prova Forense de Overfitting: Regra Base vs Filtros Refinados")
    st.info("""
    🔬 **TESTE DURO DE GOVERNANÇA: SUB-CONJUNTO MANTIDO vs DESCARTADO (Kept-vs-Discarded)**
    Ao decompor os 189 jogos no teste mantido-vs-descartado, a estatística comprova:
    * **Lay Draw:** O filtro refinado MANTIDO deu ROI **+4,0%** (N=46), enquanto o subconjunto DESCARTADO rendeu **+6,0%** (N=68).
    * **Lay Home:** O filtro refinado MANTIDO deu ROI **+6,4%** (N=32), enquanto o subconjunto DESCARTADO rendeu **+8,7%** (N=31).
    * **Veredito Científico:** O filtro refinado fica sistematicamente com a **metade pior** e corta favoritos visitantes mais fortes (o oposto do que o scan alegou). Isso comprova **sobreajuste (overfitting / garden of forking paths)**. Por isso a governança arquivou o filtro refinado e mantém a Regra Base Ampla.
    """)
    
    if "Passa_Filtro_Refinado" in df_raw.columns:
        df_base = df_raw.copy()
        df_ref = df_raw[df_raw["Passa_Filtro_Refinado"] == True].copy()
        df_cortados = df_raw[df_raw["Passa_Filtro_Refinado"] == False].copy()
        
        n_base = len(df_base[df_base["Status"].isin(["🟢 GREEN", "🔴 RED"])])
        w_base = (df_base["Status"] == "🟢 GREEN").sum()
        pnl_base_u = df_base[df_base["Status"].isin(["🟢 GREEN", "🔴 RED"])]["PnL_u"].sum()
        pnl_base_rs = pnl_base_u * (banca_total * 0.05)
        wr_base = (w_base / n_base * 100) if n_base > 0 else 0
        
        n_ref = len(df_ref[df_ref["Status"].isin(["🟢 GREEN", "🔴 RED"])])
        w_ref = (df_ref["Status"] == "🟢 GREEN").sum()
        pnl_ref_u = df_ref[df_ref["Status"].isin(["🟢 GREEN", "🔴 RED"])]["PnL_u"].sum()
        pnl_ref_rs = pnl_ref_u * (banca_total * 0.05)
        wr_ref = (w_ref / n_ref * 100) if n_ref > 0 else 0
        
        diff_n = n_ref - n_base
        diff_pnl_u = pnl_ref_u - pnl_base_u
        diff_pnl_rs = pnl_ref_rs - pnl_base_rs
        
        col_c1, col_c2, col_c3 = st.columns(3)
        with col_c1:
            st.markdown("### 👑 Regra Base Ampla")
            st.metric("Total de Jogos", f"{n_base} partidas")
            st.metric("Win Rate Real", f"{wr_base:.1f}%", f"{w_base} Greens / {n_base - w_base} Reds")
            st.metric("Lucro Líquido", f"{pnl_base_u:+.2f} u", f"R$ {pnl_base_rs:+,.2f}")
            
        with col_c2:
            st.markdown("### 🔬 Filtros Novos Refinados")
            st.metric("Total de Jogos", f"{n_ref} partidas", f"{diff_n} jogos descartados")
            st.metric("Win Rate Real", f"{wr_ref:.1f}%", f"{w_ref} Greens / {n_ref - w_ref} Reds")
            st.metric("Lucro Líquido", f"{pnl_ref_u:+.2f} u", f"R$ {pnl_ref_rs:+,.2f}")
            
        with col_c3:
            st.markdown("### 📊 Diferença de Performance")
            st.metric("Volume Descartado", f"{abs(diff_n)} jogos a menos", delta=f"{diff_n} jogos", delta_color="inverse")
            st.metric("Diferença na Win Rate", f"{wr_ref - wr_base:+.1f} pp", delta=f"{wr_ref - wr_base:+.1f} pp")
            st.metric("Lucro Deixado na Mesa", f"{diff_pnl_u:+.2f} u", delta=f"R$ {diff_pnl_rs:+,.2f}", delta_color="inverse")

        st.markdown("---")
        st.subheader("🎯 Desempenho Desdobrado por Método")
        
        comp_metodos = []
        for m in sorted(df_base["Método"].unique()):
            sub_b = df_base[(df_base["Método"] == m) & (df_base["Status"].isin(["🟢 GREEN", "🔴 RED"]))]
            sub_r = df_ref[(df_ref["Método"] == m) & (df_ref["Status"].isin(["🟢 GREEN", "🔴 RED"]))]
            
            nb = len(sub_b)
            wb = (sub_b["Status"] == "🟢 GREEN").sum()
            pnl_b = sub_b["PnL_u"].sum()
            
            nr = len(sub_r)
            wr = (sub_r["Status"] == "🟢 GREEN").sum()
            pnl_r = sub_r["PnL_u"].sum()
            
            comp_metodos.append({
                "Método": m,
                "Jogos (Base)": nb,
                "WR (Base)": f"{(wb/nb*100):.1f}%" if nb>0 else "0%",
                "PnL (Base)": f"{pnl_b:+.2f} u",
                "Jogos (Filtro Novo)": nr,
                "WR (Filtro Novo)": f"{(wr/nr*100):.1f}%" if nr>0 else "0%",
                "PnL (Filtro Novo)": f"{pnl_r:+.2f} u",
                "Diferença PnL": f"{(pnl_r - pnl_b):+.2f} u"
            })
        st.dataframe(pd.DataFrame(comp_metodos), use_container_width=True, hide_index=True)
        
        st.markdown("---")
        st.subheader("🔍 Jogos Que os Filtros Novos Descartaram (E o que aconteceu neles)")
        st.caption("Abaixo estão os 99 jogos cortados pelos filtros refinados: veja como a grande maioria deu Green com tranquilidade.")
        
        cols_corte = ["Data", "Liga", "Jogo", "Método", "Odd_Fav", "Odd_Entrada", "Placar", "Status", "PnL_u", "Detalhe_Filtro"]
        cols_corte_disp = [c for c in cols_corte if c in df_cortados.columns]
        df_cortados_show = df_cortados[cols_corte_disp].copy()
        df_cortados_show["Data"] = pd.to_datetime(df_cortados_show["Data"]).dt.strftime("%d/%m/%Y")
        st.dataframe(df_cortados_show, use_container_width=True, hide_index=True)
    else:
        st.info("A coluna de filtros refinados não está presente na base selecionada.")
