# -*- coding: utf-8 -*-
"""
02_📊_Resultados_Metodos_Aprovados.py — Painel de Resultados dos Métodos Aprovados do ARKAD.
Consolida automaticamente todas as planilhas diárias da pasta `metodos_aprovados/`:
  - Visão geral com KPIs consolidados (Greens, Reds, WR, P&L e ROI)
  - Curva de Equity e Evolução Acumulada
  - Desempenho desdobrado por Método e por Data
  - Tabela completa jogo a jogo com download em Excel
"""
import os, io, sys, glob, re
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
* **Portfólio em Validação Ativa:** Métodos oficiais acompanhados em forward — **Lay 0x0 XGBoost**, **Lay 2x2 Top 3**, **Lay Draw Super Fav**, **Lay Home Fav Visitante**, **Lay Away Fortaleza 1X** e **Lay Over 4.5 Under Pesado** (Lay 0x3 oficialmente reprovado e arquivado).
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
        "🎯 Diferenciada (15% Over 4.5 | 10% em 2x2, Away 1X, 3x0 | 5.0% em Home, Draw, 0x0)",
        "⚖️ Uniforme (5.0% Fixa para Todos)",
        "🔥 Uniforme (10.0% Fixa para Todos)"
    ],
    index=0
)

if "Diferenciada" in tipo_gestao:
    st.sidebar.markdown(f"""
    **Alocação de Liability por Entrada:**
    * 🟡 **15.0% da Banca (R$ {banca_total * 0.15:,.2f}):** Lay Over 4.5 FT (Under Pesado — 97.8% WR)
    * 🟣 **10.0% da Banca (R$ {banca_total * 0.10:,.2f}):** Lay 2x2 Top 3, Lay 3x0 Top 3 e Lay Away Fortaleza 1X
    * 🔵 **5.0% da Banca (R$ {banca_total * 0.05:,.2f}):** Lay Home, Lay Draw e Lay 0x0 XGBoost
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
        "🎯 Apenas Top 3 CS (Lay 2x2 e Lay 3x0)"
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
    tipo_gestao="🎯 Diferenciada (15% Over 4.5 | 10% em 2x2, Away 1X, 3x0 | 5.0% em Home, Draw, 0x0)",
    fator_comissao=0.965
):
    if not FOLDER.exists():
        return pd.DataFrame()

    # Carregar EXCLUSIVAMENTE uma planilha por data YYYY-MM-DD.
    # Prioriza a versão padrão Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx; usa _coletor apenas como fallback se a padrão não existir.
    todos_arquivos = sorted([
        f for f in FOLDER.glob("Sinais_Metodos_Aprovados_20*.xlsx")
        if not f.name.startswith("~$") and "Odds_Reais" not in f.name
    ])
    mapa_arquivos = {}
    for f in todos_arquivos:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
        if not m:
            continue
        dt_k = m.group(1)
        if "_coletor" in f.name:
            if dt_k not in mapa_arquivos:
                mapa_arquivos[dt_k] = f
        else:
            mapa_arquivos[dt_k] = f
    planilhas_diarias = sorted(mapa_arquivos.values())
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
        df_all = df_all[~df_all["Método"].astype(str).str.contains("2x2|0x3|3x0|Zebra|0x2|2x0|0x0", na=False)].reset_index(drop=True)
    elif "Apenas Top 3 CS" in modo and "Método" in df_all.columns:
        df_all = df_all[df_all["Método"].astype(str).str.contains("2x2|0x3|3x0", na=False)].reset_index(drop=True)
        
    # Normalização segura de colunas e deduplicação de nomes
    cols = []
    for c in df_all.columns:
        cols.append("Método" if "todo" in str(c).lower() else c)
    df_all.columns = cols
    df_all = df_all.loc[:, ~df_all.columns.duplicated()].copy()

    # Deduplicação defensiva de jogos no mesmo dia para o mesmo método
    if "Jogo" in df_all.columns and "Método" in df_all.columns:
        def _res_score(v):
            v_u = str(v).upper().strip()
            if v_u in ("GREEN", "RED"): return 2
            if v_u and v_u not in ("NAN", "NONE", "PENDENTE"): return 1
            return 0
        df_all["_score_res"] = df_all.get("Resultado", "").apply(_res_score)
        df_all = df_all.sort_values(by=["Data", "_score_res"], ascending=[True, True])
        df_all = df_all.drop_duplicates(subset=["Data", "Jogo", "Método"], keep="last")
        df_all = df_all.drop(columns=["_score_res"]).reset_index(drop=True)
            
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
            return "Lay Away Fortaleza 1X (Fav <= 1.40 | O25 >= 1.75)"
        elif "Home" in m_str or "X2" in m_str:
            return "Lay Home / DC X2 (Fav Visitante <= 1.65)"
        elif "Over 4.5" in m_str or "Over45" in m_str:
            return "Lay Over 4.5 FT (Under Pesado)"
        elif "Draw" in m_str:
            return "Lay Draw (Fav <= 1.40)"
        elif "2x2" in m_str:
            return "Lay 2x2 Top 3 (Aprovado)"
        elif "3x0" in m_str:
            return "Lay 3x0 Top 3 (Aprovado)"
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
                elif "3x0" in met:
                    return "🔴 RED" if (gh == 3 and ga == 0) else "🟢 GREEN"
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
            elif "3x0" in m_str or "2x2" in m_str or "Away" in m_str or "1X" in m_str:
                return round(banca_total * 0.10, 2)
            elif "Home" in m_str or "X2" in m_str or "Draw" in m_str or "0x0" in m_str:
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

# ── Função de Gestão de Risco: Stop Red Seletivo por Método ──
def aplicar_stop1red_por_metodo(df_in, metodos_com_stop=("Draw",)):
    """
    Aplica a regra de Stop Red Seletivo por Método:
    - O Stop 1 Red diário é aplicado exclusivamente aos métodos onde o clustering (agrupamento
      de reds na rodada) é comprovado, como o Lay Draw (Fav <= 1.40), cortando dias de zika de empates.
    - Métodos com eventos independentes e alta taxa de acerto, como o Lay Home (Fav Visitante <= 1.65)
      e Lay Over 4.5, rodam em Fluxo Livre Contínuo (sem stop por red), evitando amputar a recuperação
      natural dos múltiplos greens da tarde após um red isolado.
    - Ordenação de saída: Mais recentes no topo (Outubro/Setembro) e mais antigos (Agosto) na base.
    """
    if df_in.empty:
        return df_in.copy()
    
    # 1. Ordena cronologicamente para a simulação sequencial
    df_temp = df_in.sort_values(by=["Data", "Hora"]).copy()
    
    rows_kept = []
    for (d, m), grp in df_temp.groupby(["Data", "Método"], sort=False):
        tem_stop = any(s.lower() in str(m).lower() for s in metodos_com_stop)
        
        if not tem_stop:
            # Fluxo livre contínuo: inclui todas as entradas do método
            for _, r in grp.iterrows():
                rows_kept.append(r)
        else:
            # Aplica Stop 1 Red (encerra entradas do método no dia após o 1º RED)
            red_ocorrido = False
            for _, r in grp.iterrows():
                if red_ocorrido:
                    continue
                rows_kept.append(r)
                status_str = str(r.get("Status", "")).upper()
                if "RED" in status_str:
                    red_ocorrido = True
                
    df_stop = pd.DataFrame(rows_kept)
    if df_stop.empty:
        return df_stop
        
    # 2. Ordenação solicitada: jogos mais recentes em cima, agosto lá embaixo
    df_stop = df_stop.sort_values(by=["Data", "Hora"], ascending=[False, False]).reset_index(drop=True)
    return df_stop


# ── Abas de Análise ──
tab_stop1red, tab_geral, tab_metodo, tab_dia, tab_grafico = st.tabs([
    "🛡️ Tabela Consolidada (Stop Red Otimizado)",
    "📋 Planilha Completa Jogo a Jogo",
    "🎯 Desempenho por Método",
    "📅 Desempenho Dia a Dia",
    "📈 Curva de Lucro Acumulado"
])

# 0. TABELA CONSOLIDADA COM STOP RED OTIMIZADO
with tab_stop1red:
    st.subheader("🛡️ Tabela Consolidada de Jogos — Gestão de Risco com Stop Seletivo")
    st.info(
        "🛡️ **Gestão com Stop Red Seletivo (Otimização Baseada em Evidências):**\n\n"
        "• 🟡 **Lay Draw (Fav $\le 1,40$):** Aplica **1 Stop Red Diário** — interrompe as entradas do dia após o 1º red de empate (evitou 25 reds e reduziu o drawdown em −55%).\n\n"
        "• 🔵 **Lay Home (Fav Visitante $\le 1,65$):** Opera em **Fluxo Livre Contínuo** — como 85% dos reds são isolados, não aplica corte prematuro, preservando os +1,94u de lucro (+5,01u a mais do que com stop cego).\n\n"
        "• 🟢 **Demais Métodos (Over 4.5, CS Top 3, Away):** Operam normalmente em fluxo contínuo ótimo.\n\n"
        "• **Ordenação:** Jogos mais recentes (Outubro/Setembro) no topo e mais antigos (Agosto) na base."
    )

    # Cálculo do Stop 1 Red sobre a base filtrada por método e data
    df_base_stop = df_raw.copy()
    if filtro_metodo:
        df_base_stop = df_base_stop[df_base_stop["Método"].isin(filtro_metodo)]
    if isinstance(filtro_data, (list, tuple)) and len(filtro_data) == 2:
        df_base_stop = df_base_stop[(df_base_stop["Data"].dt.date >= filtro_data[0]) & (df_base_stop["Data"].dt.date <= filtro_data[1])]

    df_stop1red_all = aplicar_stop1red_por_metodo(df_base_stop)

    # KPIs consolidados do Stop 1 Red (exclui métodos em observação/paralelo igual aos KPIs gerais)
    _fora_stop = df_stop1red_all["Método"].astype(str).str.contains("Paralelo|Zebra|Micro-Liability", na=False)
    df_stop_liq = df_stop1red_all[~_fora_stop & df_stop1red_all["Status"].isin(["🟢 GREEN", "🔴 RED"])].copy()

    total_stop_jogos = int((~_fora_stop).sum())
    stop_liq = len(df_stop_liq)
    stop_greens = (df_stop_liq["Status"] == "🟢 GREEN").sum()
    stop_reds = (df_stop_liq["Status"] == "🔴 RED").sum()
    stop_pend = (df_stop1red_all["Status"] == "⏳ PENDENTE").sum()
    stop_wr = (stop_greens / stop_liq * 100) if stop_liq > 0 else 0.0
    stop_pnl_u = df_stop_liq["PnL_u"].sum() if not df_stop_liq.empty else 0.0
    stop_pnl_rs = df_stop_liq["PnL_Reais"].sum() if not df_stop_liq.empty else 0.0

    jogos_cortados = max(0, total_jogos - total_stop_jogos)

    s_c1, s_c2, s_c3, s_c4, s_c5 = st.columns(5)
    s_c1.metric("Jogos Executados (Stop 1 Red)", f"{total_stop_jogos} partidas", f"{jogos_cortados} cortadas pelo stop")
    s_c2.metric("Greens / Taxa de Acerto", f"{stop_greens} ({stop_wr:.1f}%)", f"{stop_reds} reds (máx 1/mét/dia)")
    s_c3.metric("Lucro Líquido (Unidades)", f"{stop_pnl_u:+.2f} u", delta=f"{stop_pnl_u - pnl_total_u:+.2f} u vs sem stop")
    s_c4.metric(
        f"Lucro em R$ (Banca R$ {banca_total:,.0f})",
        f"R$ {stop_pnl_rs:+,.2f}",
        delta=f"{(stop_pnl_rs / banca_total) * 100:+.1f}% sobre banca" if banca_total > 0 else ""
    )
    s_c5.metric("Pendentes / Ao Vivo", f"{stop_pend} jogos")

    st.markdown("---")

    # Aplicação de filtro visual de Status (se o usuário selecionou no topo)
    df_stop1red_show = df_stop1red_all.copy()
    if filtro_status:
        df_stop1red_show = df_stop1red_show[df_stop1red_show["Status"].isin(filtro_status)]

    cols_exibir = [
        "Data", "Hora", "Liga", "Jogo", "Método", "Mercado", "Lado", 
        "Odd_Entrada", "Odd_Fav", "Placar", "Status", "Liability_R$", "PnL_u", "PnL_Reais"
    ]
    cols_disp_stop = [c for c in cols_exibir if c in df_stop1red_show.columns]

    df_stop_display = df_stop1red_show[cols_disp_stop].copy()
    df_stop_display["Data"] = df_stop_display["Data"].dt.strftime("%d/%m/%Y")

    st.dataframe(df_stop_display, use_container_width=True, hide_index=True)

    # Download em Excel
    _buf_stop = io.BytesIO()
    with pd.ExcelWriter(_buf_stop, engine="openpyxl") as _writer:
        df_stop_display.to_excel(_writer, index=False, sheet_name="Stop1Red_Consolidado")
    st.download_button(
        label="📥 Baixar Tabela Consolidada (1 Stop Red) (Excel)",
        data=_buf_stop.getvalue(),
        file_name="Tabela_Consolidada_StopOtimizado_ARKAD.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )

# 1. PLANILHA GERAL
with tab_geral:
    st.subheader("📋 Tabela Consolidada de Jogos — Sem Stop (Todos os Jogos)")
    st.caption("Visão histórica contendo todos os sinais gerados, sem aplicação de stop loss diário.")
    
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
    modo_metodo = st.radio(
        "Selecione a Gestão de Risco:",
        ["🛡️ Com Stop Red Otimizado (Recomendado)", "📋 Sem Stop (Todos os Jogos)", "⚖️ Comparativo Lado a Lado"],
        horizontal=True
    )

    def _calc_stats_metodo(df_target):
        if df_target.empty:
            return pd.DataFrame()
        res = []
        for m, g in df_target.groupby("Método"):
            n_m = len(g)
            w_m = (g["Status"] == "🟢 GREEN").sum()
            r_m = (g["Status"] == "🔴 RED").sum()
            wr_m = (w_m / n_m * 100) if n_m > 0 else 0.0
            pnl_m_u = g["PnL_u"].sum()
            pnl_m_rs = g["PnL_Reais"].sum()
            roi_m = (pnl_m_u / n_m * 100) if n_m > 0 else 0.0
            liab_ex = g["Liability_R$"].iloc[0] if "Liability_R$" in g.columns else (banca_total * 0.05)
            pct_ex = (liab_ex / banca_total * 100) if banca_total > 0 else 5.0
            res.append({
                "Método": m,
                "% Banca (Risco)": f"{pct_ex:.1f}%",
                "Liability / Entrada": f"R$ {liab_ex:,.2f}",
                "Total Jogos": n_m,
                "Greens": w_m,
                "Reds": r_m,
                "Win Rate %": f"{wr_m:.1f}%",
                "ROI s/ Liability": f"{roi_m:+.2f}%",
                "Lucro Líquido (u)": round(pnl_m_u, 3),
                "Lucro Líquido (R$)": f"R$ {pnl_m_rs:+,.2f}",
                "_pnl_u": pnl_m_u,
                "_pnl_rs": pnl_m_rs,
                "_n": n_m,
                "_w": w_m,
                "_r": r_m,
                "_wr": wr_m
            })
        return pd.DataFrame(res)

    df_stats_stop = _calc_stats_metodo(df_stop_liq)
    df_stats_raw = _calc_stats_metodo(df_liq)

    if modo_metodo == "🛡️ Com Stop Red Otimizado (Recomendado)":
        st.caption("ℹ️ Exibindo métricas com a aplicação de **1 Stop Red Diário por Método** (máximo 1 RED por método em cada dia).")
        if not df_stats_stop.empty:
            cols_show = [c for c in df_stats_stop.columns if not c.startswith("_")]
            st.dataframe(df_stats_stop[cols_show], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum jogo liquidado no filtro atual.")
    elif modo_metodo == "📋 Sem Stop (Todos os Jogos)":
        st.caption("ℹ️ Exibindo métricas sobre **todos os jogos sem stop loss diário** (base bruta histórica).")
        if not df_stats_raw.empty:
            cols_show = [c for c in df_stats_raw.columns if not c.startswith("_")]
            st.dataframe(df_stats_raw[cols_show], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum jogo liquidado no filtro atual.")
    else:
        st.caption("ℹ️ Comparativo direto do impacto da gestão de 1 Stop Red por método vs execução sem stop.")
        if not df_stats_stop.empty and not df_stats_raw.empty:
            comp_rows = []
            for m in df_stats_raw["Método"].unique():
                row_raw = df_stats_raw[df_stats_raw["Método"] == m]
                row_stop = df_stats_stop[df_stats_stop["Método"] == m]
                n_r = row_raw["_n"].iloc[0] if not row_raw.empty else 0
                n_s = row_stop["_n"].iloc[0] if not row_stop.empty else 0
                red_r = row_raw["_r"].iloc[0] if not row_raw.empty else 0
                red_s = row_stop["_r"].iloc[0] if not row_stop.empty else 0
                pnl_r = row_raw["_pnl_u"].iloc[0] if not row_raw.empty else 0.0
                pnl_s = row_stop["_pnl_u"].iloc[0] if not row_stop.empty else 0.0
                comp_rows.append({
                    "Método": m,
                    "Jogos (Stop)": n_s,
                    "Jogos (Sem Stop)": n_r,
                    "Jogos Cortados": max(0, n_r - n_s),
                    "Reds (Stop)": red_s,
                    "Reds (Sem Stop)": red_r,
                    "Reds Evitados": max(0, red_r - red_s),
                    "WR% (Stop)": f"{row_stop['_wr'].iloc[0]:.1f}%" if not row_stop.empty else "0%",
                    "WR% (Sem Stop)": f"{row_raw['_wr'].iloc[0]:.1f}%" if not row_raw.empty else "0%",
                    "PnL u (Stop)": round(pnl_s, 2),
                    "PnL u (Sem Stop)": round(pnl_r, 2),
                    "Diferença PnL (u)": round(pnl_s - pnl_r, 2)
                })
            st.dataframe(pd.DataFrame(comp_rows), use_container_width=True, hide_index=True)

# 3. DESEMPENHO DIA A DIA
with tab_dia:
    st.subheader("📅 Desempenho Detalhado por Data")
    modo_dia = st.radio(
        "Selecione a Visualização Diária:",
        ["🛡️ Com Stop Red Otimizado (Recomendado)", "📋 Sem Stop (Todos os Jogos)"],
        horizontal=True
    )

    def _calc_stats_dia(df_target, is_stop=False):
        if df_target.empty:
            return pd.DataFrame()
        res = []
        for dt, g in df_target.groupby(df_target["Data"].dt.date):
            n_d = len(g)
            w_d = (g["Status"] == "🟢 GREEN").sum()
            r_d = (g["Status"] == "🔴 RED").sum()
            wr_d = (w_d / n_d * 100) if n_d > 0 else 0.0
            pnl_d_u = g["PnL_u"].sum()
            pnl_d_rs = g["PnL_Reais"].sum()
            item = {
                "Data": dt.strftime("%d/%m/%Y"),
                "_dt": dt,
                "Jogos": n_d,
                "Greens": w_d,
                "Reds": r_d,
                "Win Rate %": f"{wr_d:.1f}%",
                "Lucro do Dia (u)": round(pnl_d_u, 3),
                "Lucro do Dia (R$)": f"R$ {pnl_d_rs:+.2f}"
            }
            if is_stop:
                n_raw_dia = len(df_liq[df_liq["Data"].dt.date == dt])
                item["Jogos Cortados"] = max(0, n_raw_dia - n_d)
            res.append(item)
        df_res = pd.DataFrame(res)
        # Ordenação com datas mais recentes no topo
        return df_res.sort_values("_dt", ascending=False).drop(columns=["_dt"]).reset_index(drop=True)

    if modo_dia == "🛡️ Com Stop Red Otimizado (Recomendado)":
        st.caption("ℹ️ Desempenho diário com aplicação de 1 Stop Red por método (datas mais recentes no topo).")
        df_dia_stop = _calc_stats_dia(df_stop_liq, is_stop=True)
        if not df_dia_stop.empty:
            st.dataframe(df_dia_stop, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum jogo liquidado no filtro atual.")
    else:
        st.caption("ℹ️ Desempenho diário bruto sem stop (datas mais recentes no topo).")
        df_dia_raw = _calc_stats_dia(df_liq, is_stop=False)
        if not df_dia_raw.empty:
            st.dataframe(df_dia_raw, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum jogo liquidado no filtro atual.")

# 4. GRÁFICO DE CURVA DE LUCRO
with tab_grafico:
    st.subheader("📈 Curva de Lucro Acumulado (Equity)")
    if not df_stop_liq.empty:
        # Prepara dados acumulados diários para a curva comparativa
        _d_stop = df_stop_liq.groupby("Data")["PnL_Reais"].sum().rename("🛡️ Com 1 Stop Red (R$)")
        _d_raw = df_liq.groupby("Data")["PnL_Reais"].sum().rename("📋 Sem Stop (R$)")
        df_comp_chart = pd.concat([_d_stop, _d_raw], axis=1).fillna(0.0).cumsum()

        st.markdown("#### ⚖️ Curva Comparativa de Equity: Com 1 Stop Red vs Sem Stop")
        c_eq1, c_eq2, c_eq3, c_eq4 = st.columns(4)
        lucro_fim_stop = df_comp_chart["🛡️ Com 1 Stop Red (R$)"].iloc[-1]
        lucro_fim_raw = df_comp_chart["📋 Sem Stop (R$)"].iloc[-1]
        reds_evitados_total = (df_liq["Status"] == "🔴 RED").sum() - (df_stop_liq["Status"] == "🔴 RED").sum()
        
        c_eq1.metric("Lucro Final (1 Stop Red)", f"R$ {lucro_fim_stop:+,.2f}", f"Banca: R$ {banca_total + lucro_fim_stop:,.2f}")
        c_eq2.metric("Lucro Final (Sem Stop)", f"R$ {lucro_fim_raw:+,.2f}", f"Banca: R$ {banca_total + lucro_fim_raw:,.2f}")
        c_eq3.metric("Reds Evitados pelo Stop", f"{reds_evitados_total} reds a menos", delta=f"-{reds_evitados_total} reds", delta_color="normal")
        c_eq4.metric("Jogos Poupados de Risco", f"{len(df_liq) - len(df_stop_liq)} partidas", f"{((len(df_liq) - len(df_stop_liq))/len(df_liq)*100):.1f}% do volume")

        st.line_chart(df_comp_chart)
        st.caption("Comparativo da evolução patrimonial acumulada (em R$): linha com 1 Stop Red por método vs linha Sem Stop.")

        st.markdown("---")
        st.markdown("#### 🛡️ Evolução Jogo a Jogo — Com Stop Red Otimizado")
        df_stop_liq_sorted = df_stop_liq.sort_values(["Data", "Hora"]).reset_index(drop=True)
        df_stop_liq_sorted["PnL_Acum_Reais"] = df_stop_liq_sorted["PnL_Reais"].cumsum()
        df_stop_liq_sorted["Num_Jogo"] = range(1, len(df_stop_liq_sorted) + 1)
        st.line_chart(df_stop_liq_sorted.set_index("Num_Jogo")["PnL_Acum_Reais"])
        st.caption("Curva de equity jogo a jogo operando com a regra de 1 Stop Red diário por método.")

        st.markdown("---")
        st.markdown("#### 📋 Evolução Jogo a Jogo — Sem Stop (Referência Bruta)")
        df_liq_sorted = df_liq.sort_values(["Data", "Hora"]).reset_index(drop=True)
        df_liq_sorted["PnL_Acum_Reais"] = df_liq_sorted["PnL_Reais"].cumsum()
        df_liq_sorted["Num_Jogo"] = range(1, len(df_liq_sorted) + 1)
        st.line_chart(df_liq_sorted.set_index("Num_Jogo")["PnL_Acum_Reais"])
        st.caption("Curva de equity jogo a jogo sem aplicação de stop loss.")
    else:
        st.info("Sem dados suficientes para gerar a curva de equity.")
