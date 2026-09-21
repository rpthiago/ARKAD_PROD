# -*- coding: utf-8 -*-
"""
04_🔬_Observacao_Set_Out_2026.py — Laboratório de Observação Quantitativa (Setembro & Outubro 2026)
Central de monitoramento de métodos antigos em quarentena / resgate para validação prospectiva:
  1. Lay 1x1 c/ Favorito Forte (Fav <= 1.50 | Odd Lay 6.0 a 12.0)
  2. Lay 0x0 Super Favorito (Fav <= 1.40 | Odd Lay 7.0 a 18.0 ou Under 0.5 FT 8.0 a 25.0)
  3. Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70 | Odd Lay 6.0 a 25.0)
  4. Lay 0x2 Zebra Visitante (Fav Mandante <= 1.70 | Odd Lay 6.0 a 25.0)
  5. Lay 0x3 Zebra Visitante (Fav Mandante <= 1.60 | Odd Lay 15.0 a 50.0)

GOVERNANÇA: STAKE ZERO (Risco Real R$ 0,00). 100% odds reais de Lay da Betfair.
Apenas observação para verificar se os resultados de 2026 se sustentam em Setembro e Outubro.
"""
import os, io, sys, glob
from datetime import date, datetime, timedelta
from pathlib import Path
import numpy as np, pandas as pd, streamlit as st

try:
    import plotly.graph_objects as go
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

st.set_page_config(
    page_title="ARKAD — Laboratório de Observação Set/Out 2026",
    page_icon="🔬",
    layout="wide"
)

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from futpythontrader_client import get_daily_dataframe
except Exception:
    import importlib
    get_daily_dataframe = getattr(importlib.import_module("futpythontrader_client"), "get_daily_dataframe", None)

LEDGER_PATH = ROOT / "metodos_aprovados" / "observacao_set_out_ledger.csv"
FRESH3_PATH = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv"

# ── Estilização Visual ──
st.markdown("""
<style>
    .card-quarentena {
        background-color: #1e2638;
        border-radius: 10px;
        padding: 16px;
        border-left: 5px solid #ff9800;
        margin-bottom: 12px;
    }
    .card-observacao {
        background-color: #16202c;
        border-radius: 10px;
        padding: 14px;
        border: 1px solid #2a3b50;
        margin-bottom: 10px;
    }
    .badge-stake-zero {
        background-color: #e65100;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: bold;
        font-size: 0.85em;
    }
    .badge-zebra {
        background-color: #0288d1;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: bold;
        font-size: 0.85em;
    }
    .badge-green {
        background-color: #2e7d32;
        color: white;
        padding: 2px 6px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-red {
        background-color: #c62828;
        color: white;
        padding: 2px 6px;
        border-radius: 4px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ── Header & Governança ──
st.title("🔬 Laboratório de Observação Quantitativa — Setembro & Outubro 2026")
st.markdown("""
<div class="card-quarentena">
    <div style="display: flex; align-items: center; justify-content: space-between;">
        <span style="font-size: 1.15em; font-weight: bold; color: #ffb74d;">
            🛡️ STATUS DE GOVERNANÇA: QUARENTENA ESTATÍSTICA (OBSERVAÇÃO STAKE-ZERO)
        </span>
        <span class="badge-stake-zero">RISCO REAL: R$ 0,00</span>
    </div>
    <div style="font-size: 0.9em; color: #cfd8dc; margin-top: 8px; line-height: 1.5;">
        * <b>Por que esta página existe:</b> Na varredura dos métodos antigos arquivados, <b>5 métodos</b> apresentaram ROI positivo expressivo em 2026 (destaque para Lay 1x1 c/ Fav Forte com +2,28% e Zebras com +7,63%).<br>
        * <b>Por que NÃO apostar dinheiro real ainda:</b> Em 2024 e 2025, o Lay 1x1 foi negativo (−1,8% pela comissão da Betfair). Para aprovar qualquer método em produção, as regras do ARKAD (GEMINI.md) exigem <b>observação prospectiva de no mínimo 400 apostas ou 2 meses completos (Setembro e Outubro)</b> com odds reais executadas na Betfair Exchange.<br>
        * <b>Simulação:</b> Acompanhe os sinais gerados diariamente, o placar real e a curva de patrimônio teórica sem arriscar 1 centavo da sua banca.
    </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar: Configurações Globais ──
st.sidebar.header("⚙️ Parâmetros do Laboratório")

metodos_disponiveis = [
    "Lay 1x1 c/ Fav Forte (Fav <= 1.50)",
    "Lay 0x0 Super Fav (Fav <= 1.40)",
    "Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70)",
    "Lay 0x2 Zebra Visitante (Fav Mandante <= 1.70)",
    "Lay 0x3 Zebra Visitante (Fav Mandante <= 1.60)"
]

metodos_selecionados = st.sidebar.multiselect(
    "Métodos Ativos em Observação",
    options=metodos_disponiveis,
    default=metodos_disponiveis,
    help="Selecione quais métodos devem ser rastreados no radar diário e nos relatórios."
)

st.sidebar.markdown("---")
st.sidebar.subheader("💼 Simulação de Capital de Papel")
stake_teorica = st.sidebar.number_input(
    "Stake Teórica de Papel (R$ por 1.0u)",
    min_value=10.0, max_value=1000.0, value=50.0, step=10.0,
    help="Valor nominal de papel apenas para simular os ganhos/perdas virtuais sem risco real."
)

taxa_comissao = st.sidebar.selectbox(
    "Taxa de Comissão Betfair",
    options=[3.5, 5.0],
    format_func=lambda x: f"{x}% ({'Fórmula Pessoal (0.965)' if x == 3.5 else 'Padrão Conservador (0.950)'})",
    index=0
)
fator_comissao = 1.0 - (taxa_comissao / 100.0)

st.sidebar.markdown("---")
st.sidebar.info("""
**🎯 Critérios de Saída da Quarentena (Novembro):**
1. Amostra $N \ge 400$ apostas em Set/Out;
2. ROI positivo no período;
3. Bootstrap IC95% excluindo zero;
4. Resiliência sem clusters de reds.
""")

# ── Carregamento de Dados da FRESH3 com Cache ──
@st.cache_data(ttl=3600, show_spinner=False)
def carregar_historico_fresh3():
    if not FRESH3_PATH.exists():
        return pd.DataFrame()
    cols = [
        'Date', 'Time', 'League', 'Home', 'Away', 'Goals_H_FT', 'Goals_A_FT',
        'Odd_H_Back', 'Odd_A_Back', 'Odd_D_Back',
        'Odd_CS_1x1_Lay', 'Odd_CS_0x0_Lay', 'Odd_CS_2x0_Lay', 'Odd_CS_0x2_Lay', 'Odd_CS_0x3_Lay',
        'Odd_Under05_FT_Lay'
    ]
    try:
        df = pd.read_csv(FRESH3_PATH, usecols=cols, low_memory=False)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['gh'] = pd.to_numeric(df['Goals_H_FT'], errors='coerce')
        df['ga'] = pd.to_numeric(df['Goals_A_FT'], errors='coerce')
        df = df.dropna(subset=['Date', 'gh', 'ga'])
        df['fav'] = np.minimum(
            pd.to_numeric(df['Odd_H_Back'], errors='coerce').fillna(99),
            pd.to_numeric(df['Odd_A_Back'], errors='coerce').fillna(99)
        )
        df['oh'] = pd.to_numeric(df['Odd_H_Back'], errors='coerce')
        df['oa'] = pd.to_numeric(df['Odd_A_Back'], errors='coerce')
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        return df
    except Exception as e:
        return pd.DataFrame()

# ── Carregamento e Gestão do Ledger de Observação Set/Out ──
def carregar_ledger_observacao():
    if not LEDGER_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(LEDGER_PATH, encoding='utf-8-sig')
        return df
    except Exception:
        try:
            return pd.read_csv(LEDGER_PATH, encoding='utf-8')
        except Exception:
            return pd.DataFrame()

def salvar_no_ledger(novos_registros):
    df_existente = carregar_ledger_observacao()
    if df_existente.empty:
        df_final = pd.DataFrame(novos_registros)
    else:
        df_novos = pd.DataFrame(novos_registros)
        df_final = pd.concat([df_existente, df_novos], ignore_index=True)
        # Remove duplicatas por Data + Metodo + Home + Away
        df_final = df_final.drop_duplicates(subset=['Data', 'Metodo', 'Home', 'Away'], keep='last')
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(LEDGER_PATH, index=False, encoding='utf-8-sig')
    return len(df_final)

# ── Motor de Filtros de Oportunidades ──
def escanear_jogos(df_raw, metodos_ativos):
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()
    
    df = df_raw.copy()
    oh_back = pd.to_numeric(df.get('Odd_H_Back', df.get('Odd_H_FT_Back', df.get('Odd_H', 0))), errors='coerce').fillna(99)
    oa_back = pd.to_numeric(df.get('Odd_A_Back', df.get('Odd_A_FT_Back', df.get('Odd_A', 0))), errors='coerce').fillna(99)
    fav = np.minimum(oh_back, oa_back)
    
    l1x1 = pd.to_numeric(df.get('Odd_CS_1x1_Lay', df.get('Odd_CS_1x1', 0)), errors='coerce').fillna(0)
    l0x0 = pd.to_numeric(df.get('Odd_CS_0x0_Lay', df.get('Odd_CS_0x0', 0)), errors='coerce').fillna(0)
    l2x0 = pd.to_numeric(df.get('Odd_CS_2x0_Lay', df.get('Odd_CS_2x0', 0)), errors='coerce').fillna(0)
    l0x2 = pd.to_numeric(df.get('Odd_CS_0x2_Lay', df.get('Odd_CS_0x2', 0)), errors='coerce').fillna(0)
    l0x3 = pd.to_numeric(df.get('Odd_CS_0x3_Lay', df.get('Odd_CS_0x3', 0)), errors='coerce').fillna(0)
    lu05 = pd.to_numeric(df.get('Odd_Under05_FT_Lay', df.get('Odd_Under05_Lay', 0)), errors='coerce').fillna(0)

    oportunidades = []

    for idx, row in df.iterrows():
        h = str(row.get('Home', row.get('Home_Team', ''))).strip()
        a = str(row.get('Away', row.get('Away_Team', ''))).strip()
        if not h or not a:
            continue
        lg = str(row.get('League', row.get('Div', 'Liga'))).strip()
        tm = str(row.get('Time', row.get('horario', '15:00')))[:5]
        dt = str(row.get('Date', str(date.today())))[:10]

        f_val = float(fav.iloc[idx])
        h_odd = float(oh_back.iloc[idx])
        a_odd = float(oa_back.iloc[idx])

        # 1. Lay 1x1 c/ Fav Forte
        if "Lay 1x1 c/ Fav Forte (Fav <= 1.50)" in metodos_ativos:
            odd_lay = float(l1x1.iloc[idx])
            if f_val <= 1.50 and 6.0 <= odd_lay <= 12.0:
                be = ((odd_lay - 1.0) / (odd_lay - (1.0 - fator_comissao))) * 100.0
                oportunidades.append({
                    'Data': dt, 'Hora': tm, 'Liga': lg, 'Home': h, 'Away': a,
                    'Jogo': f"{h} x {a}", 'Método': 'Lay 1x1 c/ Fav Forte',
                    'Mercado': 'Correct Score (1x1)', 'Lado': 'LAY',
                    'Odd_Lay': odd_lay, 'Odd_Fav': f_val, 'Break_Even_%': round(be, 1),
                    'Regra': f"Fav {f_val:.2f} <= 1.50 | Odd Lay {odd_lay:.1f}",
                    'Risco_Nominal': 0.0,
                    'Stake_Teorica_R$': stake_teorica,
                    'Liability_Teorica_R$': round(stake_teorica * (odd_lay - 1.0), 2)
                })

        # 2. Lay 0x0 Super Fav
        if "Lay 0x0 Super Fav (Fav <= 1.40)" in metodos_ativos:
            odd_lay = float(l0x0.iloc[idx])
            if odd_lay == 0.0:
                odd_lay = float(lu05.iloc[idx])
            if f_val <= 1.40 and 7.0 <= odd_lay <= 18.0:
                be = ((odd_lay - 1.0) / (odd_lay - (1.0 - fator_comissao))) * 100.0
                oportunidades.append({
                    'Data': dt, 'Hora': tm, 'Liga': lg, 'Home': h, 'Away': a,
                    'Jogo': f"{h} x {a}", 'Método': 'Lay 0x0 Super Fav',
                    'Mercado': 'Correct Score (0x0)', 'Lado': 'LAY',
                    'Odd_Lay': odd_lay, 'Odd_Fav': f_val, 'Break_Even_%': round(be, 1),
                    'Regra': f"Super Fav {f_val:.2f} <= 1.40 | Odd Lay {odd_lay:.1f}",
                    'Risco_Nominal': 0.0,
                    'Stake_Teorica_R$': stake_teorica,
                    'Liability_Teorica_R$': round(stake_teorica * (odd_lay - 1.0), 2)
                })

        # 3. Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70)
        if "Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70)" in metodos_ativos:
            odd_lay = float(l2x0.iloc[idx])
            if a_odd <= 1.70 and 6.0 <= odd_lay <= 25.0:
                be = ((odd_lay - 1.0) / (odd_lay - (1.0 - fator_comissao))) * 100.0
                oportunidades.append({
                    'Data': dt, 'Hora': tm, 'Liga': lg, 'Home': h, 'Away': a,
                    'Jogo': f"{h} x {a}", 'Método': 'Lay 2x0 Zebra Mandante',
                    'Mercado': 'Correct Score (2x0)', 'Lado': 'LAY',
                    'Odd_Lay': odd_lay, 'Odd_Fav': a_odd, 'Break_Even_%': round(be, 1),
                    'Regra': f"Fav Visitante {a_odd:.2f} <= 1.70 | Odd Lay {odd_lay:.1f}",
                    'Risco_Nominal': 0.0,
                    'Stake_Teorica_R$': stake_teorica,
                    'Liability_Teorica_R$': round(stake_teorica * (odd_lay - 1.0), 2)
                })

        # 4. Lay 0x2 Zebra Visitante (Fav Mandante <= 1.70)
        if "Lay 0x2 Zebra Visitante (Fav Mandante <= 1.70)" in metodos_ativos:
            odd_lay = float(l0x2.iloc[idx])
            if h_odd <= 1.70 and 6.0 <= odd_lay <= 25.0:
                be = ((odd_lay - 1.0) / (odd_lay - (1.0 - fator_comissao))) * 100.0
                oportunidades.append({
                    'Data': dt, 'Hora': tm, 'Liga': lg, 'Home': h, 'Away': a,
                    'Jogo': f"{h} x {a}", 'Método': 'Lay 0x2 Zebra Visitante',
                    'Mercado': 'Correct Score (0x2)', 'Lado': 'LAY',
                    'Odd_Lay': odd_lay, 'Odd_Fav': h_odd, 'Break_Even_%': round(be, 1),
                    'Regra': f"Fav Mandante {h_odd:.2f} <= 1.70 | Odd Lay {odd_lay:.1f}",
                    'Risco_Nominal': 0.0,
                    'Stake_Teorica_R$': stake_teorica,
                    'Liability_Teorica_R$': round(stake_teorica * (odd_lay - 1.0), 2)
                })

        # 5. Lay 0x3 Zebra Visitante (Fav Mandante <= 1.60)
        if "Lay 0x3 Zebra Visitante (Fav Mandante <= 1.60)" in metodos_ativos:
            odd_lay = float(l0x3.iloc[idx])
            if h_odd <= 1.60 and 15.0 <= odd_lay <= 50.0:
                be = ((odd_lay - 1.0) / (odd_lay - (1.0 - fator_comissao))) * 100.0
                oportunidades.append({
                    'Data': dt, 'Hora': tm, 'Liga': lg, 'Home': h, 'Away': a,
                    'Jogo': f"{h} x {a}", 'Método': 'Lay 0x3 Zebra Visitante',
                    'Mercado': 'Correct Score (0x3)', 'Lado': 'LAY',
                    'Odd_Lay': odd_lay, 'Odd_Fav': h_odd, 'Break_Even_%': round(be, 1),
                    'Regra': f"Super Mandante {h_odd:.2f} <= 1.60 | Odd Lay {odd_lay:.1f}",
                    'Risco_Nominal': 0.0,
                    'Stake_Teorica_R$': stake_teorica,
                    'Liability_Teorica_R$': round(stake_teorica * (odd_lay - 1.0), 2)
                })

    return pd.DataFrame(oportunidades)

# ── Abas Principais de Navegação ──
tab_radar, tab_historico, tab_ledger, tab_raiox = st.tabs([
    "📡 Radar do Dia (Live & Agenda)",
    "📊 Histórico 2026 (Base FRESH3)",
    "📝 Diário de Bordo (Setembro & Outubro)",
    "🧬 Raio-X & Regras de Aprovação"
])

# ==============================================================================
# TAB 1: RADAR DO DIA (LIVE & AGENDA)
# ==============================================================================
with tab_radar:
    st.subheader("📡 Escaneador de Oportunidades Diárias (Betfair Cloud)")
    st.markdown("""
    Selecione a data para escanear jogos diretamente na **API Cloud da Betfair**. O motor filtra instantaneamente 
    os confrontos que se encaixam nos **sweet spots** dos 5 métodos em observação.
    """)
    
    col_d1, col_d2, col_d3 = st.columns([2, 3, 2])
    with col_d1:
        data_sel = st.date_input("Selecione a Data", value=date.today(), key="data_radar_obs")
    with col_d2:
        st.write("")
        st.write("")
        btn_escanear = st.button("🔄 Escanear Oportunidades do Dia", type="primary", use_container_width=True)
    with col_d3:
        st.write("")
        st.write("")
        btn_hoje = st.button("Hoje (Tempo Real)", use_container_width=True)
        if btn_hoje:
            data_sel = date.today()
            btn_escanear = True

    ds_iso = data_sel.strftime("%Y-%m-%d")

    if btn_escanear or "df_oportunidades_obs" in st.session_state:
        if btn_escanear:
            with st.spinner(f"Consultando jogos e odds de Lay da Betfair para {ds_iso}..."):
                df_api = None
                # 1. Tentar coletor / feed parquet local se existir
                f_parquet = ROOT / "scratch" / "feed_arquivo" / f"feed_forward_diario_{ds_iso}.parquet"
                if f_parquet.exists():
                    try:
                        df_api = pd.read_parquet(f_parquet)
                    except Exception:
                        df_api = None
                
                # 2. Tentar API direta Betfair
                if df_api is None or df_api.empty:
                    if get_daily_dataframe is not None:
                        try:
                            df_api = get_daily_dataframe(source="betfair", date_str=ds_iso)
                        except Exception:
                            df_api = None
                
                if df_api is not None and not df_api.empty:
                    df_ops = escanear_jogos(df_api, metodos_selecionados)
                    st.session_state["df_oportunidades_obs"] = df_ops
                    st.session_state["data_escan_obs"] = ds_iso
                else:
                    st.session_state["df_oportunidades_obs"] = pd.DataFrame()
                    st.session_state["data_escan_obs"] = ds_iso

        df_ops = st.session_state.get("df_oportunidades_obs", pd.DataFrame())
        data_exec = st.session_state.get("data_escan_obs", ds_iso)

        if not df_ops.empty:
            st.success(f"🎯 **{len(df_ops)} oportunidades qualificadas** encontradas para **{data_exec}**!")
            
            # Resumo em métricas
            m_c1, m_c2, m_c3, m_c4 = st.columns(4)
            with m_c1:
                st.metric("Total de Sinais", len(df_ops))
            with m_c2:
                st.metric("Risco Real (Stake Zero)", "R$ 0,00", delta="Capital Protegido")
            with m_c3:
                tot_liab_teorica = df_ops["Liability_Teorica_R$"].sum()
                st.metric("Liability Teórica Simulada", f"R$ {tot_liab_teorica:,.2f}")
            with m_c4:
                odd_med = df_ops["Odd_Lay"].median()
                st.metric("Odd Lay Mediana", f"{odd_med:.2f}")

            # Tabela de Sinais
            cols_show = [
                'Hora', 'Liga', 'Jogo', 'Método', 'Odd_Lay', 'Odd_Fav',
                'Break_Even_%', 'Regra', 'Stake_Teorica_R$', 'Liability_Teorica_R$'
            ]
            st.dataframe(
                df_ops[cols_show],
                use_container_width=True,
                hide_index=True
            )

            # Botões de Ação
            b_c1, b_c2 = st.columns([1, 1])
            with b_c1:
                # Download em Excel
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_ops.to_excel(writer, index=False, sheet_name="Sinais_Observacao")
                st.download_button(
                    label="📥 Baixar Grade em Excel (.xlsx)",
                    data=buffer.getvalue(),
                    file_name=f"ARKAD_Observacao_Set_Out_{data_exec}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with b_c2:
                if st.button("💾 Adicionar Sinais ao Diário de Bordo Set/Out", use_container_width=True):
                    registros_para_salvar = []
                    for _, row_op in df_ops.iterrows():
                        registros_para_salvar.append({
                            'Data': row_op['Data'],
                            'Hora': row_op['Hora'],
                            'Metodo': row_op['Método'],
                            'Liga': row_op['Liga'],
                            'Home': row_op['Home'],
                            'Away': row_op['Away'],
                            'Odd_Lay': row_op['Odd_Lay'],
                            'Odd_Fav': row_op['Odd_Fav'],
                            'Placar': '',
                            'Resultado': '',
                            'PnL_u': 0.0,
                            'Status': 'PENDENTE'
                        })
                    n_tot = salvar_no_ledger(registros_para_salvar)
                    st.success(f"✅ Sinais integrados com sucesso ao Diário de Bordo! Total acumulado: {n_tot} apostas.")
        else:
            st.info(f"Nenhum sinal qualificado para a data {data_exec} com os métodos selecionados. O motor respeita estritamente os tetos de odds e filtros de favoritismo.")

# ==============================================================================
# TAB 2: HISTÓRICO CONSOLIDADO 2026 (BASE FRESH3)
# ==============================================================================
with tab_historico:
    st.subheader("📊 Performance Auditada dos 5 Métodos em 2026 (Base FRESH3)")
    st.markdown("""
    Explore os dados históricos de 2026 (Janeiro a Agosto) com as **odds reais de Lay da Betfair**, 
    desconto da comissão e resultado real apurado jogo a jogo:
    """)

    df_hist_raw = carregar_historico_fresh3()

    if df_hist_raw.empty:
        st.error("Não foi possível carregar a base histórica FRESH3.")
    else:
        df_26 = df_hist_raw[df_hist_raw['Year'] == 2026].copy()
        
        # Filtros de Histórico
        h_col1, h_col2 = st.columns([3, 2])
        with h_col1:
            sel_metodo_hist = st.selectbox(
                "Filtrar Método para Inspeção Detalhada",
                options=["Todos Combinados"] + metodos_disponiveis
            )
        with h_col2:
            meses_nomes = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago"}
            sel_meses = st.multiselect(
                "Filtrar Meses de 2026",
                options=list(meses_nomes.keys()),
                format_func=lambda m: meses_nomes.get(m, str(m)),
                default=list(meses_nomes.keys())
            )

        # Processar cada método
        def extrair_metodo(df_in, nome_m):
            registros = []
            if nome_m == "Lay 1x1 c/ Fav Forte (Fav <= 1.50)":
                sub = df_in[(df_in['fav'] <= 1.50) & (df_in['Odd_CS_1x1_Lay'] >= 6.0) & (df_in['Odd_CS_1x1_Lay'] <= 12.0)].copy()
                sub['is_red'] = (sub['gh'] == 1) & (sub['ga'] == 1)
                sub['odd_lay'] = sub['Odd_CS_1x1_Lay']
                sub['metodo'] = nome_m
                return sub
            elif nome_m == "Lay 0x0 Super Fav (Fav <= 1.40)":
                sub = df_in[(df_in['fav'] <= 1.40) & (df_in['Odd_CS_0x0_Lay'] >= 7.0) & (df_in['Odd_CS_0x0_Lay'] <= 18.0)].copy()
                sub['is_red'] = (sub['gh'] == 0) & (sub['ga'] == 0)
                sub['odd_lay'] = sub['Odd_CS_0x0_Lay']
                sub['metodo'] = nome_m
                return sub
            elif nome_m == "Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70)":
                sub = df_in[(df_in['oa'] <= 1.70) & (df_in['Odd_CS_2x0_Lay'] >= 6.0) & (df_in['Odd_CS_2x0_Lay'] <= 25.0)].copy()
                sub['is_red'] = (sub['gh'] == 2) & (sub['ga'] == 0)
                sub['odd_lay'] = sub['Odd_CS_2x0_Lay']
                sub['metodo'] = nome_m
                return sub
            elif nome_m == "Lay 0x2 Zebra Visitante (Fav Mandante <= 1.70)":
                sub = df_in[(df_in['oh'] <= 1.70) & (df_in['Odd_CS_0x2_Lay'] >= 6.0) & (df_in['Odd_CS_0x2_Lay'] <= 25.0)].copy()
                sub['is_red'] = (sub['gh'] == 0) & (sub['ga'] == 2)
                sub['odd_lay'] = sub['Odd_CS_0x2_Lay']
                sub['metodo'] = nome_m
                return sub
            elif nome_m == "Lay 0x3 Zebra Visitante (Fav Mandante <= 1.60)":
                sub = df_in[(df_in['oh'] <= 1.60) & (df_in['Odd_CS_0x3_Lay'] >= 15.0) & (df_in['Odd_CS_0x3_Lay'] <= 50.0)].copy()
                sub['is_red'] = (sub['gh'] == 0) & (sub['ga'] == 3)
                sub['odd_lay'] = sub['Odd_CS_0x3_Lay']
                sub['metodo'] = nome_m
                return sub
            return pd.DataFrame()

        df_filtrado_mes = df_26[df_26['Month'].isin(sel_meses)]

        if sel_metodo_hist == "Todos Combinados":
            dfs_m = [extrair_metodo(df_filtrado_mes, m) for m in metodos_disponiveis]
            df_hist_final = pd.concat(dfs_m, ignore_index=True)
        else:
            df_hist_final = extrair_metodo(df_filtrado_mes, sel_metodo_hist)

        if df_hist_final.empty:
            st.warning("Nenhum jogo encontrado com os filtros selecionados.")
        else:
            df_hist_final = df_hist_final.sort_values('Date').reset_index(drop=True)
            n_tot = len(df_hist_final)
            reds_tot = df_hist_final['is_red'].sum()
            greens_tot = n_tot - reds_tot
            wr_real = (greens_tot / n_tot) * 100.0
            
            odds_v = df_hist_final['odd_lay'].values
            be_v = ((odds_v - 1.0) / (odds_v - (1.0 - fator_comissao))).mean() * 100.0
            gap_pp = wr_real - be_v
            
            # PnL por liability (1u de liability arriscada)
            pnl_arr = np.where(df_hist_final['is_red'], -1.0, fator_comissao / (odds_v - 1.0))
            roi_real = pnl_arr.mean() * 100.0
            pnl_tot_u = pnl_arr.sum()
            pnl_rs_sim = pnl_tot_u * stake_teorica

            # Métricas em Cards
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1:
                st.metric("Total de Jogos (N)", f"{n_tot:,}", f"{greens_tot}G / {reds_tot}R")
            with k2:
                st.metric("Taxa de Acerto (WR)", f"{wr_real:.1f}%", f"{gap_pp:+.1f}pp vs BE")
            with k3:
                st.metric("Break-Even Exigido", f"{be_v:.1f}%")
            with k4:
                st.metric("Lucro Líquido (u)", f"{pnl_tot_u:+.2f}u", f"ROI: {roi_real:+.2f}%")
            with k5:
                st.metric(f"Lucro Papel (R$ {stake_teorica:.0f}/u)", f"R$ {pnl_rs_sim:+,.2f}")

            # Gráfico de Equity Curve
            df_hist_final['cum_pnl'] = np.cumsum(pnl_arr)
            st.markdown("#### 📈 Curva de Patrimônio Histórica (P&L Acumulado em Unidades)")
            if HAS_PLOTLY:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_hist_final['Date'],
                    y=df_hist_final['cum_pnl'],
                    mode='lines',
                    name='P&L Acumulado (u)',
                    line=dict(color='#00e676', width=2.5),
                    fill='tozeroy',
                    fillcolor='rgba(0, 230, 118, 0.08)'
                ))
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="#111723",
                    plot_bgcolor="#111723",
                    margin=dict(l=20, r=20, t=30, b=20),
                    xaxis=dict(title="Data", showgrid=True, gridcolor="#1e2638"),
                    yaxis=dict(title="Lucro Acumulado (Unidades)", showgrid=True, gridcolor="#1e2638"),
                    hovermode="x unified"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(df_hist_final.set_index('Date')['cum_pnl'])

            # Tabela de Jogos
            with st.expander("🔍 Inspecionar Jogos Jogo a Jogo", expanded=False):
                df_show = df_hist_final.copy()
                df_show['Placar'] = df_show['gh'].astype(int).astype(str) + " x " + df_show['ga'].astype(int).astype(str)
                df_show['Resultado'] = np.where(df_show['is_red'], "RED ❌", "GREEN 🟢")
                df_show['PnL_u'] = np.round(pnl_arr, 3)
                df_show['Data_Str'] = df_show['Date'].dt.strftime('%Y-%m-%d')
                
                cols_view = ['Data_Str', 'League', 'Home', 'Away', 'Placar', 'metodo', 'odd_lay', 'fav', 'Resultado', 'PnL_u']
                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)

# ==============================================================================
# TAB 3: DIÁRIO DE BORDO (SETEMBRO & OUTUBRO 2026)
# ==============================================================================
with tab_ledger:
    st.subheader("📝 Diário de Bordo de Observação Prospectiva (Setembro & Outubro)")
    st.markdown("""
    Acompanhamento forward oficial desacoplado. Todos os sinais registrados aqui são gerados **pré-jogo** 
    e liquidados exclusivamente pelo placar final.
    """)

    df_led = carregar_ledger_observacao()

    if df_led.empty:
        st.info("O Diário de Bordo ainda não possui registros salvos. Você pode escanear os jogos do dia na Aba 1 e clicar em 'Salvar no Diário de Bordo'.")
    else:
        # Métricas do Ledger
        n_led = len(df_led)
        liq = df_led[df_led['Status'] == 'LIQUIDADO']
        n_liq = len(liq)
        n_pend = n_led - n_liq

        l_c1, l_c2, l_c3, l_c4 = st.columns(4)
        with l_c1:
            progresso = min(100.0, (n_led / 400.0) * 100.0)
            st.metric("Amostra Observada", f"{n_led} / 400 apostas", f"{progresso:.1f}% da meta")
        with l_c2:
            st.metric("Status Operacional", f"{n_liq} Liquidados", f"{n_pend} Pendentes")
        with l_c3:
            if n_liq > 0:
                reds_led = (liq['Resultado'] == 'RED').sum()
                wr_led = ((n_liq - reds_led) / n_liq) * 100.0
                st.metric("Win Rate Real (Set/Out)", f"{wr_led:.1f}%", f"{reds_led} Reds sofridos")
            else:
                st.metric("Win Rate Real", "Aguardando jogos")
        with l_c4:
            if n_liq > 0:
                pnl_led_u = liq['PnL_u'].sum()
                st.metric("P&L Acumulado (u)", f"{pnl_led_u:+.2f}u", f"R$ {pnl_led_u * stake_teorica:+,.2f}")
            else:
                st.metric("P&L Acumulado (u)", "0.00u")

        # Barra de Progresso
        st.progress(min(1.0, n_led / 400.0), text=f"Meta para Desbloqueio: 400 apostas auditadas (Faltam {max(0, 400 - n_led)} jogos)")

        # Tabela do Diário
        st.markdown("#### 📋 Registros do Diário de Bordo")
        st.dataframe(df_led, use_container_width=True, hide_index=True)

        # Download do Diário
        buf_led = io.BytesIO()
        with pd.ExcelWriter(buf_led, engine='openpyxl') as writer:
            df_led.to_excel(writer, index=False, sheet_name="Ledger_Observacao")
        st.download_button(
            label="📥 Exportar Diário de Bordo (.xlsx)",
            data=buf_led.getvalue(),
            file_name=f"ARKAD_Ledger_Observacao_Set_Out_{date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# ==============================================================================
# TAB 4: RAIO-X METODOLÓGICO & CRITÉRIOS DE APROVAÇÃO
# ==============================================================================
with tab_raiox:
    st.subheader("🧬 Raio-X dos Métodos em Quarentena e Critérios Científicos")
    st.markdown("""
    Cada um dos 5 métodos abaixo possui uma tese quantitativa fundamentada. Abaixo detalhamos **por que 
    reagiram em 2026**, **por que falharam no passado** e **o que precisam demonstrar para serem aprovados**:
    """)

    col_rx1, col_rx2 = st.columns(2)

    with col_rx1:
        st.markdown("""
        <div class="card-observacao">
            <h4>1. Lay 1x1 c/ Favorito Forte (Fav <= 1.50 | Odd Lay 6.0 a 12.0)</h4>
            <ul>
                <li><b>Tese Quantitativa:</b> Em jogos com favoritos pesados, o placar mais comum da Premier League/Ligas globais (1x1) cai drasticamente de 12,2% para <b>8,7%</b>, enquanto a odd de lay no 1-1 não explode tanto (~9.0 mediana).</li>
                <li><b>Desempenho 2026:</b> N=824 | <b>WR 91,3% vs BE 89,3% (+2,0 pp)</b> | <b>ROI +2,28% (+18,8u)</b>. Nos últimos 2 meses: <b>+1,64%</b>.</li>
                <li><b>O Risco Oculto (2024/2025):</b> Em 2024 (−1,69%) e 2025 (−1,98%), a taxa de 1-1 foi ligeiramente maior e comeu o edge pela comissão da Betfair.</li>
                <li><b>Critério de Aprovação:</b> Sustentar WR acima de 90,0% com ROI > +1,5% em Setembro e Outubro.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="card-observacao">
            <h4>2. Lay 0x0 Super Favorito (Fav <= 1.40 | Odd Lay 7.0 a 18.0)</h4>
            <ul>
                <li><b>Tese Quantitativa:</b> Super favoritos em desvantagem técnica massacrante raramente terminam em 0x0 limpo (taxa cai para menos de 3%).</li>
                <li><b>Desempenho 2026:</b> N=111 | <b>WR 97,3% vs BE 93,8% (+3,5 pp)</b> | <b>ROI +3,75% (+4,2u)</b>. Nos últimos 2 meses: <b>+6,04%</b>.</li>
                <li><b>Consistência Multiano:</b> 2024 (+1,30%), 2025 (−1,06%), 2026 (+3,75%). Agregado de 3 anos: <b>+3,84u (+0,97% ROI)</b>.</li>
                <li><b>Critério de Aprovação:</b> Já integra a Tríade do ARKAD. Manter zero reds agrupados e acumular N >= 200.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col_rx2:
        st.markdown("""
        <div class="card-observacao">
            <h4>3. Lay 2x0 Zebra Mandante (Fav Visitante <= 1.70 | Odd Lay 6.0 a 25.0)</h4>
            <ul>
                <li><b>Tese Quantitativa:</b> Zebras mandantes dificilmente vencem um visitante super favorito por 2x0 limpo sem levar gol.</li>
                <li><b>Desempenho 2026:</b> N=380 | <b>WR 98,7% vs BE 91,7% (+7,0 pp)</b> | <b>ROI +7,63% (+29,0u)</b>. Apenas 5 reds no ano inteiro!</li>
                <li><b>Perfil de Risco:</b> Micro-liability com retorno assimétrico positivo.</li>
                <li><b>Critério de Aprovação:</b> Manter taxa de reds abaixo de 1,5% em 100 novas operações.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="card-observacao">
            <h4>4 & 5. Lay 0x2 e 0x3 Zebra Visitante (Fav Mandante <= 1.60/1.70)</h4>
            <ul>
                <li><b>Tese Quantitativa:</b> Zebras visitantes sob pressão total de um mandante forte quase nunca goleiam por 0x2 ou 0x3 fora de casa.</li>
                <li><b>Desempenho 2026:</b> 
                    <br>• Lay 0x2: N=632 | <b>WR 98,3% vs BE 95,3% (+3,0 pp)</b> | <b>ROI +3,31% (+20,9u)</b>.
                    <br>• Lay 0x3: N=209 | <b>WR 99,5% vs BE 96,1% (+3,4 pp)</b> | <b>ROI +4,55% (+9,5u)</b>.
                </li>
                <li><b>Critério de Aprovação:</b> Seguir na cesta de micro-liability.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    ### ⚠️ Por que NÃO Usamos Filtros Estáticos por Liga? (Alerta contra o Garden of Forking Paths)
    Um dos achados mais importantes da nossa auditoria quantitativa é que **filtrar ligas retrospectivamente é uma miragem matemática**:
    * **Exemplo Real no Lay Under 2.5:** A liga *Spain 2* deu **+9,25%** em 2026, mas deu **−5,15%** em 2024. A liga *France 3* deu **+10,49%** em 2026, mas deu **−19,78%** em 2024!
    * **Exemplo Real no Lay 1x1:** A liga *Mexico 1* deu **+10,50%** em 2026 (97,7% WR), mas deu **−6,39%** em 2025 e **−2,71%** em 2024!
    
    > **Conclusão Científica:** Nenhuma liga é permanentemente "Under" ou "1x1". A longo prazo, todas convergem para a precificação de mercado. O único filtro robusto e resistente ao tempo é o **Favoritismo Estrutural (Odd <= 1.40 / 1.50)** somado à **Janela Estrita de Odds de Lay**.
    """)
