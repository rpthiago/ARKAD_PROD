from __future__ import annotations
import glob
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Resultados Lay 0x0", page_icon="📊", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIR_APOSTAS = ROOT_DIR / "paper_traning_lay0x0"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _carregar() -> pd.DataFrame:
    DIR_APOSTAS.mkdir(parents=True, exist_ok=True)
    arquivos = sorted(glob.glob(str(DIR_APOSTAS / "*.xlsx")))
    if not arquivos:
        return pd.DataFrame()
    frames = []
    for arq in arquivos:
        try:
            df = pd.read_excel(arq)
            df["__arquivo"] = Path(arq).name
            frames.append(df)
        except Exception as e:
            st.warning(f"Erro ao ler {Path(arq).name}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _clean_monetary(val):
    if pd.isna(val):
        return np.nan
    s = str(val).replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".").strip()
    return pd.to_numeric(s, errors="coerce")

def _pnl_lay0x0(row: pd.Series) -> float:
    # 1. Se o usuário preencheu explicitamente "Lucro (R$)", usamos esse valor
    lucro_manual = _clean_monetary(row.get("Lucro (R$)"))
    if pd.notna(lucro_manual):
        return lucro_manual
        
    # 2. Se preencheu o "Resultado"
    resultado = str(row.get("Resultado", "")).strip().upper()
    
    # Identificar a odd
    odd_entrada = pd.to_numeric(row.get("Odd Lay Betfair"), errors="coerce")
    if pd.isna(odd_entrada) or odd_entrada <= 1:
        odd_entrada = pd.to_numeric(row.get("Odd_lay_entrada"), errors="coerce")
        
    if pd.isna(odd_entrada) or odd_entrada <= 1:
        return np.nan
        
    # Identificar a responsabilidade e stake da banca
    resp_banca = _clean_monetary(row.get("Responsabilidade (R$)"))
    stake_banca = _clean_monetary(row.get("Stake Back Betfair (R$)"))
    
    # Se resultado for explicitamente RED ou 0-0
    if resultado in ["RED", "R", "0-0", "0X0", "PERDA", "L"]:
        if pd.notna(resp_banca):
            return -resp_banca
        return -round(odd_entrada - 1, 2) # P&L em Unidades de Stake
        
    # Se resultado for explicitamente GREEN ou G
    if resultado in ["GREEN", "G", "VITORIA", "GANHOU", "W"]:
        if pd.notna(stake_banca):
            return round(stake_banca * (1 - 0.05), 2)
        return round(1.0 * (1 - 0.05), 2) # P&L em Unidades de Stake
        
    # 3. Fallback automático pelo placar real (caso preenchido pela API)
    placar = str(row.get("Placar_final", "")).strip()
    if placar == "0-0":
        if pd.notna(resp_banca):
            return -resp_banca
        return -round(odd_entrada - 1, 2)
    elif placar != "":
        if pd.notna(stake_banca):
            return round(stake_banca * (1 - 0.05), 2)
        return round(1.0 * (1 - 0.05), 2)
        
    return np.nan

# ── App ───────────────────────────────────────────────────────────────────────

st.title("📊 Resultados Lay 0x0 (Monitor de Lucros)")
st.markdown(f"Acompanhe o desempenho das suas planilhas de *paper trading* do Lay 0x0. Salve os arquivos de sinais com seus resultados anotados na pasta `{DIR_APOSTAS}` para atualizar a curva de banca.")

df = _carregar()
if df.empty:
    st.info(f"📂 Nenhuma planilha `.xlsx` de sinais encontrada na pasta `{DIR_APOSTAS}`. Baixe os sinais da Página 9, preencha os resultados e salve-os lá.")
    st.stop()

# Aplica a função de PnL
df["PnL"] = df.apply(_pnl_lay0x0, axis=1)

# Separar apostas reais das Voids/Pendentes
pendentes = df[df["PnL"].isna()].copy()
resolvidas = df[df["PnL"].notna()].copy()

greens = int((resolvidas["PnL"] > 0).sum())
reds   = int((resolvidas["PnL"] < 0).sum())
lucro  = float(resolvidas["PnL"].sum())
wr     = greens / (greens + reds) * 100 if (greens + reds) > 0 else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Entradas Resolvidas", len(resolvidas))
c2.metric("Win Rate", f"{wr:.1f}%")
c3.metric("Greens / Reds", f"{greens}G / {reds}R")
sinal = "+" if lucro > 0 else ""

# Verifica se a moeda é em R$ ou Unidades de Stake
exemplo_row = resolvidas.iloc[0] if not resolvidas.empty else None
is_real = exemplo_row is not None and (pd.notna(_clean_monetary(exemplo_row.get("Lucro (R$)"))) or pd.notna(_clean_monetary(exemplo_row.get("Responsabilidade (R$)"))))
unidade_label = "R$" if is_real else "Stakes"

if is_real:
    c4.metric("P&L Acumulado", f"{sinal}R$ {lucro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
else:
    c4.metric("P&L Acumulado", f"{sinal}{lucro:,.2f} U")

st.divider()

# ── Gráfico curva de banca ────────────────────────────────────────────────────
df_ord = resolvidas.copy()
# Tenta ordenar por data e horário
data_col = "Data" if "Data" in df_ord.columns else "Date"
hora_col = "Horário" if "Horário" in df_ord.columns else "Horario"

if data_col in df_ord.columns:
    df_ord[data_col] = pd.to_datetime(df_ord[data_col], errors="coerce")
    if hora_col in df_ord.columns:
        df_ord = df_ord.sort_values([data_col, hora_col]).copy()
    else:
        df_ord = df_ord.sort_values(data_col).copy()

df_ord["_acum"] = df_ord["PnL"].cumsum()

fig = go.Figure()
if not df_ord.empty:
    fig.add_trace(go.Scatter(
        x=list(range(1, len(df_ord) + 1)),
        y=df_ord["_acum"],
        mode="lines+markers",
        line=dict(color="#27ae60" if lucro >= 0 else "#e74c3c", width=2),
        fill="tozeroy",
        hovertemplate="Aposta: %{x}<br>P&L Acumulado: %{y:.2f} " + unidade_label + "<extra></extra>"
    ))
fig.add_hline(y=0, line_dash="dash", line_color="gray")
fig.update_layout(
    title=f"Evolução da Banca ({unidade_label})",
    xaxis_title="Nº Aposta Executada",
    yaxis_title=f"Retorno Acumulado ({unidade_label})",
    height=320,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Análise Estatística Detalhada ─────────────────────────────────────────────
st.subheader("📊 Estatísticas e Análise de Desempenho")

col_stats1, col_stats2, col_stats3 = st.columns(3)

with col_stats1:
    st.markdown("#### 🏆 Melhores e Piores Ligas")
    # Agrupa por liga e calcula PnL acumulado
    if not df_ord.empty and "Liga" in df_ord.columns:
        liga_stats = df_ord.groupby("Liga")["PnL"].sum().reset_index()
        liga_stats = liga_stats.sort_values(by="PnL", ascending=False)
        
        # Formata para exibição
        liga_stats_display = liga_stats.copy()
        if is_real:
            liga_stats_display["PnL Acumulado"] = liga_stats_display["PnL"].map(lambda x: f"R$ {x:.2f}")
        else:
            liga_stats_display["PnL Acumulado"] = liga_stats_display["PnL"].map(lambda x: f"{x:.2f} U")
            
        # Top Melhores
        st.markdown("**Top 3 Ligas Mais Lucrativas:**")
        st.dataframe(liga_stats_display.head(3)[["Liga", "PnL Acumulado"]], use_container_width=True, hide_index=True)
        
        # Top Piores
        st.markdown("**Top 3 Ligas Menos Lucrativas (Piores):**")
        st.dataframe(liga_stats_display.tail(3)[["Liga", "PnL Acumulado"]], use_container_width=True, hide_index=True)
    else:
        st.info("Dados de ligas indisponíveis.")

with col_stats2:
    st.markdown("#### 🎲 Métricas de Odds")
    if not df_ord.empty:
        # Identificar a odd
        odd_col = "Odd Lay Betfair" if "Odd Lay Betfair" in df_ord.columns else "Odd_lay_entrada"
        if odd_col in df_ord.columns:
            df_ord["Odd_Float"] = pd.to_numeric(df_ord[odd_col], errors="coerce")
            
            # Filtra odds válidas
            valid_odds = df_ord[df_ord["Odd_Float"].notna() & (df_ord["Odd_Float"] > 1)]
            
            if not valid_odds.empty:
                odd_media = valid_odds["Odd_Float"].mean()
                odd_max = valid_odds["Odd_Float"].max()
                odd_min = valid_odds["Odd_Float"].min()
                
                # Odds dos Reds
                reds_df = valid_odds[valid_odds["PnL"] < 0]
                odd_media_reds = reds_df["Odd_Float"].mean() if not reds_df.empty else np.nan
                
                st.markdown(f"""
                *   **Odd Média das Entradas:** `{odd_media:.2f}`
                *   **Odd Mínima Registrada:** `{odd_min:.2f}`
                *   **Odd Máxima Registrada:** `{odd_max:.2f}`
                *   **Odd Média dos Reds (0x0):** `{f"{odd_media_reds:.2f}" if pd.notna(odd_media_reds) else "N/A"}`
                """)
            else:
                st.info("Sem odds válidas registradas.")
        else:
            st.info("Coluna de odds não encontrada.")
    else:
        st.info("Sem dados de odds.")

with col_stats3:
    st.markdown("#### 📈 Sequências e Médias")
    if not df_ord.empty:
        # P&L Médio por Entrada
        pnl_medio = df_ord["PnL"].mean()
        
        # Calcular sequências máximas de Greens (PnL > 0)
        pnl_values = df_ord["PnL"].values
        max_greens = 0
        current_greens = 0
        max_reds = 0
        current_reds = 0
        
        for p in pnl_values:
            if p > 0:
                current_greens += 1
                current_reds = 0
                if current_greens > max_greens:
                    max_greens = current_greens
            elif p < 0:
                current_reds += 1
                current_greens = 0
                if current_reds > max_reds:
                    max_reds = current_reds
            else:
                current_greens = 0
                current_reds = 0
                
        pnl_medio_label = f"R$ {pnl_medio:.2f}" if is_real else f"{pnl_medio:.2f} U"
        
        st.markdown(f"""
        *   **Retorno Médio por Entrada:** `{pnl_medio_label}`
        *   **Maior Sequência de Greens:** `{max_greens}`
        *   **Maior Sequência de Reds (0x0):** `{max_reds}`
        *   **Resultado do Último Jogo:** `{"🟢 GREEN" if pnl_values[-1] > 0 else "🔴 RED"}` (Lucro: {f"R$ {pnl_values[-1]:.2f}" if is_real else f"{pnl_values[-1]:.2f} U"})
        """)
    else:
        st.info("Sem dados de sequências.")

st.divider()

# ── Tabela principal ──────────────────────────────────────────────────────────
st.subheader("📋 Histórico de Operações (Confirmadas)")

cols_exibir = []
for c in ["Data", "Horário", "Liga", "Mandante", "Visitante", "Odd Lay Betfair", "Responsabilidade (R$)", "Stake Back Betfair (R$)", "Resultado", "Lucro (R$)", "PnL"]:
    if c in df_ord.columns:
        cols_exibir.append(c)

tbl = df_ord[cols_exibir].copy() if not df_ord.empty else pd.DataFrame(columns=cols_exibir)

def _cor(row: pd.Series) -> list:
    v = row.get("PnL", 0)
    if v > 0:
        return ["background-color: #d1fae5; color: #065f46"] * len(row)
    if v < 0:
        return ["background-color: #fee2e2; color: #991b1b"] * len(row)
    return ["color: #6b7280"] * len(row)

if not tbl.empty:
    st.dataframe(
        tbl.style.apply(_cor, axis=1),
        use_container_width=True,
        hide_index=True,
        height=420,
    )
else:
    st.info("Nenhuma entrada confirmada do Lay 0x0 até o momento.")
