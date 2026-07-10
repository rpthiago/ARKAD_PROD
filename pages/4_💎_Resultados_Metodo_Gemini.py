"""
Resultados Método Gemini (Paper Trading) — ARKAD_PROD
Tabela consolidada das planilhas de sinais legados em metodo_gemini/*.xlsx
"""
from __future__ import annotations

import glob
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Resultados Gemini", page_icon="💎", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIR_APOSTAS = ROOT_DIR / "metodo_gemini"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _carregar() -> pd.DataFrame:
    # Cria o diretório se não existir
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


def _pnl_gemini(row: pd.Series) -> float:
    # PnL no formato do metodo Gemini (Stake = 1 Unidade fixa)
    res = str(row.get("Resultado", "")).strip().upper()
    
    if res in ("", "NAN", "PENDENTE", "-"):
        return np.nan
        
    # Tenta usar o Lucro que já veio do Excel (fórmula ou digitação manual)
    lucro = row.get("Lucro")
    if pd.notna(lucro) and str(lucro).strip() not in ("", "nan"):
        try:
            return float(lucro)
        except Exception:
            pass
            
    # Fallback: Recalcula na raça via Python assumindo Stake = 1
    try:
        odd = float(row.get("Odd", 0))
    except Exception:
        return np.nan
        
    if odd <= 1:
        return np.nan
        
    tipo = str(row.get("Tipo", "")).strip().upper()
    
    # Avaliando Green
    if any(k in res for k in ("1", "GREEN", "VITORIA")):
        if "BACK" in tipo:
            return round(odd - 1.0, 2)
        else: # Lay
            return 1.0
            
    # Avaliando Red
    if any(k in res for k in ("0", "RED", "DERROTA")):
        if "BACK" in tipo:
            return -1.0
        else: # Lay
            return round(-(odd - 1.0), 2)
            
    # Void
    if any(k in res for k in ("VOID", "DEVOLVIDA", "REEMBOLSO")):
        return 0.0
        
    return np.nan


def _to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Apostas")
    return buf.getvalue()


# ── App ───────────────────────────────────────────────────────────────────────

st.title("💎 Resultados - Método Gemini")
st.markdown("Consolidação automática de todas as planilhas de *Paper Trading* salvas na pasta `metodo_gemini/`.")

df = _carregar()
if df.empty:
    st.info(f"📂 Nenhuma planilha `.xlsx` encontrada na pasta `{DIR_APOSTAS}`.")
    st.stop()

# Garantir colunas essenciais se faltarem
for col in ["Data", "Hora", "Liga", "Home", "Away", "Metodo", "Tipo", "Odd", "Prob", "Resultado", "Lucro"]:
    if col not in df.columns:
        df[col] = ""

df["Resultado_Normalizado"] = df["Resultado"].fillna("").astype(str).str.strip().str.upper()
df["PnL"]  = df.apply(_pnl_gemini, axis=1)

resolvidas = df[df["PnL"].notna()].copy()
pendentes  = df[df["PnL"].isna()].copy()

# ── Métricas ──────────────────────────────────────────────────────────────────
greens = int((resolvidas["PnL"] > 0).sum())
reds   = int((resolvidas["PnL"] < 0).sum())
lucro  = float(resolvidas["PnL"].sum())
wr     = greens / (greens + reds) * 100 if (greens + reds) > 0 else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Apostas Resolvidas", len(resolvidas))
c2.metric("Win Rate", f"{wr:.1f}%")
c3.metric("Greens / Reds", f"{greens}G / {reds}R")
c4.metric("Pendentes", len(pendentes))
sinal = "+" if lucro > 0 else ""
c5.metric("Lucro Total (Unidades)", f"{sinal}{lucro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.divider()

# ── Gráfico curva de banca ────────────────────────────────────────────────────
df_ord = resolvidas.sort_values("Data") if not resolvidas.empty else resolvidas.copy()
df_ord["_acum"] = df_ord["PnL"].cumsum()

fig = go.Figure()
if not df_ord.empty:
    fig.add_trace(go.Scatter(
        x=list(range(1, len(df_ord) + 1)),
        y=df_ord["_acum"],
        mode="lines+markers",
        line=dict(color="#27ae60" if lucro >= 0 else "#e74c3c", width=2),
        fill="tozeroy",
        hovertemplate="Aposta: %{x}<br>Lucro Acumulado: %{y:.2f} U<extra></extra>"
    ))
fig.add_hline(y=0, line_dash="dash", line_color="gray")
fig.update_layout(
    title="Evolução do P&L Acumulado (Paper Trading)",
    xaxis_title="Nº Aposta Resolvida",
    yaxis_title="Lucro Acumulado (Unidades)",
    height=280,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Tabela principal ──────────────────────────────────────────────────────────
st.subheader("📋 Histórico Consolidado")

cols_exibir = ["Data", "Hora", "Liga", "Home", "Away", "Metodo", "Tipo", "Odd", "Prob", "Resultado", "PnL"]
tbl = resolvidas[cols_exibir].copy() if not resolvidas.empty else pd.DataFrame(columns=cols_exibir)

def _cor(row: pd.Series) -> list:
    r = str(row.get("Resultado", "")).upper()
    if any(k in r for k in ("1", "GREEN", "VITORIA")):
        return ["background-color: #d1fae5; color: #065f46"] * len(row)
    if any(k in r for k in ("0", "RED", "DERROTA")):
        return ["background-color: #fee2e2; color: #991b1b"] * len(row)
    return ["color: #6b7280"] * len(row)

fmt = {}
if "PnL" in tbl.columns:
    fmt["PnL"] = "{:+,.2f} U"
if "Odd" in tbl.columns:
    fmt["Odd"] = "{:.2f}"

if not tbl.empty:
    st.dataframe(
        tbl.style.apply(_cor, axis=1).format(fmt, na_rep="-"),
        use_container_width=True,
        hide_index=True,
        height=420,
    )
else:
    st.info("Preencha o Resultado (1=Green, 0=Red) nas suas planilhas para visualizar o histórico de resolvidas.")

# ── Pendentes (expander) ──────────────────────────────────────────────────────
if not pendentes.empty:
    with st.expander(f"⏳ Entradas Pendentes de Resultado ({len(pendentes)})"):
        cols_p = ["Data", "Hora", "Liga", "Home", "Away", "Metodo", "Tipo", "Odd", "Prob"]
        st.dataframe(pendentes[cols_p], use_container_width=True, hide_index=True)

# ── Seção de Estatísticas Avançadas ──────────────────────────────────────────
st.divider()
st.subheader("📈 Análise Estatística Completa")

tab1, tab2, tab3 = st.tabs(["📊 Performance Geral", "🏆 Ligas & Métodos", "📉 Curva & Tendência"])

with tab1:
    # O Investimento no método Gemini/Paper Trading de 1 Unidade fixa 
    # (No back, arriscamos 1. No Lay, arriscamos a Responsabilidade = Odd - 1)
    def_risco = lambda row: 1.0 if str(row.get("Tipo","")).strip().upper() == "BACK" else (float(row.get("Odd",2)) - 1)
    
    if not resolvidas.empty:
        risco_total = resolvidas.apply(def_risco, axis=1).sum()
        roi = (lucro / risco_total * 100) if risco_total > 0 else 0
        
        ganhos = resolvidas[resolvidas["PnL"] > 0]["PnL"].sum()
        perdas = abs(resolvidas[resolvidas["PnL"] < 0]["PnL"].sum())
        pf = ganhos / perdas if perdas > 0 else float('inf')
        
        mdd = (df_ord["_acum"] - df_ord["_acum"].cummax()).min()
        odd_med = resolvidas["Odd"].mean()
    else:
        roi = pf = mdd = odd_med = 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("ROI (Sobre Risco)", f"{roi:.2f}%")
    m2.metric("Profit Factor", f"{pf:.2f}" if pf != float('inf') else "∞")
    m3.metric("Max Drawdown", f"{mdd:,.2f} U".replace(",", "."))
    m4.metric("Odd Média", f"{odd_med:.2f}")

with tab2:
    c_liga, c_met = st.columns(2)
    
    with c_liga:
        st.markdown("##### 🌍 Top Ligas (P&L)")
        if not resolvidas.empty:
            liga_stats = resolvidas.groupby("Liga").agg(
                Apostas=("PnL", "count"),
                Greens=("PnL", lambda x: (x > 0).sum()),
                PnL=("PnL", "sum")
            ).reset_index()
            liga_stats["WR%"] = (liga_stats["Greens"] / liga_stats["Apostas"] * 100)
            
            melhores = liga_stats.sort_values("PnL", ascending=False).head(5)
            piores = liga_stats.sort_values("PnL", ascending=True).head(5)
            
            st.dataframe(
                melhores[["Liga", "Apostas", "WR%", "PnL"]].style.format({"WR%": "{:.1f}%", "PnL": "{:+,.2f} U"}),
                use_container_width=True, hide_index=True
            )
            st.markdown("##### ⚠️ Ligas com Menor Performance")
            st.dataframe(
                piores[["Liga", "Apostas", "WR%", "PnL"]].style.format({"WR%": "{:.1f}%", "PnL": "{:+,.2f} U"}),
                use_container_width=True, hide_index=True
            )
        else:
            st.write("Sem dados suficientes.")

    with c_met:
        st.markdown("##### 🎯 Win Rate por Método")
        if not resolvidas.empty:
            met_stats = resolvidas.groupby("Metodo").agg(
                Apostas=("PnL", "count"),
                Greens=("PnL", lambda x: (x > 0).sum()),
                PnL=("PnL", "sum")
            ).reset_index()
            met_stats["WinRate"] = (met_stats["Greens"] / met_stats["Apostas"] * 100)
            
            fig_met = go.Figure(go.Bar(
                x=met_stats["Metodo"],
                y=met_stats["WinRate"],
                text=[f"{v:.1f}%" for v in met_stats["WinRate"]],
                textposition='auto',
                marker_color='#3498db'
            ))
            fig_met.update_layout(title="Win Rate % por Método", height=300, margin=dict(l=0,r=0,t=30,b=0))
            st.plotly_chart(fig_met, use_container_width=True)
            
            st.dataframe(
                met_stats[["Metodo", "Apostas", "WinRate", "PnL"]].style.format({"WinRate": "{:.1f}%", "PnL": "{:+,.2f} U"}),
                use_container_width=True, hide_index=True
            )
        else:
            st.write("Sem dados suficientes.")

with tab3:
    st.markdown("##### 📅 Performance por Dia da Semana")
    if not resolvidas.empty:
        resolvidas['DOW'] = pd.to_datetime(resolvidas["Data"]).dt.day_name()
        dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_stats = resolvidas.groupby('DOW')['PnL'].sum().reindex(dow_order).fillna(0).reset_index()
        
        fig_dow = go.Figure(go.Bar(
            x=dow_stats['DOW'],
            y=dow_stats['PnL'],
            marker_color=['#2ecc71' if v > 0 else '#e74c3c' for v in dow_stats['PnL']]
        ))
        fig_dow.update_layout(title="P&L por Dia da Semana", height=300)
        st.plotly_chart(fig_dow, use_container_width=True)
    else:
        st.write("Sem dados suficientes.")

st.divider()

# ── Download Excel ────────────────────────────────────────────────────────────
st.download_button(
    "📥 Baixar Excel Consolidado (Todas as planilhas num arquivo só)",
    data=_to_excel(df),
    file_name="gemini_paper_trading_consolidado.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
