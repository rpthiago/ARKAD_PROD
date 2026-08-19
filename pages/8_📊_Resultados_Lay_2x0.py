from __future__ import annotations
import glob
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Resultados Lay 2x0", page_icon="📊", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = ROOT_DIR / "coleta_lay2x0_aovivo.xlsx"

def _carregar() -> pd.DataFrame:
    if not FILE_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(FILE_PATH)
        return df
    except Exception as e:
        st.warning(f"Erro ao ler planilha: {e}")
        return pd.DataFrame()

def _pnl_lay2x0_full_match(row: pd.Series) -> float:
    status = str(row.get("status", "")).strip().upper()
    if status != "ENCERRADO":
        return np.nan
    placar = str(row.get("Placar_final", "")).strip()
    odd_entrada = pd.to_numeric(row.get("Odd_lay_entrada"), errors="coerce")
    if pd.isna(odd_entrada) or odd_entrada <= 1:
        return np.nan
        
    # Somente aqueles que passarem no filtro de odd <= 12
    if odd_entrada > 12.0:
        return 0.0 # Void / Filtrado
        
    if placar == "2-0":
        # Perde a responsabilidade (Odd - 1)
        return -round(odd_entrada - 1, 2)
    else:
        # Ganha a stake (1 unit) livre de comissão (5%)
        return round(1.0 * (1 - 0.05), 2)

st.title("📊 Resultados Lay 2x0 (Monitor de Lucros)")
st.markdown("Acompanhe o desempenho das suas planilhas de *paper trading* do Lay 2x0 com odd <= 12 e ligas filtradas.")

df = _carregar()
if df.empty:
    st.info(f"📂 Nenhuma planilha `coleta_lay2x0_aovivo.xlsx` encontrada na raiz do projeto. Rode a coleta diária para gerá-la.")
    st.stop()

df["PnL"] = df.apply(_pnl_lay2x0_full_match, axis=1)

# Separar apostas reais das Voids
pendentes = df[df["PnL"].isna()].copy()
resolvidas = df[(df["PnL"].notna()) & (df["PnL"] != 0.0)].copy()
voids = df[(df["PnL"].notna()) & (df["PnL"] == 0.0)].copy()

greens = int((resolvidas["PnL"] > 0).sum())
reds   = int((resolvidas["PnL"] < 0).sum())
lucro  = float(resolvidas["PnL"].sum())
wr     = greens / (greens + reds) * 100 if (greens + reds) > 0 else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Entradas Resolvidas", len(resolvidas))
c2.metric("Win Rate", f"{wr:.1f}%")
c3.metric("Greens / Reds", f"{greens}G / {reds}R")
c4.metric("Descartados (Void/Filtro)", len(voids))
sinal = "+" if lucro > 0 else ""
c5.metric("P&L Acumulado (Stakes)", f"{sinal}{lucro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.divider()

# ── Gráfico curva de banca ────────────────────────────────────────────────────
df_ord = resolvidas.sort_values(["Date", "Horario"]).copy() if not resolvidas.empty else resolvidas.copy()
df_ord["_acum"] = df_ord["PnL"].cumsum()

fig = go.Figure()
if not df_ord.empty:
    fig.add_trace(go.Scatter(
        x=list(range(1, len(df_ord) + 1)),
        y=df_ord["_acum"],
        mode="lines+markers",
        line=dict(color="#27ae60" if lucro >= 0 else "#e74c3c", width=2),
        fill="tozeroy",
        hovertemplate="Aposta: %{x}<br>P&L Acumulado: %{y:.2f} Stakes<extra></extra>"
    ))
fig.add_hline(y=0, line_dash="dash", line_color="gray")
fig.update_layout(
    title="Evolução da Banca (Full Match)",
    xaxis_title="Nº Aposta Executada",
    yaxis_title="Retorno Acumulado (Stakes)",
    height=320,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Desempenho Mensal (Lucro por Mês) ─────────────────────────────────────────
st.subheader("📅 Desempenho Mensal (Lucro por Mês)")

if not df_ord.empty and "Date" in df_ord.columns:
    df_mes = df_ord.copy()
    df_mes["_data_dt"] = pd.to_datetime(df_mes["Date"], errors="coerce")
    df_mes = df_mes[df_mes["_data_dt"].notna()].copy()
    
    if not df_mes.empty:
        df_mes["AnoMes"] = df_mes["_data_dt"].dt.strftime("%Y-%m")
        
        meses_pt = {
            "01": "Janeiro", "02": "Fevereiro", "03": "Março",
            "04": "Abril", "05": "Maio", "06": "Junho",
            "07": "Julho", "08": "Agosto", "09": "Setembro",
            "10": "Outubro", "11": "Novembro", "12": "Dezembro"
        }
        
        monthly_stats = []
        for anomes, group in df_mes.groupby("AnoMes"):
            ano, mes_num = anomes.split("-")
            nome_mes = f"{meses_pt.get(mes_num, mes_num)}/{ano}"
            
            n_entradas = len(group)
            g_count = int((group["PnL"] > 0).sum())
            r_count = int((group["PnL"] < 0).sum())
            wr_month = (g_count / n_entradas * 100) if n_entradas > 0 else 0.0
            pnl_month = float(group["PnL"].sum())
            
            monthly_stats.append({
                "Mês / Ano": nome_mes,
                "Entradas": n_entradas,
                "Greens": g_count,
                "Reds": r_count,
                "Win Rate": f"{wr_month:.1f}%",
                "Lucro Líquido (Stakes)": f"{pnl_month:+.2f} U",
                "_pnl_raw": pnl_month,
                "_anomes": anomes
            })
            
        df_monthly_out = pd.DataFrame(monthly_stats)
        
        col_m1, col_m2 = st.columns([1, 2])
        
        with col_m1:
            st.markdown("#### 🏆 Métricas por Mês")
            for m in monthly_stats:
                badge = "🟢" if m["_pnl_raw"] >= 0 else "🔴"
                st.metric(
                    label=f"{badge} {m['Mês / Ano']}",
                    value=m["Lucro Líquido (Stakes)"],
                    delta=f"{m['Win Rate']} Win Rate ({m['Greens']}G / {m['Reds']}R)"
                )
                
        with col_m2:
            st.markdown("#### 📊 Tabela Consolidada Mensal")
            st.dataframe(
                df_monthly_out.drop(columns=["_pnl_raw", "_anomes"]),
                use_container_width=True,
                hide_index=True
            )
    else:
        st.info("Sem datas válidas para agrupamento mensal.")

st.divider()

# ── Tabela principal ──────────────────────────────────────────────────────────
st.subheader("📋 Histórico de Operações (Somente Entradas Confirmadas)")

cols_exibir = ["Date", "Horario", "Liga", "Mandante", "Visitante", "Metodo", "Odd_lay_entrada", "Placar_final", "Momento_gols", "PnL"]
tbl = df_ord[cols_exibir].copy() if not df_ord.empty else pd.DataFrame(columns=cols_exibir)
tbl.columns = ["Data", "Horário", "Liga", "Mandante", "Visitante", "Modelo", "Odd Lay", "Placar Final", "Minuto Gols", "Retorno (Stakes)"]

def _cor(row: pd.Series) -> list:
    v = row.get("Retorno (Stakes)", 0)
    if v > 0:
        return ["background-color: #d1fae5; color: #065f46"] * len(row)
    if v < 0:
        return ["background-color: #fee2e2; color: #991b1b"] * len(row)
    return ["color: #6b7280"] * len(row)

fmt = {"Retorno (Stakes)": "{:+,.2f} U", "Odd Lay": "{:.2f}"}

if not tbl.empty:
    st.dataframe(
        tbl.style.apply(_cor, axis=1).format(fmt, na_rep="-"),
        use_container_width=True,
        hide_index=True,
        height=420,
    )
else:
    st.info("Nenhuma entrada confirmada 2x0 até o momento.")
