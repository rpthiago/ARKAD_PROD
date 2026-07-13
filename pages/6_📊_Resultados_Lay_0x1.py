from __future__ import annotations
import glob
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Resultados Lay 0x1", page_icon="📊", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIR_APOSTAS = ROOT_DIR / "paper_trading_lay0x1"

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

def _pnl_lay0x1_full_match(row: pd.Series) -> float:
    status = str(row.get("status", "")).strip().upper()
    if status != "ENCERRADO":
        return np.nan
    placar = str(row.get("Placar_final", "")).strip()
    odd_entrada = pd.to_numeric(row.get("Odd_lay_entrada"), errors="coerce")
    if pd.isna(odd_entrada) or odd_entrada <= 1:
        return np.nan
    if placar == "0-1":
        # Perde a responsabilidade/responsabilidade total (Odd - 1)
        return -round(odd_entrada - 1, 2)
    else:
        # Ganha a stake (1 unit) livre de comissão (6.5%)
        return round(1.0 * (1 - 0.065), 2)

def _pnl_lay0x1_rota60(row: pd.Series) -> float:
    # 1. Se o jogo não está encerrado, NaN
    status = str(row.get("status", "")).strip().upper()
    if status != "ENCERRADO":
        return np.nan
        
    momento = str(row.get("Momento_gols", ""))
    placar = str(row.get("Placar_final", ""))
    odd_entrada = pd.to_numeric(row.get("Odd_lay_entrada"), errors="coerce")
    
    if pd.isna(odd_entrada) or odd_entrada <= 1:
        return np.nan
        
    # Verifica se bateu 0x1
    try:
        gols_timeline = []
        for parte in momento.split("|"):
            parte = parte.strip()
            if "Casa:" in parte:
                mins = parte.replace("Casa:", "").strip()
                if mins:
                    for m in mins.split(","): gols_timeline.append({"team": "H", "min": int(m)})
            if "Fora:" in parte:
                mins = parte.replace("Fora:", "").strip()
                if mins:
                    for m in mins.split(","): gols_timeline.append({"team": "A", "min": int(m)})
                    
        gols_timeline = sorted(gols_timeline, key=lambda x: x["min"])
        
        score_h, score_a = 0, 0
        
        for g in gols_timeline:
            if g["min"] > 60:
                break
            
            if g["team"] == "H": score_h += 1
            else: score_a += 1
            
            # Se o Home marcar, ou Away marcar 2 (impossibilita o 0x1) -> Green
            if score_h > 0 or score_a > 1:
                return round(1.0 * (1 - 0.065), 2)
                
        # Chegou aos 60 minutos sem Green (Placar 0x0 ou 0x1)
        odd_min60_manual = pd.to_numeric(row.get("PREENCHER_odd_min60"), errors="coerce")
        
        if pd.notna(odd_min60_manual) and odd_min60_manual > 1:
            odd_exit = odd_min60_manual
        else:
            if score_h == 0 and score_a == 0:
                # Placar 0-0 aos 60 mins -> Decay médio de 55%
                odd_exit = odd_entrada * 0.45 
            elif score_h == 0 and score_a == 1:
                # Placar 0-1 aos 60 mins -> Decay gigante
                odd_exit = odd_entrada * 0.20
            else:
                odd_exit = 1.01
                
        if odd_exit <= 1.01: odd_exit = 1.01
        
        # Formula Lay PL em Stakes = 1 - (Odd Entrada / Odd Saida)
        return round(1 - (odd_entrada / odd_exit), 2)

    except Exception:
        # Se der erro no parse, usamos Void
        return 0.0

# ── App ───────────────────────────────────────────────────────────────────────

st.title("📊 Resultados Lay 0x1 (Monitor de Lucros)")
st.markdown("Acompanhe o desempenho das suas planilhas de *paper trading* do Lay 0x1. Salve os arquivos gerados pela Página 5 na pasta `paper_trading_lay0x1` e acompanhe os gráficos de evolução da banca.")

# Configuração do Tipo de Liquidação no Sidebar
tipo_liquidacao = st.sidebar.selectbox(
    "Escolha a Regra de Liquidação",
    ["Full Match (Segurar até o final)", "Rota 60 (Cash Out aos 60 min)"],
    index=0,
    help="O backtest provou que o Full Match é o cenário mais lucrativo a longo prazo."
)

df = _carregar()
if df.empty:
    st.info(f"📂 Nenhuma planilha `.xlsx` de paper trading encontrada na pasta `{DIR_APOSTAS}`. Gere os sinais na Página 5 e salve-os lá.")
    st.stop()

# Aplica a função de PnL correta conforme a seleção do usuário
if tipo_liquidacao.startswith("Full Match"):
    df["PnL"] = df.apply(_pnl_lay0x1_full_match, axis=1)
else:
    df["PnL"] = df.apply(_pnl_lay0x1_rota60, axis=1)

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
c4.metric("Descartados (Void)", len(voids))
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
    title=f"Evolução da Banca ({tipo_liquidacao})",
    xaxis_title="Nº Aposta Executada",
    yaxis_title="Retorno Acumulado (Stakes)",
    height=320,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Tabela principal ──────────────────────────────────────────────────────────
st.subheader("📋 Histórico de Operações (Somente Entradas Confirmadas)")

cols_exibir = ["Date", "Horario", "Liga", "Mandante", "Visitante", "Metodo", "Odd_lay_entrada", "Placar_final", "Momento_gols", "PnL"]
tbl = df_ord[cols_exibir].copy() if not df_ord.empty else pd.DataFrame(columns=cols_exibir)

# Renomeia colunas para ficar elegante
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
    st.info("Nenhuma entrada confirmada 0x1 até o momento.")
