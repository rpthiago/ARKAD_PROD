"""
Minhas Apostas Reais — ARKAD_PROD
Tabela consolidada de todas as apostas em Apostas_Diarias/*.xlsx
"""
from __future__ import annotations

import glob
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Minhas Apostas Reais", page_icon="💰", layout="wide")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIR_APOSTAS = ROOT_DIR / "Apostas_Diarias"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _carregar() -> pd.DataFrame:
    arquivos = sorted(glob.glob(str(DIR_APOSTAS / "Apostas_*.xlsx")))
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


def _pnl(row: pd.Series) -> float:
    res = str(row.get("Resultado", "")).strip().upper()
    if res in ("", "NAN", "PENDENTE", "-"):
        return np.nan
    manual = row.get("Lucro_Prejuizo") or row.get("PnL") or row.get("PnL_Linha")
    if pd.notna(manual) and str(manual).strip() not in ("", "nan"):
        try:
            return float(manual)
        except Exception:
            pass
    try:
        stake = float(row.get("Stake") or row.get("Responsabilidade_Sugerida_R$") or 0)
        odd   = float(row.get("Odd_Real_Pega") or row.get("Odd real") or row.get("Odd_Base") or 0)
    except Exception:
        return np.nan
    if stake <= 0 or odd <= 1:
        return np.nan
    if any(k in res for k in ("GREEN", "VITORIA", "VIT")):
        return round(stake / (odd - 1.0), 2)
    if any(k in res for k in ("RED", "DERROTA", "DER")):
        return round(-stake, 2)
    if any(k in res for k in ("VOID", "DEVOLVIDA", "REEMBOLSO")):
        return 0.0
    return np.nan


def _prio(row: pd.Series) -> str:
    m = str(row.get("Metodo", ""))
    o = pd.to_numeric(
        row.get("Odd_Base") or row.get("Odd real") or row.get("Odd_Real_Pega"),
        errors="coerce"
    )
    o = float(o) if pd.notna(o) else 0.0
    if m == "Lay_CS_0x1_B365":
        return "P1 ⭐" if o >= 9 else "P2"
    if m == "Lay_CS_1x0_B365":
        return "P3" if o < 9 else "P4"
    return "P?"


def _to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Apostas")
    return buf.getvalue()


# ── App ───────────────────────────────────────────────────────────────────────

st.title("💰 Minhas Apostas Reais")

df = _carregar()
if df.empty:
    st.info("📂 Nenhuma planilha `Apostas_*.xlsx` encontrada em `Apostas_Diarias/`.")
    st.stop()

res_col = next((c for c in df.columns if "resultado" in c.lower()), None)
df["Resultado"] = (df[res_col].fillna("").astype(str).str.strip().str.upper()
                   if res_col else "")

df["PnL"]  = df.apply(_pnl, axis=1)
df["Prio"] = df.apply(_prio, axis=1)

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
sinal = "+" if lucro >= 0 else ""
c5.metric("Lucro Total", f"R$ {sinal}{lucro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.divider()

# ── Gráfico curva de banca ────────────────────────────────────────────────────
date_col = next((c for c in resolvidas.columns
                 if "data" in c.lower() or c.lower() == "date"), None)
df_ord = resolvidas.sort_values(date_col) if date_col else resolvidas.copy()
df_ord["_acum"] = df_ord["PnL"].cumsum()

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=list(range(1, len(df_ord) + 1)),
    y=df_ord["_acum"],
    mode="lines+markers",
    line=dict(color="#27ae60" if lucro >= 0 else "#e74c3c", width=2),
    fill="tozeroy",
))
fig.add_hline(y=0, line_dash="dash", line_color="gray")
fig.update_layout(
    title="Evolução do P&L Acumulado",
    xaxis_title="Nº Aposta",
    yaxis_title="Lucro Acumulado (R$)",
    height=280,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Tabela principal ──────────────────────────────────────────────────────────
st.subheader("📋 Histórico Consolidado")

cols_exibir = []
for c in ["Prio", date_col, "Horario_Entrada", "Liga", "Jogo", "Metodo",
          "Odd_Base", "Odd_Real_Pega", "Stake", "Responsabilidade_Sugerida_R$",
          "Resultado", "PnL"]:
    if c and c in resolvidas.columns:
        cols_exibir.append(c)
if not cols_exibir:
    cols_exibir = [c for c in resolvidas.columns if not c.startswith("__")]

tbl = resolvidas[cols_exibir].copy()


def _cor(row: pd.Series) -> list:
    r = str(row.get("Resultado", "")).upper()
    if any(k in r for k in ("GREEN", "VITORIA")):
        return ["background-color: #d1fae5; color: #065f46"] * len(row)
    if any(k in r for k in ("RED", "DERROTA")):
        return ["background-color: #fee2e2; color: #991b1b"] * len(row)
    return ["color: #6b7280"] * len(row)


fmt = {}
if "PnL" in tbl.columns:
    fmt["PnL"] = "R$ {:+,.2f}"
if "Odd_Base" in tbl.columns:
    fmt["Odd_Base"] = "{:.2f}"
if "Odd_Real_Pega" in tbl.columns:
    fmt["Odd_Real_Pega"] = "{:.2f}"

st.dataframe(
    tbl.style.apply(_cor, axis=1).format(fmt, na_rep="-"),
    use_container_width=True,
    hide_index=True,
    height=420,
)

# ── Pendentes (expander) ──────────────────────────────────────────────────────
if not pendentes.empty:
    with st.expander(f"⏳ Pendentes de resultado ({len(pendentes)})"):
        cols_p = [c for c in ["Prio", date_col, "Liga", "Jogo", "Metodo", "Odd_Base", "Stake"]
                  if c and c in pendentes.columns]
        st.dataframe(pendentes[cols_p] if cols_p else pendentes,
                     use_container_width=True, hide_index=True)

st.divider()

# ── Download Excel ────────────────────────────────────────────────────────────
st.download_button(
    "📥 Baixar Excel consolidado",
    data=_to_excel(tbl),
    file_name="apostas_reais_consolidado.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
