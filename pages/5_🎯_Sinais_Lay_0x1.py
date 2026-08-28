# -*- coding: utf-8 -*-
"""
5_Sinais_Lay_0x1 — Lay 0x1 FAVORITÃO (Odd_H<=2.20) em observação FORWARD stake-zero.
Regra: Lay Correct Score 0x1, só quando Odd_H_FT<=2.20 e a odd de LAY REAL do 0-1 está 5.00-13.00.
Hold até o fim. GREEN se FT != 0-1; RED se FT == 0-1. Odd real do coletor Betfair.
Lê o log do observador (lay0x1_fav_acumulado.csv) + a validação histórica (Lay0x1_Favoritao_21ago.xlsx).
"""
import os, subprocess
from pathlib import Path
import numpy as np, pandas as pd, streamlit as st

st.set_page_config(page_title="Lay 0x1 Favoritão", page_icon="🎯", layout="wide")
ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "lay0x1_fav_acumulado.csv"
HIST = ROOT / "Lay0x1_Favoritao_21ago.xlsx"
COMM = 0.045


def be(o):
    return (o - 1) / (o - COMM) if o and o > 1 else np.nan


st.title("🎯 Lay 0x1 — Favoritão (Odd_H ≤ 2,20)")
st.warning(
    "**Observação FORWARD stake-ZERO — NÃO é aposta real.** Lay 0x1 puro perde (−7% no paper). O filtro "
    "**favoritão em casa (Odd_H≤2,20)** é o único que sobrevive na odd de lay REAL. No forward OOS na odd real "
    "(21/08+, após corrigir 2 falsos-reds do coletor): **WR 92,7% vs BE 91,3% → ROI +2,2%** — positivo, mas "
    "**fino e de alta variância** (cauda gorda de CS). Segue acumulando pra confirmar.", icon="⚠️")
st.caption("O log é atualizado pela tarefa **local** (`LAY0X1_FAV_forward`, diária) — o coletor só roda na máquina "
           "local, não no Streamlit Cloud. Esta página **exibe** o resultado.")


@st.cache_data(ttl=300, show_spinner=False)
def carregar(p, mt):
    return pd.read_csv(p)


# ── FORWARD (log do observador) ──
st.subheader("Forward stake-zero (a partir de 2026-08-29)")
if LOG.exists():
    fwd = carregar(str(LOG), os.path.getmtime(LOG))
    fin = fwd[fwd["status"] == "Finalizado"]; pend = fwd[fwd["status"] == "Pendente"]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Liquidados", f"{len(fin)}"); k2.metric("Pendentes", f"{len(pend)}")
    if len(fin):
        wr = (fin["resultado"] == "GREEN").mean() * 100
        bem = fin["odd_lay01"].apply(be).mean() * 100
        roi = fin["pnl"].sum() / (fin["odd_lay01"] - 1).sum() * 100
        k3.metric("WR vs BE", f"{wr:.1f}%", f"{wr - bem:+.1f}%")
        k4.metric("ROI / liability", f"{roi:+.1f}%")
    if len(pend):
        st.caption("Sinais de HOJE/próximos (siga estes):")
        st.dataframe(pend[["data", "jogo", "liga", "odd_h", "odd_lay01"]].sort_values("data"),
                     use_container_width=True, hide_index=True)
    if len(fin):
        st.dataframe(fin[["data", "jogo", "odd_h", "odd_lay01", "resultado", "pnl"]].sort_values("data", ascending=False),
                     use_container_width=True, hide_index=True)
else:
    st.info("Log forward ainda vazio. Rode **🔄 Escanear hoje** (ou a tarefa diária).")

# ── FORWARD OOS 21/08+ (odd real do coletor, não selecionado por resultado) ──
st.subheader("Forward OOS na odd real — 21/08 em diante (conta)")
if HIST.exists():
    h = pd.read_excel(HIST)
    liq = h[h["Resultado"].isin(["GREEN", "RED"])]
    if len(liq):
        wr = (liq["Resultado"] == "GREEN").mean() * 100
        bem = liq["Odd_Lay_0x1"].apply(be).mean() * 100
        roi = np.where(liq["Resultado"] == "GREEN", 0.955, -(liq["Odd_Lay_0x1"] - 1)).sum() / (liq["Odd_Lay_0x1"] - 1).sum() * 100
        a, b, c = st.columns(3)
        a.metric("N", f"{len(liq)}"); b.metric("WR vs BE", f"{wr:.1f}%", f"{wr - bem:+.1f}%")
        c.metric("ROI / liability", f"{roi:+.1f}%")
    st.dataframe(h.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)
    st.caption("Break-even ≈ 91-92% na odd real (10-13). A WR precisa ficar **confortavelmente acima** — hoje raspa.")
else:
    st.info("Validação histórica não gerada ainda.")
