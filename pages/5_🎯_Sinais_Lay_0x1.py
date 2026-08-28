# -*- coding: utf-8 -*-
"""
5_Sinais_Lay_0x1 — Lay 0x1 FAVORITÃO (Odd_H<=2.20) em observação FORWARD stake-zero.
Regra: Lay Correct Score 0x1, só quando Odd_H_FT<=2.20 e a odd de LAY REAL do 0-1 está 5.00-13.00.
Hold até o fim. GREEN se FT != 0-1; RED se FT == 0-1. Odd real do coletor Betfair.
Lê o log do observador (lay0x1_fav_acumulado.csv) + a validação histórica (Lay0x1_Favoritao_21ago.xlsx).
"""
import os, io, subprocess
from datetime import date
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
@st.cache_data(ttl=900, show_spinner=False)
def scan_sinais_api(_hoje):
    """Config OURO COMBINADA via API Betfair (roda inline no Cloud):
    Lay 0x1: Odd_H_Back<=2.20 + Odd_CS_0x1_Lay 5-13 + Odd_Over25_FT_Back<=2.00
    Lay 1x0: Odd_A_Back<=2.20 + Odd_CS_1x0_Lay 5-13 + Odd_Over25_FT_Back<=2.00"""
    from datetime import date, timedelta
    from futpythontrader_client import get_daily_dataframe
    out = []
    for dd in (0, 1):
        ds = (date.today() + timedelta(days=dd)).isoformat()
        try:
            df = get_daily_dataframe(source="betfair", date_str=ds)
        except Exception:
            continue
        if df is None or df.empty or "Odd_CS_0x1_Lay" not in df.columns:
            continue
        oh = pd.to_numeric(df["Odd_H_Back"], errors="coerce"); oa = pd.to_numeric(df["Odd_A_Back"], errors="coerce")
        ov = pd.to_numeric(df["Odd_Over25_FT_Back"], errors="coerce")
        l01 = pd.to_numeric(df["Odd_CS_0x1_Lay"], errors="coerce"); l10 = pd.to_numeric(df["Odd_CS_1x0_Lay"], errors="coerce")
        base = dict(Data=ds, Hora=df["Time"].astype(str).str[:5], Liga=df["League"], Mandante=df["Home"], Visitante=df["Away"])
        m1 = (oh <= 2.20) & (l01 >= 5) & (l01 <= 13) & (ov <= 2.00)
        if m1.any():
            s = pd.DataFrame(base)[m1].assign(Metodo="Lay 0x1", Fav_odd=oh[m1].round(2), Lay=l01[m1].round(2), Over25=ov[m1].round(2))
            out.append(s)
        m2 = (oa <= 2.20) & (l10 >= 5) & (l10 <= 13) & (ov <= 2.00)
        if m2.any():
            s = pd.DataFrame(base)[m2].assign(Metodo="Lay 1x0", Fav_odd=oa[m2].round(2), Lay=l10[m2].round(2), Over25=ov[m2].round(2))
            out.append(s)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


c1, c2 = st.columns([1, 4])
with c1:
    scan = st.button("🔄 Scan sinais OURO (hoje/amanhã)", use_container_width=True, type="primary")
with c2:
    st.caption("Config **OURO combinada** direto no Cloud via API: **Lay 0x1** (favoritão casa) + **Lay 1x0** "
               "(favoritão fora), ambos com **Over 2.5 ≤ 2,00** e lay do placar 5-13. Layar o placar (0-1 ou 1-0) na odd mostrada.")
if scan:
    with st.spinner("Puxando sinais OURO na API Betfair..."):
        sig = scan_sinais_api(date.today().isoformat())
    if len(sig):
        n1 = (sig["Metodo"] == "Lay 0x1").sum(); n2 = (sig["Metodo"] == "Lay 1x0").sum()
        st.success(f"🎯 {len(sig)} sinais OURO (hoje/amanhã) — {n1} Lay 0x1 + {n2} Lay 1x0.")
        st.dataframe(sig[["Data", "Hora", "Liga", "Mandante", "Visitante", "Metodo", "Fav_odd", "Lay", "Over25"]].sort_values(["Data", "Hora"]),
                     use_container_width=True, hide_index=True)
    else:
        st.warning("API não retornou sinais agora (token/rede) ou sem favoritão na faixa. Tente mais tarde.")


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
    _cols_p = [c for c in ["data", "jogo", "liga", "metodo", "fav_odd", "odd_lay01"] if c in fwd.columns]
    _cols_f = [c for c in ["data", "jogo", "metodo", "fav_odd", "odd_lay01", "resultado", "pnl"] if c in fwd.columns]
    if len(pend):
        st.caption("Sinais de HOJE/próximos (siga estes):")
        st.dataframe(pend[_cols_p].sort_values("data"), use_container_width=True, hide_index=True)
    if len(fin):
        st.dataframe(fin[_cols_f].sort_values("data", ascending=False), use_container_width=True, hide_index=True)
else:
    st.info("Log forward ainda vazio. Rode **🔄 Escanear hoje** (ou a tarefa diária).")

# ── FORWARD OOS 21/08+ (odd real do coletor, não selecionado por resultado) ──
st.subheader("Forward OOS na odd real — 21/08 em diante (conta)")
if HIST.exists():
    h = pd.read_excel(HIST)
    # ── filtros (faça seus testes) ──
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        olo, ohi = st.slider("Faixa Odd_Lay_0x1", 5.0, 13.0, (5.0, 13.0), 0.5)
    with fc2:
        ohmax = st.slider("Odd_H máx (favoritismo)", 1.10, 2.20, 2.20, 0.05)
    with fc3:
        resf = st.multiselect("Resultado", ["GREEN", "RED"], default=["GREEN", "RED"])
    f = h[(h["Odd_Lay_0x1"] >= olo) & (h["Odd_Lay_0x1"] <= ohi) & (h["Odd_H"] <= ohmax) & (h["Resultado"].isin(resf))]
    liq = f[f["Resultado"].isin(["GREEN", "RED"])]
    if len(liq):
        wr = (liq["Resultado"] == "GREEN").mean() * 100
        bem = liq["Odd_Lay_0x1"].apply(be).mean() * 100
        pnl = np.where(liq["Resultado"] == "GREEN", 0.955, -(liq["Odd_Lay_0x1"] - 1))
        roi = pnl.sum() / (liq["Odd_Lay_0x1"] - 1).sum() * 100
        a, b, c, d = st.columns(4)
        a.metric("N", f"{len(liq)}"); b.metric("WR vs BE", f"{wr:.1f}%", f"{wr - bem:+.1f}%")
        c.metric("ROI / liability", f"{roi:+.1f}%"); d.metric("Reds", f"{(liq['Resultado']=='RED').sum()}")
    st.dataframe(f.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)
    _buf = io.BytesIO()
    with pd.ExcelWriter(_buf, engine="openpyxl") as _w:
        f.to_excel(_w, index=False, sheet_name="lay0x1_fav")
    st.download_button("⬇️ Baixar (filtro atual)", _buf.getvalue(), file_name="lay0x1_favoritao_filtrado.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.caption("Break-even ≈ 91-92% na odd real (10-13). A WR precisa ficar **confortavelmente acima** — hoje raspa. "
               "⚠️ Um RED 0-1 pode ser 0-2/0-3 real (coletor perde gol tardio) — confira os reds suspeitos na web.")
else:
    st.info("Validação histórica não gerada ainda.")
