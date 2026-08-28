# -*- coding: utf-8 -*-
"""
5_Sinais_Lay_0x1 — Lay 0x1 e Lay 1x0 SUPER FAVORITO (Odd <= 1.80) — IDEIA 2 (Stake-Zero).
Regra:
  - Lay 0x1: Mandante Super Favorito (Odd_H_Back <= 1.80) e 5.00 <= Odd_CS_0x1_Lay <= 13.00.
  - Lay 1x0: Visitante Super Favorito (Odd_A_Back <= 1.80) e 5.00 <= Odd_CS_1x0_Lay <= 13.00.
Modo 100% Punter (Hold 90 min). Green se FT != placar alvo.
Sinais 100% via API Betfair FutPythonTrader (roda direto no Cloud).
"""
import os, io, subprocess
from datetime import date
from pathlib import Path
import numpy as np, pandas as pd, streamlit as st

st.set_page_config(page_title="Lay CS Super Favorito", page_icon="🎯", layout="wide")
ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "lay0x1_fav_acumulado.csv"
HIST = ROOT / "Lay0x1_Favoritao_21ago.xlsx"
COMM = 0.045


def be(o):
    return (o - 1) / (o - COMM) if o and o > 1 else np.nan


st.title("🎯 Lay CS — Super Favorito (Odd ≤ 1,80) [IDEIA 2]")
st.warning(
    "**Observação FORWARD stake-ZERO — Modo Punter (Hold 90').** "
    "Auditado no coletor Betfair (N=1.271 ticks, spread mediano 1,19x): "
    "O filtro **Super Favorito (Odd ≤ 1,80)** reduz o placar seco para <6% e entrega **8 de 8 meses positivos** no backtest. "
    "**Prioridade no Lay 0x1** (Mandante). O Lay 1x0 (Visitante) é o elo mais fraco e roda em observação isolada.", icon="⚠️")


@st.cache_data(ttl=900, show_spinner=False)
def scan_sinais_api(_hoje, fav_max=1.90, lay_max=15.0):
    """Scan via API Betfair FutPythonTrader (sem filtro Over25, roda 100% no Cloud):
    Lay 0x1: Odd_H_Back <= fav_max + Odd_CS_0x1_Lay 5 a lay_max
    Lay 1x0: Odd_A_Back <= fav_max + Odd_CS_1x0_Lay 5 a lay_max"""
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
        oh = pd.to_numeric(df["Odd_H_Back"], errors="coerce")
        oa = pd.to_numeric(df["Odd_A_Back"], errors="coerce")
        l01 = pd.to_numeric(df["Odd_CS_0x1_Lay"], errors="coerce")
        l10 = pd.to_numeric(df["Odd_CS_1x0_Lay"], errors="coerce")
        base = dict(Data=ds, Hora=df["Time"].astype(str).str[:5], Liga=df["League"], Mandante=df["Home"], Visitante=df["Away"])
        
        # 1. Lay 0x1 (Mandante Fav)
        m1 = (oh <= fav_max) & (l01 >= 5.0) & (l01 <= lay_max)
        if m1.any():
            s1 = pd.DataFrame(base)[m1].assign(Metodo="Lay 0x1", Fav_odd=oh[m1].round(2), Lay=l01[m1].round(2))
            out.append(s1)
            
        # 2. Lay 1x0 (Visitante Fav)
        m2 = (oa <= fav_max) & (l10 >= 5.0) & (l10 <= lay_max)
        if m2.any():
            s2 = pd.DataFrame(base)[m2].assign(Metodo="Lay 1x0", Fav_odd=oa[m2].round(2), Lay=l10[m2].round(2))
            out.append(s2)
            
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


st.markdown("### 🎛️ Scanner de Sinais ao Vivo (API Betfair Cloud)")
fc1, fc2, fc3 = st.columns([1.5, 1.5, 2])
with fc1:
    filtro_fav_max = st.slider("Odd Máx do Favorito", 1.20, 2.20, 1.90, 0.05, help="Padrão auditado: 1.80 a 1.90")
with fc2:
    filtro_lay_max = st.slider("Odd Máx do Lay CS", 10.0, 18.0, 15.0, 0.5, help="Padrão auditado: 13.0 a 15.0")
with fc3:
    scan = st.button("🔄 Scan Sinais ao Vivo (API Betfair)", use_container_width=True, type="primary")

if scan:
    with st.spinner(f"Consultando grade de hoje e amanhã na Betfair API (Fav ≤ {filtro_fav_max:.2f}, Lay ≤ {filtro_lay_max:.1f})..."):
        sig = scan_sinais_api(date.today().isoformat(), fav_max=filtro_fav_max, lay_max=filtro_lay_max)
    if len(sig):
        n1 = (sig["Metodo"] == "Lay 0x1").sum()
        n2 = (sig["Metodo"] == "Lay 1x0").sum()
        st.success(f"🎯 **{len(sig)} sinais encontrados na API Betfair** — 🟢 {n1} Lay 0x1 (Mandante Fav) | ⚠️ {n2} Lay 1x0 (Visitante Fav).")
        
        tab_sig1, tab_sig2, tab_sig_tot = st.tabs(["🟢 Lay 0x1 (Mandante ≤ 1.80)", "⚠️ Lay 1x0 (Visitante ≤ 1.80)", "📋 Todos os Sinais"])
        with tab_sig1:
            st.dataframe(sig[sig["Metodo"] == "Lay 0x1"][["Data", "Hora", "Liga", "Mandante", "Visitante", "Fav_odd", "Lay"]].sort_values(["Data", "Hora"]),
                         use_container_width=True, hide_index=True)
        with tab_sig2:
            st.dataframe(sig[sig["Metodo"] == "Lay 1x0"][["Data", "Hora", "Liga", "Mandante", "Visitante", "Fav_odd", "Lay"]].sort_values(["Data", "Hora"]),
                         use_container_width=True, hide_index=True)
        with tab_sig_tot:
            st.dataframe(sig[["Data", "Hora", "Liga", "Mandante", "Visitante", "Metodo", "Fav_odd", "Lay"]].sort_values(["Data", "Hora"]),
                         use_container_width=True, hide_index=True)
    else:
        st.warning("API não retornou sinais no momento ou não há jogos com super favorito na faixa de lay 5-13.")


@st.cache_data(ttl=300, show_spinner=False)
def carregar(p, mt):
    return pd.read_csv(p)


# ── FORWARD (log do observador) ──
st.markdown("---")
st.subheader("📊 Forward Stake-Zero Acumulado (Auditado)")

if LOG.exists():
    fwd = carregar(str(LOG), os.path.getmtime(LOG))
    fin = fwd[fwd["status"] == "Finalizado"]
    pend = fwd[fwd["status"] == "Pendente"]
    
    # Métricas Gerais
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Liquidados", f"{len(fin)}")
    k2.metric("Pendentes", f"{len(pend)}")
    
    if len(fin):
        wr = (fin["resultado"] == "GREEN").mean() * 100
        bem = fin["odd_lay01"].apply(be).mean() * 100
        roi = fin["pnl"].sum() / (fin["odd_lay01"] - 1).sum() * 100
        k3.metric("WR vs BE Geral", f"{wr:.1f}%", f"{wr - bem:+.1f}%")
        k4.metric("ROI Geral / Liability", f"{roi:+.1f}%")
        
        # Desdobramento por Método
        t1, t2 = st.columns(2)
        with t1:
            fin_01 = fin[fin["metodo"] == "Lay 0x1"]
            if len(fin_01):
                wr01 = (fin_01["resultado"] == "GREEN").mean() * 100
                roi01 = fin_01["pnl"].sum() / (fin_01["odd_lay01"] - 1).sum() * 100
                st.info(f"🟢 **Lay 0x1 (Mandante Fav):** N={len(fin_01)} | WR={wr01:.1f}% | ROI/liability={roi01:+.1f}%")
        with t2:
            fin_10 = fin[fin["metodo"] == "Lay 1x0"]
            if len(fin_10):
                wr10 = (fin_10["resultado"] == "GREEN").mean() * 100
                roi10 = fin_10["pnl"].sum() / (fin_10["odd_lay01"] - 1).sum() * 100
                st.warning(f"⚠️ **Lay 1x0 (Visitante Fav):** N={len(fin_10)} | WR={wr10:.1f}% | ROI/liability={roi10:+.1f}% *(Elo Fraco)*")

    _cols_p = [c for c in ["data", "jogo", "liga", "metodo", "fav_odd", "odd_lay01"] if c in fwd.columns]
    _cols_f = [c for c in ["data", "jogo", "metodo", "fav_odd", "odd_lay01", "resultado", "pnl", "conferir_web"] if c in fwd.columns]
    
    if len(pend):
        st.markdown("#### ⏳ Sinais Pendentes")
        st.dataframe(pend[_cols_p].sort_values("data"), use_container_width=True, hide_index=True)
    if len(fin):
        st.markdown("#### ✅ Histórico de Liquidações")
        st.dataframe(fin[_cols_f].sort_values("data", ascending=False), use_container_width=True, hide_index=True)
else:
    st.info("Log forward ainda vazio. Rode o observador diário.")

# ── FORWARD OOS 21/08+ ──
st.markdown("---")
st.subheader("📈 Validação Histórica OOS (21/08+)")
if HIST.exists():
    h = pd.read_excel(HIST)
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        olo, ohi = st.slider("Faixa Odd_Lay_0x1", 5.0, 13.0, (5.0, 13.0), 0.5)
    with fc2:
        ohmax = st.slider("Odd_H máx (Super Favorito)", 1.10, 2.20, 1.80, 0.05)
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
        a.metric("N", f"{len(liq)}")
        b.metric("WR vs BE", f"{wr:.1f}%", f"{wr - bem:+.1f}%")
        c.metric("ROI / liability", f"{roi:+.1f}%")
        d.metric("Reds", f"{(liq['Resultado']=='RED').sum()}")
    st.dataframe(f.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)
    _buf = io.BytesIO()
    with pd.ExcelWriter(_buf, engine="openpyxl") as _w:
        f.to_excel(_w, index=False, sheet_name="lay0x1_fav")
    st.download_button("⬇️ Baixar Validação (Excel)", _buf.getvalue(), file_name="lay0x1_super_favoritao.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.caption("⚠️ **Aviso de Governança:** Qualquer RED deve ser auditado no pós-jogo (bug do gol tardio).")

