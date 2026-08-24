# -*- coding: utf-8 -*-
"""
20_Resultados_Paper — painel unico do paper trading (le paper_consolidado.csv)
Atualizado pelo atualizar_paper.bat (gerar_sinais_local + consolidar_sinais).
Mostra por metodo: WR vs break-even, ROI, P&L, curva acumulada, pendentes.
"""
import os
import subprocess
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Resultados Paper ARKAD", page_icon="📊", layout="wide")
ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "paper_consolidado.csv"

st.title("📊 Resultados — Paper Trading")
st.caption("Fonte única: `paper_consolidado.csv` (gerado local + placar do coletor Betfair). "
           "Sem planilha, sem Google.")

# ── botao atualizar agora (roda o pipeline local) ──
c1, c2 = st.columns([1, 4])
with c1:
    if st.button("🔄 Atualizar agora", use_container_width=True):
        with st.spinner("Gerando sinais + puxando placares do coletor..."):
            try:
                py = str(ROOT / ".venv" / "Scripts" / "python.exe")
                if not os.path.exists(py):
                    py = "python"
                subprocess.run([py, str(ROOT / "gerar_sinais_local.py")], cwd=str(ROOT), timeout=600)
                subprocess.run([py, str(ROOT / "consolidar_sinais.py"), "--dias", "5"], cwd=str(ROOT), timeout=600)
                st.cache_data.clear()
                st.success("Atualizado!")
            except Exception as e:
                st.error(f"Falha ao atualizar (rode local): {str(e)[:120]}")

if not CSV.exists():
    st.warning("`paper_consolidado.csv` ainda não existe. Rode `atualizar_paper.bat` "
               "ou clique em **Atualizar agora**.")
    st.stop()


@st.cache_data(ttl=300, show_spinner=False)
def carregar(path, mtime):
    d = pd.read_csv(path)
    d["Data"] = pd.to_datetime(d["Data"], errors="coerce")
    for c in ["Odd", "Stake_R", "Resp_R", "Lucro_Est_R", "Lucro_Real_R", "Gols_M", "Gols_V"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    return d


df = carregar(str(CSV), os.path.getmtime(CSV))
if df.empty:
    st.info("Consolidado vazio."); st.stop()

# ── filtros ──
with st.sidebar:
    st.header("Filtros")
    import datetime as _dt
    INICIO_COLETA = _dt.date(2026, 8, 9)   # coleta Betfair comecou aqui; antes nao interessa
    _dmin = max(df["Data"].min().date(), INICIO_COLETA) if pd.notna(df["Data"].min()) else INICIO_COLETA
    _dmax = df["Data"].max().date() if pd.notna(df["Data"].max()) else INICIO_COLETA
    periodo = st.date_input("Período", value=(_dmin, _dmax))
    metodos = sorted(df["Metodo"].dropna().unique())
    sel = st.multiselect("Métodos", metodos, default=metodos)

f = df[df["Metodo"].isin(sel)].copy()
if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
    f = f[(f["Data"].dt.date >= periodo[0]) & (f["Data"].dt.date <= periodo[1])]

liq = f[f["Resultado"].isin(["GREEN", "RED"])].copy()
pend = f[~f["Resultado"].isin(["GREEN", "RED"])].copy()


def be(o):
    return (o - 1) / (o - 0.05) if pd.notna(o) and o > 1 else np.nan

# ── KPIs topo ──
st.subheader("Visão geral (apostas liquidadas)")
k1, k2, k3, k4, k5 = st.columns(5)
n = len(liq); pnl = liq["Lucro_Real_R"].sum()
wr = (liq["Resultado"] == "GREEN").mean() * 100 if n else 0
stake_tot = liq["Stake_R"].fillna(100).sum() if n else 0
roi = (pnl / stake_tot * 100) if stake_tot else 0
k1.metric("Apostas", f"{n}")
k2.metric("P&L real", f"R$ {pnl:,.0f}")
k3.metric("Win rate", f"{wr:.1f}%")
k4.metric("ROI s/ stake", f"{roi:+.1f}%")
k5.metric("Pendentes", f"{len(pend)}")

# ── download Excel (conferir os jogos) ──
import io
_buf = io.BytesIO()
with pd.ExcelWriter(_buf, engine="openpyxl") as _w:
    f.sort_values(["Data", "Metodo"]).to_excel(_w, index=False, sheet_name="paper")
st.download_button("⬇️ Baixar Excel (conferir jogos)", _buf.getvalue(),
                   file_name="paper_resultados.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── por metodo ──
st.subheader("Por método")
linhas = []
for m, g in liq.groupby("Metodo"):
    nn = len(g); gr = (g["Resultado"] == "GREEN").sum()
    wrm = gr / nn * 100
    oddm = g["Odd"].median()
    bem = be(oddm) * 100
    p = g["Lucro_Real_R"].sum()
    stk = g["Stake_R"].fillna(100).sum()
    linhas.append({
        "Método": m, "Apostas": nn, "Green": gr, "Red": nn - gr,
        "WR %": round(wrm, 1), "Odd méd": round(oddm, 2), "Break-even %": round(bem, 1),
        "Margem (WR−BE)": round(wrm - bem, 1),
        "P&L R$": round(p, 0), "ROI %": round(p / stk * 100, 1) if stk else 0,
        "Status": "✅ acima do BE" if wrm >= bem and p > 0 else ("⚠️ abaixo do BE" if wrm < bem else "➖"),
    })
if linhas:
    tab = pd.DataFrame(linhas).sort_values("P&L R$", ascending=False)
    st.dataframe(tab, use_container_width=True, hide_index=True)
    st.caption("**Break-even %** = WR mínima pra empatar na odd mediana `(odd−1)/(odd−0,05)`. "
               "Se a WR real não está confortavelmente acima do break-even, o método é frágil "
               "(a odd real cara come os greens).")
else:
    st.info("Nenhuma aposta liquidada no filtro.")

# ── curva acumulada + por dia ──
if n:
    st.subheader("Evolução")
    cc1, cc2 = st.columns(2)
    with cc1:
        cur = liq.dropna(subset=["Data"]).sort_values("Data")
        cur["Acumulado R$"] = cur["Lucro_Real_R"].cumsum()
        st.line_chart(cur.set_index("Data")["Acumulado R$"], height=280)
        st.caption("P&L acumulado (todas as apostas liquidadas do filtro).")
    with cc2:
        dia = liq.dropna(subset=["Data"]).groupby(liq["Data"].dt.date)["Lucro_Real_R"].sum()
        st.bar_chart(dia, height=280)
        st.caption("P&L por dia.")

# ── pendentes (conferir) ──
if len(pend):
    st.subheader(f"Pendentes / sem placar ({len(pend)})")
    st.caption("Jogos futuros (preenchem no próximo run) ou que a **base histórica** e o **coletor** "
               "não casaram. Para preencher à mão: abra **`placares_manuais.xlsx`** (pasta ARKAD_PROD), "
               "digite `Gols_M` e `Gols_V` dos que já jogaram, salve e rode 🔄 Atualizar agora. "
               "O consolidador exporta os pendentes pra esse Excel automaticamente.")
    cols = [c for c in ["Data", "Metodo", "Liga", "Mandante", "Visitante", "Odd", "Resultado"] if c in pend.columns]
    st.dataframe(pend[cols].sort_values("Data"), use_container_width=True, hide_index=True)
