# -*- coding: utf-8 -*-
"""
21_Observacao_Under15 — painel do candidato Lay Under 1.5 FT (XGBoost) em OBSERVACAO FORWARD.
Le observacao_under15_forward.csv (gerado por atualizar_feed_forward_diario.py + observar_under15_forward.py).
STAKE-ZERO: nao e aposta real; e teste cego forward. Mostra WR vs break-even, ROI (unidades),
curva, progresso ate N>=100 (quando roda bootstrap+FDR pra decidir).
"""
import os
import subprocess
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Observação Under 1.5", page_icon="🔬", layout="wide")
ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "observacao_under15_forward.csv"
COMM = 0.045
META_N = 100  # N forward liquidado p/ fechar a amostra e rodar bootstrap+FDR

st.title("🔬 Observação Forward — Lay Under 1.5 FT (XGBoost)")
st.warning(
    "**CANDIDATO em observação — stake-ZERO (NÃO é aposta real).** Este é o teste cego forward "
    "do único método que sobreviveu à auditoria: split leak-free, sem leak de feature, "
    "OOS 2026 +16% (IC95 bootstrap [+4,3%, +34,2%]), 7/8 meses positivos. Falta **confirmar "
    "para frente** (single-split não é walk-forward) + **FDR**. Só vira operável se segurar aqui.",
    icon="⚠️")

# ── atualizar agora (roda o pipeline diario) ──
c1, c2 = st.columns([1, 4])
with c1:
    if st.button("🔄 Rodar scan agora", use_container_width=True):
        with st.spinner("Baixando fixtures + features + observando..."):
            try:
                py = str(ROOT / ".venv" / "Scripts" / "python.exe")
                if not os.path.exists(py):
                    py = str(ROOT.parent / "DASHBOARD_ARKAD-1" / ".venv" / "Scripts" / "python.exe")
                if not os.path.exists(py):
                    py = "python"
                subprocess.run([py, str(ROOT / "atualizar_feed_forward_diario.py")], cwd=str(ROOT), timeout=600)
                st.cache_data.clear(); st.success("Scan rodado!")
            except Exception as e:
                st.error(f"Falha (rode local): {str(e)[:120]}")

if not CSV.exists():
    st.info("`observacao_under15_forward.csv` ainda não existe. Rode `observar_under15.bat` "
            "(tarefa diária UNDER15_observacao) ou clique em **Rodar scan agora**. "
            "O log fica vazio até o feed trazer jogos futuros com sinal (EV≥5%).")
    st.stop()


@st.cache_data(ttl=300, show_spinner=False)
def carregar(path, mtime):
    d = pd.read_csv(path)
    d["data"] = pd.to_datetime(d["data"], errors="coerce")
    for c in ["odd_lay", "prob_ml", "ev", "pnl_unidades"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    return d


df = carregar(str(CSV), os.path.getmtime(CSV))
if df.empty:
    st.info("Log vazio — nenhum sinal forward ainda (o método dispara ~1-2×/semana)."); st.stop()

liq = df[df["status"] == "Finalizado"].copy()
pend = df[df["status"] == "Pendente"].copy()


def be(o):
    return (o - 1) / (o - COMM) if pd.notna(o) and o > 1 else np.nan

# ── KPIs ──
n = len(liq)
wr = (liq["resultado"] == "GREEN").mean() * 100 if n else 0.0
bem = liq["odd_lay"].apply(be).mean() * 100 if n else 0.0
roi = liq["pnl_unidades"].mean() * 100 if n else 0.0
pnl_u = liq["pnl_unidades"].sum() if n else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Liquidados (forward)", f"{n}")
k2.metric("Pendentes", f"{len(pend)}")
k3.metric("Win rate", f"{wr:.1f}%", f"{wr - bem:+.1f}% vs BE" if n else None)
k4.metric("Break-even", f"{bem:.1f}%")
k5.metric("ROI (unid.)", f"{roi:+.1f}%")

# ── progresso ate a amostra pre-registrada ──
st.subheader("Progresso do teste cego")
st.progress(min(1.0, n / META_N),
            text=f"{n} / {META_N} apostas forward liquidadas "
                 f"({'PRONTO p/ bootstrap+FDR' if n >= META_N else f'faltam {META_N - n}'})")
if n >= META_N:
    st.success(f"N≥{META_N}: hora de rodar o bootstrap por mês + FDR e decidir **promover ou matar**.")
else:
    st.caption(f"O veredito só sai com amostra forward suficiente (~{META_N}). O método dispara "
               "raro (~1-2/semana), então isso leva algumas semanas — é assim que tem que ser (cego).")

# ── veredito parcial (indicativo, nao decide) ──
if n:
    acima = wr > bem and roi > 0
    st.info(f"**Parcial (indicativo, não decide):** WR {wr:.1f}% vs break-even {bem:.1f}% "
            f"→ {'margem POSITIVA' if acima else 'margem negativa/nula'}. "
            "Decisão real só com N cheio + bootstrap + FDR.")

# ── curva ──
if n:
    st.subheader("Curva forward (P&L em unidades, stake-zero)")
    cur = liq.dropna(subset=["data"]).sort_values("data")
    cur["Acumulado (unid.)"] = cur["pnl_unidades"].cumsum()
    st.line_chart(cur.set_index("data")["Acumulado (unid.)"], height=280)
    st.caption("Cada green = +0,955 unid.; cada red = −(odd−1). Stake-zero: é o P&L de papel que valida o edge.")

# ── liquidados ──
if n:
    st.subheader(f"Apostas liquidadas ({n})")
    cols = ["data", "jogo", "liga", "odd_lay", "prob_ml", "ev", "resultado", "pnl_unidades"]
    st.dataframe(liq[[c for c in cols if c in liq.columns]].sort_values("data", ascending=False),
                 use_container_width=True, hide_index=True)

# ── pendentes ──
if len(pend):
    st.subheader(f"Pendentes ({len(pend)})")
    st.caption("Jogos com sinal já registrados (vistos antes do apito). Liquidam sozinhos quando "
               "o placar entra na base de resultados (FRESH) no próximo scan.")
    cols = ["data", "jogo", "liga", "odd_lay", "prob_ml", "ev", "primeiro_visto"]
    st.dataframe(pend[[c for c in cols if c in pend.columns]].sort_values("data"),
                 use_container_width=True, hide_index=True)
