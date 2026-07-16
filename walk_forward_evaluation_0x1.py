"""
walk_forward_evaluation_0x1.py — Crivo estatístico HONESTO do Lay 0x1 (Frente B).
================================================================================
Pipeline completo do protocolo honesto do projeto:
  1. Features leak-free (features_builder_0x1.build_features) sobre b365_base_lean.csv.
  2. AUDITORIA DE TRUNCAMENTO (teste unitário): features de uma data D calculadas com a
     base FULL == calculadas só com Date<=D. Prova empírica de ausência de look-ahead.
  3. Walk-forward OOS estrito: warmup 12m, retreino mês a mês, prevê só o mês seguinte.
  4. Modelo XGBoost + calibração ISOTÔNICA as-of (CalibratedClassifierCV DENTRO do treino).
  5. Odd lay REAL Betfair casada por jogo (download + matcher lay_de(), idêntico ao
     gerar_oos_real_multi_xgb.py). NUNCA a odd de back da base.
  6. EV lay verdadeiro; P&L por-aposta; recorte ago/2025+ (janela Betfair confiável).
  7. Bootstrap 20k por mês -> IC95 + p-valor. FDR Benjamini-Hochberg (m=45 hipóteses).

Rodar:  python walk_forward_evaluation_0x1.py --audit-only   # só a auditoria de truncamento
        python walk_forward_evaluation_0x1.py                # pipeline completo + veredito
"""
from __future__ import annotations

import argparse
import difflib
import io
import re
import sys
import unicodedata
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Caminhos: a base e o config vivem no repo DASHBOARD (irmão do ARKAD_PROD) ──
DASH = Path(r"c:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1")
sys.path.insert(0, str(DASH))            # p/ importar config
sys.path.insert(0, str(Path(__file__).resolve().parent))  # p/ importar features_builder_0x1

from features_builder_0x1 import build_features  # noqa: E402

LEAN = DASH / "b365_base_lean.csv"
BF_CACHE = Path(__file__).resolve().parent / "_betfair_lay0x1_cache.csv"

COMM = 0.05          # comissão Betfair
WARMUP = 12          # meses de aquecimento
EV_MIN = 0.02        # limiar de EV lay
WINDOW_START = "2025-08"   # janela confiável (odd Betfair calibrada a partir de ago/2025)
NB = 20_000          # reamostragens do bootstrap
M_HYP = 45           # hipóteses já testadas no projeto (FDR BH)
Q_FDR = 0.05
LIM_FDR = Q_FDR / M_HYP
RNG = np.random.default_rng(42)


def canon(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


# ── Odd lay REAL Betfair (download + matcher idêntico ao harness do projeto) ──
def load_betfair_matcher():
    if BF_CACHE.exists():
        bf = pd.read_csv(BF_CACHE, low_memory=False)
        print(f"[betfair] cache local: {len(bf)} jogos ({BF_CACHE.name})")
    else:
        import requests
        from config import API_BETFAIR_BASE_URL, API_HEADERS
        print("[betfair] baixando base...")
        r = requests.get(API_BETFAIR_BASE_URL, headers=dict(API_HEADERS), timeout=180)
        full = pd.read_csv(io.StringIO(r.text), low_memory=False)
        keep = [c for c in ["Date", "Home", "Away", "Odd_CS_0x1_Lay", "Odd_CS_0x1_Back"] if c in full.columns]
        bf = full[keep].copy()
        bf.to_csv(BF_CACHE, index=False)
        print(f"[betfair] baixado e cacheado: {len(bf)} jogos")
    bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
    bf["d"] = bf["Date"].dt.strftime("%Y-%m-%d")
    bf["ch"] = bf["Home"].map(canon)
    bf["ca"] = bf["Away"].map(canon)
    exact, byday = {}, {}
    for i, row in bf.iterrows():
        exact[(row["d"], row["ch"], row["ca"])] = i
        byday.setdefault(row["d"], []).append(i)
    bf_min, bf_max = bf["d"].min(), bf["d"].max()

    def lay_de(d, h, a, col="Odd_CS_0x1_Lay"):
        ch, ca = canon(h), canon(a)
        idx = exact.get((d, ch, ca))
        if idx is None:
            best, bs = None, 0.0
            for i in byday.get(d, []):
                sh = difflib.SequenceMatcher(None, ch, bf.at[i, "ch"]).ratio()
                sa = difflib.SequenceMatcher(None, ca, bf.at[i, "ca"]).ratio()
                if (sh + sa) / 2 > bs and max(sh, sa) >= 0.5:
                    bs, best = (sh + sa) / 2, i
            idx = best if bs >= 0.55 else None
        if idx is None:
            return np.nan
        v = pd.to_numeric(bf.at[idx, col], errors="coerce")
        return v if (pd.notna(v) and v > 1) else np.nan

    return lay_de, bf_min, bf_max


# ── Modelo: XGBoost + calibração isotônica as-of ─────────────────────────────
def make_model():
    from sklearn.calibration import CalibratedClassifierCV
    try:
        import xgboost as xgb
        base = xgb.XGBClassifier(
            n_estimators=250, max_depth=4, learning_rate=0.05, subsample=0.8,
            colsample_bytree=0.8, reg_lambda=1.0, tree_method="hist",
            random_state=42, verbosity=0, eval_metric="logloss")
    except Exception:
        from sklearn.ensemble import RandomForestClassifier
        base = RandomForestClassifier(n_estimators=300, max_depth=8, min_samples_leaf=20,
                                      class_weight="balanced", random_state=42, n_jobs=-1)
    # calibração isotônica ajustada SÓ nos dados de treino (cv interno), nunca prefit global
    return CalibratedClassifierCV(base, cv=3, method="isotonic")


# ── Auditoria de truncamento (teste unitário de leakage) ─────────────────────
def truncation_audit(raw: pd.DataFrame, feats: list[str], probe_date="2026-03-15") -> bool:
    D = pd.Timestamp(probe_date)
    full, cols_f = build_features(raw, verbose=False)
    trunc, cols_t = build_features(raw[pd.to_datetime(raw["Date"], errors="coerce") <= D].copy(), verbose=False)
    common = [c for c in feats if c in cols_f and c in cols_t]
    a = full[pd.to_datetime(full["Date"]) == D].sort_values(["Home", "Away"])
    b = trunc[pd.to_datetime(trunc["Date"]) == D].sort_values(["Home", "Away"])
    if len(a) == 0 or len(a) != len(b):
        print(f"[AUDITORIA] sem jogos em {D.date()} p/ comparar (ou contagem difere) — escolha outra data")
        return False
    A = a[common].to_numpy(dtype=float)
    B = b[common].to_numpy(dtype=float)
    same = A.shape == B.shape and np.allclose(A, B, equal_nan=True)
    print(f"[AUDITORIA truncamento] {D.date()}: {len(a)} jogos, {len(common)} features | "
          f"{'PASS (sem look-ahead)' if same else 'FAIL — LEAKAGE!'}")
    if not same:
        diff = np.nanmax(np.abs(A - B), axis=0)
        piores = sorted(zip(common, diff), key=lambda x: -x[1])[:5]
        print("  features divergentes:", piores)
    return same


# ── Bootstrap por mês + FDR ──────────────────────────────────────────────────
def bootstrap_month(bets: pd.DataFrame):
    meses = sorted(bets["mes"].unique())
    if len(meses) < 2:
        return np.nan, np.nan, np.nan
    pnl_m = bets.groupby("mes")["pnl"].sum().reindex(meses).to_numpy()
    n_m = bets.groupby("mes").size().reindex(meses).to_numpy()
    idx = RNG.integers(0, len(meses), size=(NB, len(meses)))
    roi = pnl_m[idx].sum(axis=1) / np.maximum(n_m[idx].sum(axis=1), 1)
    lo, hi = np.percentile(roi, [2.5, 97.5])
    return float(lo), float(hi), float((roi <= 0).mean())


def report(bets: pd.DataFrame, label: str):
    if len(bets) == 0:
        print(f"  {label:<22}: 0 apostas"); return None
    meses = sorted(bets["mes"].unique()); h = len(meses) // 2
    r1 = bets[bets["mes"].isin(meses[:h])]; r2 = bets[bets["mes"].isin(meses[h:])]
    roi = bets["pnl"].sum() / len(bets)
    wr = bets["target"].mean()
    om = bets["odd_lay"].median()
    be = (om - 1) / ((om - 1) + (1 - COMM))
    lo, hi, p = bootstrap_month(bets)
    mm = bets.groupby("mes")["pnl"].agg(["size", "sum"]); mm["roi"] = mm["sum"] / mm["size"]
    fdr = "PASSA" if (p <= LIM_FDR and lo > 0) else "reprova"
    print(f"  {label:<22}: n={len(bets):>4} odd={om:>4.1f} WR={wr:>6.2%} BE={be:>6.2%} "
          f"ROI={roi:>+7.2%} IC95=[{lo:>+6.2%},{hi:>+6.2%}] p={p:.4f} FDR={fdr} "
          f"m+={int((mm['roi']>0).sum())}/{len(mm)} H1={r1['pnl'].sum()/max(len(r1),1):+.1%} H2={r2['pnl'].sum()/max(len(r2),1):+.1%}")
    return dict(fatia=label, n=len(bets), odd=round(om, 1), wr=round(wr, 4), be=round(be, 4),
                roi=round(roi, 4), ic_lo=round(lo, 4), ic_hi=round(hi, 4), p=round(p, 5), fdr=fdr)


# ── Walk-forward ─────────────────────────────────────────────────────────────
def walk_forward(feat: pd.DataFrame, feats: list[str], lay_de, bf_min, bf_max) -> pd.DataFrame:
    feat = feat.dropna(subset=feats + ["target"]).copy()
    feat["Date"] = pd.to_datetime(feat["Date"], errors="coerce")
    feat["_month"] = feat["Date"].dt.to_period("M")
    feat["d"] = feat["Date"].dt.strftime("%Y-%m-%d")
    meses = sorted(feat["_month"].unique())
    from sklearn.preprocessing import StandardScaler
    rows = []
    for mes in meses[WARMUP:]:
        tr = feat[feat["_month"] < mes]
        te = feat[(feat["_month"] == mes) & (feat["d"] >= bf_min) & (feat["d"] <= bf_max)].copy()
        if len(tr) < 400 or len(te) < 5 or tr["target"].nunique() < 2:
            continue
        # odd lay real só p/ os jogos de teste (treino não usa odd)
        te["odd_lay"] = [lay_de(d, h, a) for d, h, a in zip(te["d"], te["Home"], te["Away"])]
        te = te.dropna(subset=["odd_lay"])
        if len(te) < 5:
            continue
        sc = StandardScaler()
        Xtr = sc.fit_transform(tr[feats].fillna(0.0))
        Xte = sc.transform(te[feats].fillna(0.0))
        m = make_model(); m.fit(Xtr, tr["target"].values)
        p = m.predict_proba(Xte)[:, 1]
        te = te.assign(p=p)
        te["ev"] = te["p"] * (1 - COMM) - (1 - te["p"]) * (te["odd_lay"] - 1)
        bet = te[te["ev"] > EV_MIN]
        for _, b in bet.iterrows():
            pnl = (1 - COMM) if int(b["target"]) == 1 else -(b["odd_lay"] - 1)
            rows.append(dict(mes=str(mes), Date=b["d"], Home=b["Home"], Away=b["Away"],
                             odd_lay=round(float(b["odd_lay"]), 2), p=round(float(b["p"]), 4),
                             ev=round(float(b["ev"]), 4), target=int(b["target"]), pnl=round(float(pnl), 3)))
        print(f"  {mes}: treino={len(tr)} teste={len(te)} apostas={len(bet)}", flush=True)
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-only", action="store_true", help="roda só a auditoria de truncamento")
    args = ap.parse_args()

    print("Carregando b365_base_lean.csv + construindo features...")
    raw = pd.read_csv(LEAN, low_memory=False)
    feat, feats = build_features(raw)

    print("\n=== AUDITORIA DE TRUNCAMENTO (teste de look-ahead) ===")
    ok = truncation_audit(raw, feats)
    if not ok:
        print("!! Auditoria FALHOU — abortando (não confiar em ROI de pipeline que vaza).")
        sys.exit(1)
    if args.audit_only:
        print("\n--audit-only: auditoria PASSOU. Encerrando sem walk-forward.")
        return

    lay_de, bf_min, bf_max = load_betfair_matcher()
    print(f"[betfair] janela de odds: {bf_min} .. {bf_max}")

    print("\n=== WALK-FORWARD (XGBoost + isotônica as-of, odd lay real) ===")
    bets = walk_forward(feat, feats, lay_de, bf_min, bf_max)
    if bets.empty:
        print("Sem apostas geradas."); return
    out = Path(__file__).resolve().parent / "wf_0x1_bets.csv"
    bets.to_csv(out, index=False)

    win = bets[bets["mes"] >= WINDOW_START].copy()
    print(f"\n=== VEREDITO (janela confiável {WINDOW_START}+, limiar FDR={LIM_FDR:.5f}) ===")
    verd = [report(win, "0x1 todas")]
    for lo_, hi_ in [(6.0, 9.5), (6.6, 13.2), (9.5, 13.2), (13.2, 18.0)]:
        verd.append(report(win[(win["odd_lay"] >= lo_) & (win["odd_lay"] <= hi_)], f"0x1 odd {lo_}-{hi_}"))
    pd.DataFrame([v for v in verd if v]).to_csv(Path(__file__).resolve().parent / "wf_0x1_verdict.csv", index=False)
    print(f"\nSalvos: {out.name}, wf_0x1_verdict.csv")
    print("Regra: PASSA só se ROI>0, IC95 exclui zero E p<=limiar FDR, estável H1/H2.")


if __name__ == "__main__":
    main()
