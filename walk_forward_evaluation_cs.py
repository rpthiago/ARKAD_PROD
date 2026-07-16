"""
walk_forward_evaluation_cs.py — Crivo estatístico HONESTO de LAY de placar correto (CS).
================================================================================
Generaliza o walk_forward_evaluation_0x1 para QUALQUER placar via --scoreline, e usa um
SNAPSHOT Betfair CONGELADO (betfair_snapshot.csv) em vez de download ao vivo — assim runs
diferentes (e máquinas diferentes) dão resultado idêntico byte-a-byte.

Protocolo honesto (idêntico ao do projeto):
  1. Features leak-free (features_builder_0x1.build_features, parametrizado por scoreline).
  2. Auditoria de truncamento (prova de ausência de look-ahead) antes de qualquer número.
  3. Walk-forward OOS estrito: warmup 12m, retreino mês a mês, prevê só o mês seguinte.
  4. XGBoost + calibração isotônica as-of (dentro do treino, nunca prefit global).
  5. Odd lay REAL Betfair casada por jogo (matcher lay_de) a partir do SNAPSHOT congelado.
  6. EV lay verdadeiro; recorte ago/2025+ (janela de odd confiável); bootstrap 20k; FDR.

Uso:
    python walk_forward_evaluation_cs.py --scoreline 0x0
    python walk_forward_evaluation_cs.py --scoreline 0x1 --audit-only
    python walk_forward_evaluation_cs.py --refresh-snapshot   # re-baixa e re-congela
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import io
import re
import sys
import unicodedata
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

HERE = Path(__file__).resolve().parent
DASH = Path(r"c:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1")
sys.path.insert(0, str(DASH))
sys.path.insert(0, str(HERE))
from features_builder_0x1 import build_features  # noqa: E402

LEAN = DASH / "b365_base_lean.csv"
FULL = DASH / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"   # base completa (238k jogos)
SNAPSHOT = HERE / "betfair_snapshot.csv"     # snapshot CONGELADO, versionável

COMM = 0.05
WARMUP = 12
EV_MIN = 0.02
WINDOW_START = "2025-08"
NB = 20_000
M_HYP = 45
LIM_FDR = 0.05 / M_HYP
RNG = np.random.default_rng(42)

# Bandas de odd reportadas por placar (0x0 concentra edge em odd>=10; 0x1 em faixas altas)
SLICES = {
    "0x0": [(6.0, 10.0), (10.0, 16.0), (10.0, 99.0)],
    "default": [(6.0, 9.5), (9.5, 13.2), (13.2, 18.0)],
}


def canon(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


# ── Snapshot Betfair CONGELADO (baixa uma vez, guarda TODAS as Odd_CS_*_Lay) ──
def ensure_snapshot(refresh: bool = False) -> pd.DataFrame:
    if SNAPSHOT.exists() and not refresh:
        bf = pd.read_csv(SNAPSHOT, low_memory=False)
    else:
        import requests
        from config import API_BETFAIR_BASE_URL, API_HEADERS
        print("[snapshot] baixando base Betfair (uma vez)...")
        r = requests.get(API_BETFAIR_BASE_URL, headers=dict(API_HEADERS), timeout=180)
        full = pd.read_csv(io.StringIO(r.text), low_memory=False)
        lay_cols = [c for c in full.columns if re.match(r"Odd_CS_\d+x\d+_Lay$", c)]
        keep = ["Date", "Home", "Away"] + lay_cols
        bf = full[[c for c in keep if c in full.columns]].copy()
        bf.to_csv(SNAPSHOT, index=False)
        print(f"[snapshot] congelado: {len(bf)} jogos, {len(lay_cols)} colunas de odd lay")
    h = hashlib.md5(pd.util.hash_pandas_object(bf, index=True).values.tobytes()).hexdigest()[:12]
    dts = pd.to_datetime(bf["Date"], errors="coerce")
    print(f"[snapshot] {SNAPSHOT.name} | {len(bf)} jogos | {dts.min().date()}..{dts.max().date()} | md5={h}")
    return bf


def build_matcher(bf: pd.DataFrame, lay_col: str):
    if lay_col not in bf.columns:
        raise SystemExit(f"Snapshot não tem a coluna {lay_col}. Rode --refresh-snapshot.")
    bf = bf.copy()
    bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
    bf["d"] = bf["Date"].dt.strftime("%Y-%m-%d")
    bf["ch"] = bf["Home"].map(canon)
    bf["ca"] = bf["Away"].map(canon)
    exact, byday = {}, {}
    for i, row in bf.iterrows():
        exact[(row["d"], row["ch"], row["ca"])] = i
        byday.setdefault(row["d"], []).append(i)
    bf_min, bf_max = bf["d"].min(), bf["d"].max()

    def lay_de(d, home, away):
        ch, ca = canon(home), canon(away)
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
        v = pd.to_numeric(bf.at[idx, lay_col], errors="coerce")
        return v if (pd.notna(v) and v > 1) else np.nan

    return lay_de, bf_min, bf_max


def make_model():
    from sklearn.calibration import CalibratedClassifierCV
    try:
        import xgboost as xgb
        base = xgb.XGBClassifier(n_estimators=250, max_depth=4, learning_rate=0.05, subsample=0.8,
                                 colsample_bytree=0.8, reg_lambda=1.0, tree_method="hist",
                                 random_state=42, verbosity=0, eval_metric="logloss")
        make_model.name = "XGBoost"
    except Exception:
        from sklearn.ensemble import RandomForestClassifier
        base = RandomForestClassifier(n_estimators=300, max_depth=8, min_samples_leaf=20,
                                      class_weight="balanced", random_state=42, n_jobs=-1)
        make_model.name = "RandomForest(fallback)"
    return CalibratedClassifierCV(base, cv=3, method="isotonic")


def truncation_audit(raw, feats, builder, probe_date="2026-03-15") -> bool:
    D = pd.Timestamp(probe_date)
    full, cf = builder(raw, verbose=False)
    trunc, ct = builder(raw[pd.to_datetime(raw["Date"], errors="coerce") <= D].copy(), verbose=False)
    common = [c for c in feats if c in cf and c in ct]
    a = full[pd.to_datetime(full["Date"]) == D].sort_values(["Home", "Away"])
    b = trunc[pd.to_datetime(trunc["Date"]) == D].sort_values(["Home", "Away"])
    if len(a) == 0 or len(a) != len(b):
        print(f"[AUDITORIA] sem jogos comparáveis em {D.date()}"); return False
    A = a[common].to_numpy(float); B = b[common].to_numpy(float)
    same = np.allclose(A, B, equal_nan=True)
    print(f"[AUDITORIA truncamento] {D.date()}: {len(a)} jogos, {len(common)} features | "
          f"{'PASS (sem look-ahead)' if same else 'FAIL — divergencia detectada'}")
    if not same:
        diff = np.nanmax(np.abs(A - B), axis=0)
        piores = sorted(zip(common, diff), key=lambda x: -x[1])[:6]
        print("  maior |diff| por feature:", [(c, round(float(d), 5)) for c, d in piores])
    return same


def bootstrap_month(bets):
    meses = sorted(bets["mes"].unique())
    if len(meses) < 2:
        return np.nan, np.nan, np.nan
    pnl_m = bets.groupby("mes")["pnl"].sum().reindex(meses).to_numpy()
    n_m = bets.groupby("mes").size().reindex(meses).to_numpy()
    idx = RNG.integers(0, len(meses), size=(NB, len(meses)))
    roi = pnl_m[idx].sum(axis=1) / np.maximum(n_m[idx].sum(axis=1), 1)
    lo, hi = np.percentile(roi, [2.5, 97.5])
    return float(lo), float(hi), float((roi <= 0).mean())


def report(bets, label):
    if len(bets) == 0:
        print(f"  {label:<20}: 0 apostas"); return None
    meses = sorted(bets["mes"].unique()); h = len(meses) // 2
    r1 = bets[bets["mes"].isin(meses[:h])]; r2 = bets[bets["mes"].isin(meses[h:])]
    roi = bets["pnl"].sum() / len(bets); wr = bets["target"].mean(); om = bets["odd_lay"].median()
    be = (om - 1) / ((om - 1) + (1 - COMM))
    lo, hi, p = bootstrap_month(bets)
    mm = bets.groupby("mes")["pnl"].agg(["size", "sum"]); mm["roi"] = mm["sum"] / mm["size"]
    fdr = "PASSA" if (p <= LIM_FDR and lo > 0) else "reprova"
    print(f"  {label:<20}: n={len(bets):>4} odd={om:>4.1f} WR={wr:>6.2%} BE={be:>6.2%} "
          f"ROI={roi:>+7.2%} IC95=[{lo:>+6.2%},{hi:>+6.2%}] p={p:.4f} FDR={fdr} "
          f"m+={int((mm['roi']>0).sum())}/{len(mm)} H1={r1['pnl'].sum()/max(len(r1),1):+.1%} "
          f"H2={r2['pnl'].sum()/max(len(r2),1):+.1%}")
    return dict(fatia=label, n=len(bets), odd=round(om, 1), wr=round(wr, 4), be=round(be, 4),
                roi=round(roi, 4), ic_lo=round(lo, 4), ic_hi=round(hi, 4), p=round(p, 5), fdr=fdr)


def walk_forward(feat, feats, lay_de, bf_min, bf_max, ctx=False):
    from sklearn.preprocessing import StandardScaler
    feat = feat.dropna(subset=feats + ["target"]).copy()
    feat["Date"] = pd.to_datetime(feat["Date"], errors="coerce")
    feat["_month"] = feat["Date"].dt.to_period("M")
    feat["d"] = feat["Date"].dt.strftime("%Y-%m-%d")
    rows = []
    for mes in sorted(feat["_month"].unique())[WARMUP:]:
        tr = feat[feat["_month"] < mes]
        te = feat[(feat["_month"] == mes) & (feat["d"] >= bf_min) & (feat["d"] <= bf_max)].copy()
        if len(tr) < 400 or len(te) < 5 or tr["target"].nunique() < 2:
            continue
        te["odd_lay"] = [lay_de(d, h, a) for d, h, a in zip(te["d"], te["Home"], te["Away"])]
        te = te.dropna(subset=["odd_lay"])
        # Filtros de contexto da produção do 0x0: corta ligas defensivas / mercado caro / odd baixa
        if ctx:
            if "liga_0x0_rate" in te.columns:
                te = te[te["liga_0x0_rate"] < 0.12]
            if "mkt_prob_0x0" in te.columns:
                te = te[te["mkt_prob_0x0"] < 0.10]
            te = te[te["odd_lay"] >= 10.0]
        if len(te) < 5:
            continue
        sc = StandardScaler(); Xtr = sc.fit_transform(tr[feats].fillna(0.0)); Xte = sc.transform(te[feats].fillna(0.0))
        m = make_model(); m.fit(Xtr, tr["target"].values)
        te = te.assign(p=m.predict_proba(Xte)[:, 1])
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
    ap.add_argument("--scoreline", default="0x0", help="placar do LAY, ex: 0x0, 0x1, 1x0")
    ap.add_argument("--features", default="generic", choices=["generic", "prod"],
                    help="'generic' = features_builder_0x1 (CS-genérico); 'prod' = builder de produção do 0x0")
    ap.add_argument("--audit-only", action="store_true")
    ap.add_argument("--skip-audit-abort", action="store_true",
                    help="roda o walk-forward mesmo se a auditoria acusar divergencia (p/ diagnostico)")
    ap.add_argument("--refresh-snapshot", action="store_true", help="re-baixa e re-congela o snapshot")
    ap.add_argument("--base", default="lean", choices=["lean", "full"], help="base b365: lean (158k) ou full (238k)")
    ap.add_argument("--context-filters", action="store_true",
                    help="aplica filtros de produção 0x0: liga_0x0_rate<0.12, mkt_prob_0x0<0.10, odd>=10")
    args = ap.parse_args()

    m = re.fullmatch(r"(\d+)x(\d+)", args.scoreline.strip())
    if not m:
        raise SystemExit("--scoreline inválido (use ex: 0x0)")
    scoreline = (int(m.group(1)), int(m.group(2))); sc = f"{scoreline[0]}x{scoreline[1]}"
    tag = f"{sc}_{args.features}_{args.base}" + ("_ctx" if args.context_filters else "")
    print(f"=== LAY {sc} | features={args.features} | base={args.base} | "
          f"ctx_filters={args.context_filters} | snapshot congelado ===")

    # Seleção do builder de features (mesmo harness, isola o efeito das features)
    if args.features == "prod":
        if sc != "0x0":
            raise SystemExit("--features prod só implementado para 0x0 (treinar_lay_0x0_rf_v2).")
        import treinar_lay_0x0_rf_v2 as T0x0
        def builder(df, verbose=True):
            return T0x0.build_features(df, f"Odd_CS_{sc}")
    else:
        def builder(df, verbose=True):
            return build_features(df, scoreline=scoreline, verbose=verbose)

    base_path = FULL if args.base == "full" else LEAN
    print(f"[base] {base_path.name}")
    raw = pd.read_csv(base_path, low_memory=False)
    feat, feats = builder(raw)

    print("\n=== AUDITORIA DE TRUNCAMENTO ===")
    audit_ok = truncation_audit(raw, feats, builder)
    if not audit_ok and not args.skip_audit_abort:
        print("!! Auditoria FALHOU — abortando (use --skip-audit-abort p/ diagnostico)."); sys.exit(1)
    if not audit_ok:
        print("!! PROSSEGUINDO apesar da divergencia (--skip-audit-abort) — resultado p/ diagnostico.")
    if args.audit_only:
        print("--audit-only: PASSOU."); return

    bf = ensure_snapshot(refresh=args.refresh_snapshot)
    lay_de, bf_min, bf_max = build_matcher(bf, f"Odd_CS_{sc}_Lay")
    print(f"[snapshot] janela de odds: {bf_min}..{bf_max}")

    print(f"\n=== WALK-FORWARD (modelo carregado sob demanda; odd lay real {sc}) ===")
    bets = walk_forward(feat, feats, lay_de, bf_min, bf_max, ctx=args.context_filters)
    print(f"[modelo] classificador usado: {getattr(make_model, 'name', '?')}")
    if bets.empty:
        print("Sem apostas."); return
    bets.to_csv(HERE / f"wf_{tag}_bets.csv", index=False)

    win = bets[bets["mes"] >= WINDOW_START].copy()
    print(f"\n=== VEREDITO LAY {sc} (features={args.features}, janela {WINDOW_START}+, limiar FDR={LIM_FDR:.5f}) ===")
    verd = [report(win, f"{sc} todas")]
    for lo_, hi_ in SLICES.get(sc, SLICES["default"]):
        verd.append(report(win[(win["odd_lay"] >= lo_) & (win["odd_lay"] <= hi_)], f"{sc} odd {lo_:g}-{hi_:g}"))
    pd.DataFrame([v for v in verd if v]).to_csv(HERE / f"wf_{tag}_verdict.csv", index=False)
    print(f"\nSalvos: wf_{tag}_bets.csv, wf_{tag}_verdict.csv | snapshot: {SNAPSHOT.name}")
    print("PASSA só se ROI>0, IC95 exclui zero E p<=limiar FDR, estável H1/H2.")


if __name__ == "__main__":
    main()
