# -*- coding: utf-8 -*-
"""
medir_forward_lay0x0.py — mede o FORWARD da regra congelada do Lay 0x0.

Le todas as planilhas 'sinais_lay0x0_gestao_*.xlsx' de paper_traning_lay0x0/, aplica a
REGRA CONGELADA (pre-registro 2026-08-02) e cospe ROI / IC95 (bootstrap por mes) vs o
criterio pre-registrado. NAO re-otimiza nada — so mede.

REGRA CONGELADA:  XGBoost · liga_0x0_rate < 0.08 · mkt_prob_0x0 < 0.10 · odd >= 10 · ev>0.02
CRITERIO FORWARD: >= 300 apostas liquidadas (ou >=4 meses) E piso do IC95 > +2%  => APROVA.

Uso:  python medir_forward_lay0x0.py
Precisa: as planilhas precisam ter as colunas 'liga_0x0_rate' e 'mkt_prob_0x0' (logger
augmentado em 2026-08-02) e 'Resultado' preenchido (GREEN/RED).
"""
import glob, os, re
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PASTA = os.path.join(HERE, "paper_traning_lay0x0")

# --- parametros CONGELADOS (nao mexer sem invalidar o pre-registro) ---
COMM = 0.05
LIGA_MAX = 0.08
MKT_MAX  = 0.10
ODD_MIN  = 10.0
FORWARD_START = "2026-08-03"   # so conta jogos a partir daqui (nao contamina com retro)
N_MIN = 300
IC_FLOOR = 0.02
NB = 20_000
RNG = np.random.default_rng(42)


def _col(df, *cands):
    """acha a 1a coluna existente entre candidatos (case/acento-insensitive)."""
    norm = {re.sub(r"[^a-z0-9]", "", c.lower()): c for c in df.columns}
    for cand in cands:
        k = re.sub(r"[^a-z0-9]", "", cand.lower())
        if k in norm:
            return norm[k]
    return None


def carregar():
    arqs = sorted(glob.glob(os.path.join(PASTA, "sinais_lay0x0_gestao_*.xlsx")))
    if not arqs:
        print(f"Nenhuma planilha em {PASTA}"); return pd.DataFrame()
    frames = []
    for a in arqs:
        try:
            d = pd.read_excel(a)
        except Exception as e:
            print(f"  [pulei {os.path.basename(a)}: {e}]"); continue
        c_data = _col(d, "Data")
        c_odd  = _col(d, "Odd Lay Betfair", "odd_lay_entrada", "Odd")
        c_liga = _col(d, "liga_0x0_rate")
        c_mkt  = _col(d, "mkt_prob_0x0")
        c_res  = _col(d, "Resultado")
        if not all([c_data, c_odd, c_res]):
            continue
        out = pd.DataFrame({
            "Data": pd.to_datetime(d[c_data], errors="coerce").dt.strftime("%Y-%m-%d"),
            "odd_lay": pd.to_numeric(d[c_odd], errors="coerce"),
            "liga_0x0_rate": pd.to_numeric(d[c_liga], errors="coerce") if c_liga else np.nan,
            "mkt_prob_0x0":  pd.to_numeric(d[c_mkt], errors="coerce") if c_mkt else np.nan,
            "Resultado": d[c_res].astype(str).str.upper().str.strip(),
        })
        frames.append(out)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates()
    df["mes"] = df["Data"].str[:7]
    # green=1 (nao saiu 0-0) ; red=0 (saiu 0-0) ; resto = pendente
    df["res"] = np.where(df["Resultado"].str.contains("GREEN"), 1,
                np.where(df["Resultado"].str.contains("RED"), 0, -1))
    return df


def bootstrap_mes(bets):
    meses = sorted(bets["mes"].unique())
    if len(meses) < 2:  # forward jovem: cai p/ bootstrap por jogo (provisorio)
        pnl = bets["pnl"].to_numpy()
        idx = RNG.integers(0, len(pnl), size=(NB, len(pnl)))
        r = pnl[idx].mean(1)
        return float(np.percentile(r, 2.5)), float(np.percentile(r, 97.5)), float((r <= 0).mean()), "por-jogo(provisorio)"
    pnl_m = bets.groupby("mes")["pnl"].sum().reindex(meses).to_numpy()
    n_m = bets.groupby("mes").size().reindex(meses).to_numpy()
    idx = RNG.integers(0, len(meses), size=(NB, len(meses)))
    r = pnl_m[idx].sum(1) / np.maximum(n_m[idx].sum(1), 1)
    return float(np.percentile(r, 2.5)), float(np.percentile(r, 97.5)), float((r <= 0).mean()), "por-mes"


def main():
    df = carregar()
    if df.empty:
        print("Sem dados."); return
    total = len(df)
    fwd = df[df["Data"] >= FORWARD_START].copy()
    # aplica a REGRA CONGELADA
    regra = fwd[(fwd["odd_lay"] >= ODD_MIN) &
                (fwd["liga_0x0_rate"] < LIGA_MAX) &
                (fwd["mkt_prob_0x0"] < MKT_MAX)].copy()
    liq = regra[regra["res"].isin([0, 1])].copy()   # liquidadas
    pend = int((regra["res"] == -1).sum())

    print("=" * 66)
    print("FORWARD — Lay 0x0 regra congelada (liga<0.08 & mkt<0.10 & odd>=10)")
    print("=" * 66)
    print(f"linhas lidas: {total} | forward (>= {FORWARD_START}): {len(fwd)} | "
          f"passam na regra: {len(regra)} | liquidadas: {len(liq)} | pendentes: {pend}")
    faltam_taxa = int(fwd["liga_0x0_rate"].isna().sum())
    if faltam_taxa:
        print(f"  [aviso] {faltam_taxa} sinais forward SEM liga_0x0_rate/mkt_prob_0x0 "
              f"(planilha antiga, pre-augmentacao) — excluidos da regra.")
    if liq.empty:
        print("\nAinda sem apostas liquidadas na regra. Preencha 'Resultado' (GREEN/RED) e rode de novo.")
        return

    liq["pnl"] = np.where(liq["res"] == 1, (1 - COMM), -(liq["odd_lay"] - 1))
    n = len(liq); reds = int((liq["res"] == 0).sum())
    roi = liq["pnl"].mean(); wr = liq["res"].mean(); om = liq["odd_lay"].median()
    be = (om - 1) / ((om - 1) + (1 - COMM))
    lo, hi, p, metodo = bootstrap_mes(liq)

    print(f"\napostas liquidadas: {n} | 0-0 (reds): {reds} | WR: {wr:.1%} | BE: {be:.1%} | odd med: {om:.1f}")
    print(f"ROI forward: {roi:+.2%}")
    print(f"IC95 ({metodo}): [{lo:+.2%}, {hi:+.2%}] | p(ROI<=0): {p:.4f}")

    # veredito vs criterio pre-registrado
    print("\n--- VEREDITO vs criterio pre-registrado (N>=300 E piso IC95 > +2%) ---")
    tem_n = n >= N_MIN
    tem_ic = lo > IC_FLOOR
    if tem_n and tem_ic:
        print(f"  >>> APROVA: N={n} (>=300) e piso IC95={lo:+.2%} (> +2%).")
    elif not tem_n:
        print(f"  >>> AINDA IMATURO: faltam {N_MIN - n} apostas p/ decidir (tem {n}/{N_MIN}).")
        print(f"      (parcial: piso IC95={lo:+.2%} {'ja>+2%' if tem_ic else 'ainda<=+2%'})")
    else:
        print(f"  >>> REPROVA (por ora): N={n} ok, mas piso IC95={lo:+.2%} <= +2%.")

    # por mes
    mm = liq.groupby("mes").agg(apostas=("pnl", "size"), reds=("res", lambda s: int((s == 0).sum())),
                                roi=("pnl", "mean"))
    mm["roi"] = (mm["roi"] * 100).round(1)
    print("\npor mes:")
    print(mm.rename(columns={"roi": "roi_%"}).to_string())
    liq.to_csv(os.path.join(HERE, "forward_lay0x0_liquidadas.csv"), index=False)
    print("\nsalvo: forward_lay0x0_liquidadas.csv")


if __name__ == "__main__":
    main()
