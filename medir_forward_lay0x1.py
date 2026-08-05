# -*- coding: utf-8 -*-
"""
medir_forward_lay0x1.py — mede o paper do Lay 0x1 (WR / ROI / IC95 bootstrap por mes).

Le 'sinais_lay0x1_*.xlsx' de paper_trading_lay0x1/. Deriva o resultado da coluna
'Resultado' (GREEN/RED) ou, se em branco, do 'Placar Final' (RED = 0-1). So MEDE.

*** AVISO HONESTO ***  O Lay 0x1 NAO e um metodo validado. O harness honesto ja o
reprovou hold-to-settlement (OOS: -6.7%/-3.2% odd baixa; +1.2%/-27.7% odd alta,
dependente de fonte). Isto aqui e acompanhamento descritivo do paper, NAO um selo de
aprovacao. Um mes verde na odd ~16 (onde 1 red apaga ~15 greens) esta dentro do ruido.
A carteira validada continua sendo SO o Lay 0x0.

Uso:  python medir_forward_lay0x1.py                 # mede (read-only)
      python medir_forward_lay0x1.py --preencher     # grava GREEN/RED do placar nos xlsx
      python medir_forward_lay0x1.py --odd-min 10 --odd-max 20   # recorta faixa de odd
"""
import argparse, glob, os, re
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PASTA = os.path.join(HERE, "paper_trading_lay0x1")
COMM = 0.05
NB = 20_000
RNG = np.random.default_rng(42)


def _col(df, *cands):
    norm = {re.sub(r"[^a-z0-9]", "", c.lower()): c for c in df.columns}
    for cand in cands:
        k = re.sub(r"[^a-z0-9]", "", cand.lower())
        if k in norm:
            return norm[k]
    return None


def _placar(p):
    if pd.isna(p):
        return None
    nums = re.findall(r"\d+", str(p))
    return (int(nums[0]), int(nums[1])) if len(nums) >= 2 else None


def _res_de_placar(p):
    r = _placar(p)
    if r is None:
        return ""
    return "RED" if r == (0, 1) else "GREEN"


def carregar(preencher=False):
    arqs = sorted(glob.glob(os.path.join(PASTA, "sinais_lay0x1_*.xlsx")))
    if not arqs:
        print(f"Nenhuma planilha em {PASTA}"); return pd.DataFrame()
    frames = []
    for a in arqs:
        try:
            d = pd.read_excel(a)
        except Exception as e:
            print(f"  [pulei {os.path.basename(a)}: {e}]"); continue
        c_data = _col(d, "Data")
        c_odd = _col(d, "Odd Lay Betfair", "Odd_Lay", "Odd")
        c_res = _col(d, "Resultado")
        c_pl = _col(d, "Placar Final", "Placar_Final")
        c_est = _col(d, "Estratégia", "Metodo")
        c_home = _col(d, "Mandante", "Home")
        c_away = _col(d, "Visitante", "Away")
        if not (c_data and c_odd):
            continue
        res = d[c_res].astype(str).str.upper().str.strip() if c_res else pd.Series([""] * len(d))
        # deriva do placar onde Resultado esta em branco
        if c_pl:
            faltando = ~res.str.contains("GREEN|RED", na=False)
            derivado = d[c_pl].apply(_res_de_placar)
            res = res.where(~faltando, derivado)
            if preencher and c_res and faltando.any():
                d[c_res] = res.values
                d.to_excel(a, index=False)
        frames.append(pd.DataFrame({
            "Data": pd.to_datetime(d[c_data], errors="coerce").dt.strftime("%Y-%m-%d"),
            "Mandante": d[c_home].astype(str) if c_home else "",
            "Visitante": d[c_away].astype(str) if c_away else "",
            "odd_lay": pd.to_numeric(d[c_odd], errors="coerce"),
            "Estrategia": d[c_est].astype(str) if c_est else "",
            "Resultado": res.astype(str),
        }))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["Data", "Mandante", "Visitante"])
    df["mes"] = df["Data"].str[:7]
    df["res"] = np.where(df["Resultado"].str.contains("GREEN"), 1,
                np.where(df["Resultado"].str.contains("RED"), 0, -1))
    return df


def carregar_ledger(path):
    """le o ledger vivo (coleta_lay0x1_aovivo.xlsx do DASHBOARD) — schema diferente."""
    d = pd.read_excel(path, sheet_name=0)
    c_data = _col(d, "Date", "Data")
    c_odd = _col(d, "Odd_lay_entrada", "Odd Lay Betfair", "Odd")
    c_pl = _col(d, "Placar_final", "Placar Final")
    c_home = _col(d, "Mandante", "Home")
    c_away = _col(d, "Visitante", "Away")
    c_est = _col(d, "Metodo", "Estratégia")
    res = d[c_pl].apply(_res_de_placar) if c_pl else pd.Series([""] * len(d))
    out = pd.DataFrame({
        "Data": pd.to_datetime(d[c_data], errors="coerce").dt.strftime("%Y-%m-%d"),
        "Mandante": d[c_home].astype(str) if c_home else "",
        "Visitante": d[c_away].astype(str) if c_away else "",
        "odd_lay": pd.to_numeric(d[c_odd], errors="coerce"),
        "Estrategia": d[c_est].astype(str) if c_est else "",
        "Resultado": res.astype(str),
    }).drop_duplicates(subset=["Data", "Mandante", "Visitante"])
    out["mes"] = out["Data"].str[:7]
    out["res"] = np.where(out["Resultado"].str.contains("GREEN"), 1,
                 np.where(out["Resultado"].str.contains("RED"), 0, -1))
    return out


def bootstrap_mes(bets):
    meses = sorted(bets["mes"].unique())
    if len(meses) < 2:
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--preencher", action="store_true", help="grava GREEN/RED (do placar) nos xlsx")
    ap.add_argument("--odd-min", type=float, default=0.0)
    ap.add_argument("--odd-max", type=float, default=1e9)
    ap.add_argument("--ledger", default=None,
                    help="le direto um ledger unico (ex: coleta_lay0x1_aovivo.xlsx do DASHBOARD)")
    args = ap.parse_args()

    if args.ledger:
        print(f"[fonte: ledger {os.path.basename(args.ledger)}]")
        df = carregar_ledger(args.ledger)
    else:
        df = carregar(preencher=args.preencher)
    if df.empty:
        print("Sem dados."); return
    df = df[(df["odd_lay"] >= args.odd_min) & (df["odd_lay"] <= args.odd_max)]
    liq = df[df["res"].isin([0, 1])].copy()
    pend = int((df["res"] == -1).sum())

    print("=" * 66)
    print("PAPER — Lay 0x1  (descritivo; metodo NAO validado — ver aviso no topo)")
    print("=" * 66)
    print(f"linhas: {len(df)} | liquidadas: {len(liq)} | pendentes (sem resultado): {pend}")
    if args.preencher:
        print("  [--preencher] GREEN/RED derivado do placar foi gravado nos xlsx.")
    if liq.empty:
        print("\nSem apostas liquidadas. Preencha 'Placar Final' (ou 'Resultado') e rode de novo.")
        return

    liq["pnl"] = np.where(liq["res"] == 1, (1 - COMM), -(liq["odd_lay"] - 1))
    n = len(liq); reds = int((liq["res"] == 0).sum())
    roi = liq["pnl"].mean(); wr = liq["res"].mean(); om = liq["odd_lay"].median()
    be = (om - 1) / ((om - 1) + (1 - COMM))
    lo, hi, p, metodo = bootstrap_mes(liq)

    print(f"\napostas liquidadas: {n} | 0-1 (reds): {reds} | WR: {wr:.2%} | BE: {be:.2%} | odd med: {om:.1f}")
    print(f"margem vs break-even: {(wr - be) * 100:+.2f}pp")
    print(f"ROI (stake=1): {roi:+.2%}")
    print(f"IC95 ({metodo}): [{lo:+.2%}, {hi:+.2%}] | p(ROI<=0): {p:.4f}")
    if lo <= 0:
        print("  -> IC95 INCLUI o zero: nao da pra distinguir de variancia (esperado num paper novo).")

    mm = liq.groupby("mes").agg(apostas=("pnl", "size"), reds=("res", lambda s: int((s == 0).sum())),
                                roi=("pnl", "mean"))
    mm["roi"] = (mm["roi"] * 100).round(1)
    print("\npor mes:")
    print(mm.rename(columns={"roi": "roi_%"}).to_string())
    liq.to_csv(os.path.join(HERE, "forward_lay0x1_liquidadas.csv"), index=False)
    print("\nsalvo: forward_lay0x1_liquidadas.csv")
    print("\nLembrete honesto: 1 mes na odd ~16 nao valida nada. Para decidir de verdade:")
    print("  walk-forward OOS + bootstrap por mes + FDR, com odd lay REAL (nao back-derivada).")


if __name__ == "__main__":
    main()
