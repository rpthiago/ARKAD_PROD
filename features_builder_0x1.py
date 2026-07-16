"""
features_builder_0x1.py — Extração de features LEAK-FREE para o método Lay 0x1.
================================================================================
Corrige os três vícios estruturais dos scripts antigos:
  (1) Venue-lock  -> forma calculada na TIMELINE REAL do time (long-format),
                     agnóstica a mando, e não em groupby("Home"/"Away").
  (2) Poluição de calendário -> stats sempre na ótica do time (gols que ELE fez),
                     nunca forçando jogo-fora a 0.0.
  (3) Vazamento temporal -> toda janela usa shift(1) (o próprio jogo NUNCA entra),
                     e janela incompleta vira NaN (nada de fillna com constante mágica).

Padrão herdado do lay_0x0_rf_v2 (único método do projeto que passa FDR).

Uso (Frente B / walk-forward):
    from features_builder_0x1 import build_features
    df_feat, feature_cols = build_features(df_raw)
    # df_feat tem: Date, Home, Away, League?, target, Odd_CS_0x1(_Lay), <features>
    # O caller faz dropna(subset=feature_cols + ["target"]) por mês e casa a odd lay real.

Regra de ouro: NUNCA fillna de feature com constante. Janela cheia ou a linha sai.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── Resolução flexível de colunas (casa/fora) ────────────────────────────────
# Cada stat lógico -> (candidatos_HOME, candidatos_AWAY). Usa o 1º que existir.
STAT_SPECS: dict[str, tuple[list[str], list[str]]] = {
    "gf":   (["Goals_H_FT"], ["Goals_A_FT"]),                       # gols marcados (ótica do time)
    "ga":   (["Goals_A_FT"], ["Goals_H_FT"]),                       # gols sofridos (espelho)
    "dang": (["DangerousAttacks_H", "Dang_H"], ["DangerousAttacks_A", "Dang_A"]),
    "xg":   (["xG_H", "xG_H_FT", "xGOT_H_FT"], ["xG_A", "xG_A_FT", "xGOT_A_FT"]),
    "sot":  (["Shots_On_Target_H", "SoT_H", "Shots_On_Target_H_FT"],
             ["Shots_On_Target_A", "SoT_A", "Shots_On_Target_A_FT"]),
    "bc":   (["Big_Chances_H", "Big_Chances_H_FT"], ["Big_Chances_A", "Big_Chances_A_FT"]),
}
ROLL_WINDOW = 5      # forma recente = últimos 5 jogos do time
ROLL_ALPHA = 0.25    # decaimento exponencial (jogo mais recente pesa mais)
H2H_WINDOW = 4       # últimos 4 confrontos diretos
H2H_MIN = 2


def _resolve(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _decay_roll(g: pd.DataFrame, group_col: str, val_col: str,
                window: int = ROLL_WINDOW, alpha: float = ROLL_ALPHA,
                min_g: int | None = None) -> pd.Series:
    """Média exponencialmente decaída sobre os `window` eventos ANTERIORES do grupo.
    shift(1+j) garante leak-free (o evento atual nunca entra). Janela incompleta -> NaN.
    Idêntico em espírito ao _decay_roll_grouped do lay_0x0_rf_v2 (validado byte-a-byte)."""
    min_g = window if min_g is None else min_g
    grp = g.groupby(group_col)[val_col]
    numer = np.zeros(len(g)); count = np.zeros(len(g)); wsum = 0.0
    for j in range(window):
        sj = grp.shift(1 + j)
        w = float(np.exp(-alpha * j))
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * w, 0.0)
        count += m
        wsum += w
    out = numer / wsum
    out[count < min_g] = np.nan
    return pd.Series(out, index=g.index)


def _build_team_timeline(df: pd.DataFrame, stats: dict[str, tuple[str, str]]) -> tuple[pd.DataFrame, list[str], list[str], list[str]]:
    """Derrete cada partida em 2 linhas (ótica de cada time) e computa a forma recente
    LEAK-FREE. Retorna (timeline, form_cols agnósticas, casa_cols, fora_cols).
    A forma de mando é atribuída SÓ às linhas do próprio mando (sem ffill cruzado, que
    vazava): o mandante usa casa_* (do seu jogo em casa), o visitante usa fora_*."""
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

    def side(is_home: bool) -> pd.DataFrame:
        base = {
            "_mid": df["_mid"],
            "Date": df["Date"],
            "team": df["Home"] if is_home else df["Away"],
            "is_home": 1.0 if is_home else 0.0,
        }
        for logical, (hcol, acol) in stats.items():
            base[logical] = df[hcol] if is_home else df[acol]
        t = pd.DataFrame(base)
        # sinais derivados por-jogo (ótica do time) — NUNCA forçados a 0 por mando
        t["won"] = (t["gf"] > t["ga"]).astype(float)
        t["fail_score"] = (t["gf"] == 0).astype(float)          # não marcou (chave p/ 0x1)
        t["conceded_one"] = (t["ga"] == 1).astype(float)        # sofreu exatamente 1
        return t

    tl = pd.concat([side(True), side(False)], ignore_index=True)
    tl = tl.sort_values(["team", "Date"], kind="mergesort").reset_index(drop=True)

    roll_cols = list(stats.keys()) + ["won", "fail_score", "conceded_one"]

    # (1) Forma AGNÓSTICA a mando: últimos N jogos reais do time
    form_cols: list[str] = []
    for c in roll_cols:
        name = f"form_{c}"
        tl[name] = _decay_roll(tl, "team", c)
        form_cols.append(name)

    # (2) Forma ESPECÍFICA de mando, atribuída SÓ às linhas do próprio mando (sem ffill).
    #     Assim H_casa_* vem do jogo-em-casa do mandante e A_fora_* do jogo-fora do
    #     visitante — cada uma computada no subconjunto certo, no próprio jogo (leak-free).
    casa_cols: list[str] = []
    fora_cols: list[str] = []
    venue_stats = [c for c in ["gf", "ga", "dang", "fail_score"] if c in tl.columns]
    for flag, tag, bucket in [(1.0, "casa", casa_cols), (0.0, "fora", fora_cols)]:
        sub = tl[tl["is_home"] == flag]
        for c in venue_stats:
            nm = f"{tag}_{c}"
            tl[nm] = np.nan
            tl.loc[sub.index, nm] = _decay_roll(sub, "team", c, window=3, min_g=2)
            bucket.append(nm)

    return tl, form_cols, casa_cols, fora_cols


def _build_h2h(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Confronto direto recente (leak-free): média sobre os confrontos ANTERIORES do par,
    independente de mando. Sinais simétricos relevantes a placar baixo (0x1 é low-score)."""
    d = df[["_mid", "Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]].copy()
    d["pair"] = [
        "|".join(sorted([str(h), str(a)])) for h, a in zip(d["Home"], d["Away"])
    ]
    d["tot_goals"] = d["Goals_H_FT"] + d["Goals_A_FT"]
    d["low_score"] = (d["tot_goals"] <= 1).astype(float)          # 0-0,0-1,1-0
    d["btts"] = ((d["Goals_H_FT"] > 0) & (d["Goals_A_FT"] > 0)).astype(float)
    d = d.sort_values(["pair", "Date"], kind="mergesort").reset_index(drop=True)
    out_cols = []
    for c, nm in [("tot_goals", "h2h_avg_goals"), ("low_score", "h2h_low_rate"), ("btts", "h2h_btts_rate")]:
        d[nm] = _decay_roll(d, "pair", c, window=H2H_WINDOW, alpha=0.15, min_g=H2H_MIN)
        out_cols.append(nm)
    return d[["_mid", *out_cols]], out_cols


def build_features(df: pd.DataFrame, scoreline: tuple[int, int] = (0, 1),
                   verbose: bool = True) -> tuple[pd.DataFrame, list[str]]:
    """Features CS leak-free para o LAY do placar `scoreline`=(h,a). Genérico:
    (0,1) -> Lay 0x1 | (0,0) -> Lay 0x0 | (1,0) -> Lay 1x0, etc.
    Retorna (df_com_features, feature_cols). As features de forma/mando/h2h são as mesmas
    (força dos times); só o ALVO e a coluna de odd mudam com o placar.
    df precisa de: Date, Home, Away, Goals_H_FT, Goals_A_FT (+ stats/odds opcionais)."""
    req = ["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"])
    df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
    df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")
    df = df.dropna(subset=["Goals_H_FT", "Goals_A_FT"])
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)
    df["_mid"] = df.index

    # Resolve quais stats existem na base
    stats: dict[str, tuple[str, str]] = {}
    for logical, (hc, ac) in STAT_SPECS.items():
        h = _resolve(df, hc); a = _resolve(df, ac)
        if h is not None and a is not None:
            df[h] = pd.to_numeric(df[h], errors="coerce")
            df[a] = pd.to_numeric(df[a], errors="coerce")
            stats[logical] = (h, a)
    if verbose:
        print(f"[features_builder_0x1] stats resolvidos: {sorted(stats.keys())}")
        skipped = [k for k in STAT_SPECS if k not in stats]
        if skipped:
            print(f"[features_builder_0x1] stats ausentes na base (pulados): {skipped}")

    # ── Timeline por time (forma recente leak-free) ──────────────────────────
    tl, form_cols, casa_cols, fora_cols = _build_team_timeline(df, stats)
    # Mandante leva forma agnóstica + forma-EM-CASA; visitante leva agnóstica + forma-FORA.
    h_feats = form_cols + casa_cols
    a_feats = form_cols + fora_cols
    H = tl[tl["is_home"] == 1.0][["_mid", *h_feats]].add_prefix("H_").rename(columns={"H__mid": "_mid"})
    A = tl[tl["is_home"] == 0.0][["_mid", *a_feats]].add_prefix("A_").rename(columns={"A__mid": "_mid"})
    feat = df.merge(H, on="_mid", how="left").merge(A, on="_mid", how="left")

    feature_cols: list[str] = [f"H_{c}" for c in h_feats] + [f"A_{c}" for c in a_feats]

    # ── H2H recente ──────────────────────────────────────────────────────────
    h2h, h2h_cols = _build_h2h(df)
    feat = feat.merge(h2h, on="_mid", how="left")
    feature_cols += h2h_cols

    # ── Derivadas específicas do 0x1 (0-1 = casa não marca, fora marca ~1) ────
    # Ataque da casa vs defesa do visitante; xG ponderado; falha de gol da casa.
    if "xg" in stats:
        feat["xG_ratio_H"] = feat["H_form_xg"] / (feat["A_form_ga"].abs() + 0.10)
        feat["xG_ratio_A"] = feat["A_form_xg"] / (feat["H_form_ga"].abs() + 0.10)
        feat["total_form_xg"] = feat["H_form_xg"] + feat["A_form_xg"]   # poder ofensivo combinado (chave 0x0)
        feature_cols += ["xG_ratio_H", "xG_ratio_A", "total_form_xg"]
    feat["form_gf_diff"] = feat["H_form_gf"] - feat["A_form_ga"]   # ataque casa − defesa fora
    feat["home_dry_signal"] = feat["H_form_fail_score"] * feat["A_form_won"]  # casa seca × fora vencendo
    feat["total_form_gf"] = feat["H_form_gf"] + feat["A_form_gf"]              # gols-acontecem (chave p/ 0x0)
    feat["total_dry"] = feat["H_form_fail_score"] + feat["A_form_fail_score"]  # ambos secos (0x0)
    feature_cols += ["form_gf_diff", "home_dry_signal", "total_form_gf", "total_dry"]

    # Mercado pré-jogo (leak-free por construção — é odd de abertura, não resultado)
    if _resolve(df, ["Odd_H_FT"]) and _resolve(df, ["Odd_A_FT"]):
        oh = pd.to_numeric(feat["Odd_H_FT"], errors="coerce")
        oa = pd.to_numeric(feat["Odd_A_FT"], errors="coerce")
        feat["Spread_Forca"] = 1.0 / oh - 1.0 / oa
        feature_cols.append("Spread_Forca")
        if "Odd_D_FT" in feat.columns:
            od = pd.to_numeric(feat["Odd_D_FT"], errors="coerce")
            ov = 1.0 / oh + 1.0 / od + 1.0 / oa
            feat["mkt_pH"] = (1.0 / oh) / ov
            feat["mkt_pA"] = (1.0 / oa) / ov
            feature_cols += ["mkt_pH", "mkt_pA"]

    # ── Alvo do LAY: green (=1) quando o placar NÃO é h-a ─────────────────────
    h, a = int(scoreline[0]), int(scoreline[1])
    sc = f"{h}x{a}"
    is_target_score = ((feat["Goals_H_FT"] == h) & (feat["Goals_A_FT"] == a)).astype(float)
    feat["target"] = (1 - is_target_score).astype(int)

    # Taxa recente do placar-alvo por LIGA (shift(1) leak-free) — sinal de contexto (0x0)
    if "League" in feat.columns:
        lg = feat[["_mid", "Date", "League"]].copy()
        lg["is_t"] = is_target_score.values
        lg = lg.sort_values(["League", "Date"], kind="mergesort")
        lg["liga_target_rate"] = lg.groupby("League")["is_t"].transform(
            lambda x: x.shift(1).rolling(100, min_periods=20).mean())
        feat = feat.merge(lg[["_mid", "liga_target_rate"]], on="_mid", how="left")
        feature_cols.append("liga_target_rate")

    # Prob. implícita do mercado no placar-alvo (odd de back, pré-jogo → leak-free)
    if _resolve(df, [f"Odd_CS_{sc}"]):
        odd_t = pd.to_numeric(feat[f"Odd_CS_{sc}"], errors="coerce")
        feat["mkt_prob_target"] = 1.0 / odd_t.where(odd_t > 1)
        feature_cols.append("mkt_prob_target")

    # Passa adiante a coluna de odd de sinal (a odd lay REAL é casada na Frente B via lay_de())
    odd_lay, odd_bk = f"Odd_CS_{sc}_Lay", f"Odd_CS_{sc}"
    odd_col = _resolve(df, [odd_lay, odd_bk])
    if odd_col and odd_col != odd_bk:
        feat[odd_bk] = pd.to_numeric(feat[odd_col], errors="coerce")

    keep = ["Date", "Home", "Away", "_mid", "target"]
    for extra in ["League", "Liga", "Metodo", odd_bk, odd_lay, "Odd_H_FT", "Odd_A_FT"]:
        if extra in feat.columns and extra not in keep:
            keep.append(extra)
    feature_cols = [c for i, c in enumerate(feature_cols) if c not in feature_cols[:i]]  # dedup, ordem estável
    feat = feat[keep + feature_cols].copy()

    if verbose:
        n_full = feat[feature_cols].notna().all(axis=1).sum()
        print(f"[features_builder_0x1] {len(feat)} jogos | {len(feature_cols)} features | "
              f"{n_full} com janela cheia ({n_full/len(feat):.0%})")
    return feat, feature_cols


if __name__ == "__main__":
    # Smoke test com dados sintéticos (verifica leak-free e ausência de fillna mágico).
    import sys
    rng = np.random.default_rng(0)
    n = 3000
    teams = [f"T{i}" for i in range(30)]
    rows = []
    base = pd.Timestamp("2024-01-01")
    for k in range(n):
        h, a = rng.choice(teams, 2, replace=False)
        rows.append(dict(
            Date=base + pd.Timedelta(days=int(k // 5)),
            Home=h, Away=a,
            Goals_H_FT=int(rng.poisson(1.3)), Goals_A_FT=int(rng.poisson(1.1)),
            DangerousAttacks_H=rng.integers(30, 90), DangerousAttacks_A=rng.integers(30, 90),
            xG_H=rng.gamma(2, 0.6), xG_A=rng.gamma(2, 0.5),
            Odd_H_FT=rng.uniform(1.5, 3.0), Odd_A_FT=rng.uniform(2.0, 5.0),
            Odd_CS_0x1_Lay=rng.uniform(6.0, 18.0),
        ))
    df = pd.DataFrame(rows)
    feat, cols = build_features(df)
    assert feat["target"].isin([0, 1]).all(), "target inválido"
    assert not feat[cols].isin([np.inf, -np.inf]).any().any(), "inf em feature (fillna mágico?)"
    print("OK — features:", cols)
    sys.exit(0)
