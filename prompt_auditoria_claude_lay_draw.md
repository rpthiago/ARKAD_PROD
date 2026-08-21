# PROMPT DE AUDITORIA RIGOROSA — MÉTODO LAY DRAW (ARKAD QUANT)

> **Instruções para envio ao Claude:** Copie e cole todo o conteúdo abaixo diretamente no Claude (Claude 3.5 Sonnet ou Opus). O prompt foi estruturado com mentalidade adversária para encontrar **bugs, vazamentos de dados, look-ahead bias e discrepâncias entre o Backtest e os Sinais ao Vivo**.

---

```markdown
Você é um Auditor Sênior de Sistemas Quantitativos, Machine Learning e Engenharia Financeira de Alta Fidelidade (Betting Exchange & Betfair Markets).

Sua missão é realizar um **PENTEST / CODE AUDIT ADVERSARIAL** no método **Lay Draw (Aposta Contra o Empate)** do sistema ARKAD.
⚠️ **ATENÇÃO:** O seu papel NÃO É elogiar ou validar a lucratividade do método, mas sim **ENCONTRAR ERROS, VAZAMENTOS DE DADOS (DATA LEAKAGE), LOOK-AHEAD BIAS, FALHAS DE CÁLCULO E DIVERGÊNCIAS ENTRE O MOTOR DE BACKTEST E O MOTOR DE SINAIS AO VIVO**.

---

### 1. CONTEXTO DA ARQUITETURA DO SISTEMA:

O sistema possui dois fluxos que DEVEM ser 100% idênticos:
1. **Fluxo de Backtest Histórico:** Processa a base histórica (`Bases_de_Dados_API_FutPythonTrader_Bet365.csv`) gerando predições e métricas passadas.
2. **Fluxo de Sinais Diários ao Vivo (Streamlit):** Recebe o feed diário da Betfair Exchange API (`fetch_betfair_daily(date)`), extrai métricas históricas de suporte via `coleta_lay_cs_aovivo._hist_df()`, roda o modelo Random Forest (`lay_draw_rf_v2_strategy.py`) e entrega a grade diária em `pages/19_🤝_Sinais_Lay_Draw.py`.

Recentemente, foram observadas divergências de jogos selecionados, variações de probabilidades e impactos em filtros de taxa de empates da liga.

---

### 2. CÓDIGO FONTE DO MOTOR CENTRAL (`lay_draw_rf_v2_strategy.py`):

```python
"""lay_draw_rf_v2_strategy.py — Lay Draw v2 | ARKAD PROD"""
import os, pandas as pd, numpy as np, joblib
import unicodedata, re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_draw_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_draw_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_draw_rf_v2.pkl")

COMMISSION        = 0.05
EV_MIN            = 0.03
PROB_MIN          = 0.88        # Padrão Sniper ARKAD: Probabilidade mínima 88.0%
ODD_MIN           = 3.20        # Faixa Sweet Spot de Odds (3.20 a 4.20)
ODD_MAX           = 4.20        # Teto 4.20 para responsabilidade baixa
FAV_ODD_MAX       = 2.10        # Exige favorito claro (Mandante ou Visitante <= 2.10)
LIGA_DRAW_RATE_MAX = 0.36       # Filtro anti-ligas hiper-empatadoras (ex: >36% empates)


def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def _ev_lay(prob_not_draw, odd):
    return prob_not_draw * (1 - COMMISSION) - (1 - prob_not_draw) * (odd - 1)


def _decay_roll_grouped_unshifted(df, group_col, val_col, window=6, alpha=0.25):
    g = df.groupby(group_col)[val_col]
    numer = np.zeros(len(df)); count = np.zeros(len(df)); wsum = 0.0
    for j in range(window):
        sj = g.shift(j)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 3] = np.nan
    return pd.Series(res, index=df.index)


def check_entry_conditions(ms):
    odd = ms.get("Odd_D_FT", 0) or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    if prob < PROB_MIN:
        return False, f"PROB_BAIXA({prob*100:.1f}%)"
    
    odd_h = ms.get("Odd_H_FT", np.nan)
    odd_a = ms.get("Odd_A_FT", np.nan)
    if FAV_ODD_MAX is not None:
        tem_favorito = (pd.notna(odd_h) and 0 < odd_h <= FAV_ODD_MAX) or (pd.notna(odd_a) and 0 < odd_a <= FAV_ODD_MAX)
        if not tem_favorito:
            return False, "SEM_FAVORITO_CLARO"

    ev = _ev_lay(prob, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    liga_rate = ms.get("liga_draw_rate", None)
    if liga_rate is not None and pd.notna(liga_rate) and LIGA_DRAW_RATE_MAX > 0 and liga_rate > LIGA_DRAW_RATE_MAX:
        return False, f"LIGA_EMPATADORA({liga_rate:.2f})"
    return True, "APROVADO"


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    if df_historical is None or not isinstance(df_historical, pd.DataFrame) or df_historical.empty or "Date" not in df_historical.columns:
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist[df_hist["Date"] < "2026-08-01"].copy()
    df_hist = df_hist.dropna(subset=["Goals_H_FT","Goals_A_FT","Date","Home","Away"]).copy()
    df_hist = df_hist.sort_values("Date", kind="mergesort").reset_index(drop=True)

    stat_cols = ["Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_A_FT",
                 "xGOT_Faced_H_FT","xGOT_Faced_A_FT",
                 "Goals_Prevented_H_FT","Goals_Prevented_A_FT",
                 "Big_Chances_H_FT","Big_Chances_A_FT",
                 "Shots_On_Target_H_FT","Shots_On_Target_A_FT",
                 "Possession_H_FT","Possession_A_FT"]
    for c in stat_cols:
        df_hist[c] = pd.to_numeric(df_hist.get(c, 0), errors="coerce").fillna(0.0) if c in df_hist.columns else 0.0

    df_hist["_draw_flag"] = (df_hist["Goals_H_FT"] == df_hist["Goals_A_FT"]).astype(float)

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT","Possession_H_FT","_draw_flag"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={"Home":"Team"})
    dh = dh.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_H_FT","h_Gf"),("Goals_A_FT","h_Gc"),("xGOT_H_FT","h_xGOT"),
                    ("xGOT_Faced_H_FT","h_xGOT_faced"),("Goals_Prevented_H_FT","h_GP"),
                    ("Big_Chances_H_FT","h_BC"),("Shots_On_Target_H_FT","h_SoT"),
                    ("Possession_H_FT","h_Poss"),("won","h_WR"),("_draw_flag","h_draw_rate")]:
        dh[nm] = _decay_roll_grouped_unshifted(dh, "Team", col)
    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR","h_draw_rate"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT","Possession_A_FT","_draw_flag"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={"Away":"Team"})
    da = da.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_A_FT","a_Gf"),("Goals_H_FT","a_Gc"),("xGOT_A_FT","a_xGOT"),
                    ("xGOT_Faced_A_FT","a_xGOT_faced"),("Goals_Prevented_A_FT","a_GP"),
                    ("Big_Chances_A_FT","a_BC"),("Shots_On_Target_A_FT","a_SoT"),
                    ("Possession_A_FT","a_Poss"),("won","a_WR"),("_draw_flag","a_draw_rate")]:
        da[nm] = _decay_roll_grouped_unshifted(da, "Team", col)
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR","a_draw_rate"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # Liga draw rate
    df_lig = df_hist[["Date","League","_draw_flag"]].sort_values(["League","Date"], kind="mergesort").reset_index(drop=True)
    df_lig["liga_draw_rate"] = df_lig.groupby("League")["_draw_flag"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_last = df_lig.groupby("League")["liga_draw_rate"].last().to_dict()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        sh_df = home_last[home_last["Team"].map(_canon) == _canon(home)]
        sa_df = away_last[away_last["Team"].map(_canon) == _canon(away)]
        
        sh = sh_df.iloc[0] if not sh_df.empty else pd.Series(dtype=float)
        sa = sa_df.iloc[0] if not sa_df.empty else pd.Series(dtype=float)

        odd_d = pd.to_numeric(g.get("Odd_D_Lay") or g.get("Odd_D_FT") or g.get("Odd_D_Back") or g.get("Odd_D_FT_Back") or g.get("Odd_D") or np.nan, errors="coerce")
        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or g.get("Odd_H_Lay") or g.get("Odd_H") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or g.get("Odd_A_Lay") or g.get("Odd_A") or np.nan, errors="coerce")

        if pd.isna(odd_d) or odd_d <= 0:
            continue

        ms = {"Home": home, "Away": away, "League": league, "Date": date_v,
              "Time": g.get("Time", ""), "Odd_D_FT": odd_d,
              "Odd_H_FT": odd_h, "Odd_A_FT": odd_a}

        for col in h_feats:
            ms["H_" + col] = sh.get(col, np.nan) if not sh.empty else np.nan
        for col in a_feats:
            ms["A_" + col] = sa.get(col, np.nan) if not sa.empty else np.nan

        h_wr = ms.get("H_h_WR", 0) if pd.notna(ms.get("H_h_WR")) else 0.35
        a_wr = ms.get("A_a_WR", 0) if pd.notna(ms.get("A_a_WR")) else 0.25
        h_dr = ms.get("H_h_draw_rate", 0) if pd.notna(ms.get("H_h_draw_rate")) else 0.28
        a_dr = ms.get("A_a_draw_rate", 0) if pd.notna(ms.get("A_a_draw_rate")) else 0.28

        ms["total_WR"]        = h_wr + a_wr
        ms["wr_diff"]         = abs(h_wr - a_wr)
        ms["draw_rate_prod"]  = h_dr * a_dr
        ms["draw_rate_mean"]  = (h_dr + a_dr) / 2
        ms["total_xGOT"]      = (ms.get("H_h_xGOT",0) or 0) + (ms.get("A_a_xGOT",0) or 0)
        ms["xGOT_diff"]       = abs((ms.get("H_h_xGOT",0) or 0) - (ms.get("A_a_xGOT",0) or 0))
        ms["total_Gf"]        = (ms.get("H_h_Gf",0) or 0) + (ms.get("A_a_Gf",0) or 0)
        ms["gf_diff"]         = abs((ms.get("H_h_Gf",0) or 0) - (ms.get("A_a_Gf",0) or 0))
        ms["decisive_score"]  = ms["total_WR"] * ms["wr_diff"]

        ms["mkt_prob_draw"]     = 1.0 / odd_d if odd_d > 0 else np.nan
        ms["mkt_prob_draw_norm"] = np.nan
        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _ov = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_draw_norm"] = ms["mkt_prob_draw"] / _ov if _ov > 0 else np.nan
        ms["mkt_overvalue_draw"] = (ms["mkt_prob_draw"] - ms["draw_rate_mean"]
                                    if ms["mkt_prob_draw"] else np.nan)

        ms["liga_draw_rate"] = liga_last.get(league, np.nan)
        ms["h2h_draw_rate"]  = np.nan

        row_dict = {col: (0.0 if pd.isna(ms.get(col)) else ms.get(col)) for col in features}
        row_mat = pd.DataFrame([row_dict])
        
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_d)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        evaluated.append(ms)

    return evaluated
```

---

### 3. QUESTÕES E PONTOS CRÍTICOS QUE VOCÊ DEVE INVESTIGAR:

Por favor, responda com precisão cirúrgica e código corretivo para cada uma das perguntas abaixo:

1. **Vazamento de Dados / Look-Ahead Bias:**
   - Na função `_decay_roll_grouped_unshifted`, o shift inicial é zero (`sj = g.shift(0)`). Isso causa vazamento da partida atual para o passado durante o treino ou predição?
   - A linha `df_hist = df_hist[df_hist["Date"] < "2026-08-01"].copy()` é uma trava fixa "hardcoded". Ao operar em Setembro/Outubro de 2026, isso congelará o histórico? Como isso deve ser calculado dinamicamente em relação à data da partida (`date_v`)?

2. **Divergência entre Backtest e Sinais Diários:**
   - No Backtest Histórico tradicional (que roda em CSV), as odds utilizadas são Back ou Lay? No fluxo de sinais ao vivo, usamos `Odd_D_Lay`. Existe discrepância de spread?
   - Como o fallback de times desconhecidos (`h_wr = 0.35`, `a_wr = 0.25`, etc.) afeta o modelo? Isso gera falsos positivos de probabilidade alta (ex: 88% a 100%) quando a IA não conhece os times?

3. **Validação Matemática de EV e Gestão de Banca:**
   - A fórmula de EV utilizada: `_ev_lay(prob, odd) = prob * (1 - 0.05) - (1 - prob) * (odd - 1)` está matematicamente correta para o mercado Lay da Betfair?
   - A dedução da comissão de 5% sobre os ganhos e o cálculo de responsabilidade `(odd - 1) * stake` em caso de Red refletem 100% a liquidação da bolsa?

4. **Correções e Código Refatorado:**
   - Aponte todos os erros encontrados por ordem de gravidade (Crítico, Alto, Moderado, Baixo).
   - Forneça o script Python completo e definitivo corrigindo todas as falhas identificadas para garantir 100% de paridade entre o Backtest e os Sinais Diários sem nenhum vazamento de dados.
```
