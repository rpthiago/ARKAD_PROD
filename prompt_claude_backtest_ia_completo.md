# PROMPT DE AUDITORIA E REPRODUÇÃO DO BACKTEST DE IA — LAY DRAW (CLAUDE)

> **Instruções para o usuário:** Copie todo o conteúdo abaixo e envie para o Claude. Ele contém o script completo e autônomo com o carregamento do modelo `.pkl` e o cálculo exato das 34 features por mando de campo.

---

```markdown
Você é um Auditor Independente de Machine Learning e Engenharia Financeira Quantitativa.

Sua tarefa é executar uma **REPRODUÇÃO AUTÔNOMA DO BACKTEST DE 2026 DO MÉTODO LAY DRAW (Random Forest v2)** sobre a base histórica oficial `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` e os artefatos `modelo_lay_draw_rf_v2.pkl`, `scaler_lay_draw_rf_v2.pkl` e `features_lay_draw_rf_v2.pkl`.

---

### 1. REGRAS DO MOTOR E CRITÉRIOS DE FILTRAGEM:

1. **Features por Mando (Venue-Specific):**
   - $H\_h\_*$: Forma, xGOT, gols e vitórias do time jogando estritamente em **Casa** (decaimento exponencial $\alpha=0.25$, janela 6, min 3 partidas passadas).
   - $A\_a\_*$: Forma, xGOT, gols e vitórias do time jogando estritamente **Fora** (decaimento exponencial $\alpha=0.25$, janela 6, min 3 partidas passadas).
   - $\text{liga\_draw\_rate}$: Taxa histórica de empate da liga (shift 1, rolling 100, min 20).
   - $\text{h2h\_draw\_rate}$: Taxa histórica de empate do confronto direto (shift 1, rolling 8, min 2).
2. **Descarte Estrito (`dropna`):** Sem fallbacks inventados. Partidas com qualquer feature nula são descartadas.
3. **Filtros de Entrada:**
   - $\text{Odd\_D\_FT} \in [3.20, 4.20]$
   - $\text{Odd\_H\_FT} \le 2.10$ OU $\text{Odd\_A\_FT} \le 2.10$ (Favorito Claro)
   - $\text{Prob\_ML} \ge 75.0\%$
   - $\text{EV} \ge +0.03$ onde $\text{EV} = \text{Prob\_ML} \times 0.95 - (1 - \text{Prob\_ML}) \times (\text{Odd\_D\_FT} - 1.0)$
   - $\text{liga\_draw\_rate} \le 0.36$
4. **Liquidação Financeira (Stake R$ 100):**
   - Green (Mandante ou Visitante vence): $+\text{R\$} 95,00$
   - Red (Empate): $-(\text{Odd\_D\_FT} - 1.0) \times 100$

---

### 2. CÓDIGO PYTHON COMPLETO PARA VOCÊ EXECUTAR:

```python
import pandas as pd
import numpy as np
import joblib
import unicodedata
import re

# 1. Carregar base e modelo
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
model = joblib.load("modelo_lay_draw_rf_v2.pkl")
scaler = joblib.load("scaler_lay_draw_rf_v2.pkl")
features = joblib.load("features_lay_draw_rf_v2.pkl")

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
df["won_H"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_A"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["h2h_pair"] = [tuple(sorted(x)) for x in zip(df["c_Home"], df["c_Away"])]

df["odd_d"] = pd.to_numeric(df.get("Odd_D_FT", np.nan), errors="coerce")
df["odd_h"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df["odd_a"] = pd.to_numeric(df.get("Odd_A_FT", np.nan), errors="coerce")

stat_cols = [
    "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT",
    "xGOT_Faced_H_FT", "xGOT_Faced_A_FT", "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
    "Big_Chances_H_FT", "Big_Chances_A_FT", "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
    "Possession_H_FT", "Possession_A_FT"
]
for c in stat_cols:
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0) if c in df.columns else 0.0

# 2. Features por Mando
h_cols_in = ["Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_Faced_H_FT", "Goals_Prevented_H_FT", "Big_Chances_H_FT", "Shots_On_Target_H_FT", "Possession_H_FT", "won_H", "_draw_flag"]
h_cols_out = ["H_h_Gf", "H_h_Gc", "H_h_xGOT", "H_h_xGOT_faced", "H_h_GP", "H_h_BC", "H_h_SoT", "H_h_Poss", "H_h_WR", "H_h_draw_rate"]

g_h = df.groupby("c_Home")
wsum = sum(np.exp(-0.25 * (j - 1)) for j in range(1, 7))

for src, dst in zip(h_cols_in, h_cols_out):
    numer = np.zeros(len(df))
    count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_h[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    df[dst] = res

a_cols_in = ["Goals_A_FT", "Goals_H_FT", "xGOT_A_FT", "xGOT_Faced_A_FT", "Goals_Prevented_A_FT", "Big_Chances_A_FT", "Shots_On_Target_A_FT", "Possession_A_FT", "won_A", "_draw_flag"]
a_cols_out = ["A_a_Gf", "A_a_Gc", "A_a_xGOT", "A_a_xGOT_faced", "A_a_GP", "A_a_BC", "A_a_SoT", "A_a_Poss", "A_a_WR", "A_a_draw_rate"]

g_a = df.groupby("c_Away")
for src, dst in zip(a_cols_in, a_cols_out):
    numer = np.zeros(len(df))
    count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_a[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    df[dst] = res

# 3. Liga e H2H
df["liga_draw_rate"] = df.groupby("League")["_draw_flag"].transform(lambda s: s.shift(1).rolling(100, min_periods=20).mean())
df["h2h_draw_rate"] = df.groupby("h2h_pair")["_draw_flag"].transform(lambda s: s.shift(1).rolling(8, min_periods=2).mean())

# 4. Features Combinadas
df["total_WR"]       = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"]        = abs(df["H_h_WR"] - df["A_a_WR"])
df["draw_rate_prod"] = df["H_h_draw_rate"] * df["A_a_draw_rate"]
df["draw_rate_mean"] = (df["H_h_draw_rate"] + df["A_a_draw_rate"]) / 2
df["total_xGOT"]     = df["H_h_xGOT"] + df["A_a_xGOT"]
df["xGOT_diff"]      = abs(df["H_h_xGOT"] - df["A_a_xGOT"])
df["total_Gf"]       = df["H_h_Gf"] + df["A_a_Gf"]
df["gf_diff"]        = abs(df["H_h_Gf"] - df["A_a_Gf"])
df["decisive_score"] = df["total_WR"] * df["wr_diff"]

df["mkt_prob_draw"]     = 1.0 / df["odd_d"]
_ov = 1.0 / df["odd_h"] + 1.0 / df["odd_d"] + 1.0 / df["odd_a"]
df["mkt_prob_draw_norm"] = df["mkt_prob_draw"] / _ov
df["mkt_overvalue_draw"] = df["mkt_prob_draw"] - df["draw_rate_mean"]

# 5. Avaliação 2026 com dropna estrito
df_2026 = df[(df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-08-20")].copy()
df_2026_clean = df_2026.dropna(subset=features).copy()

X = scaler.transform(df_2026_clean[features])
df_2026_clean["Prob_ML"] = model.predict_proba(X)[:, 1]

COMMISSION = 0.05
STAKE = 100.0
df_2026_clean["ev_lay"] = df_2026_clean["Prob_ML"] * (1 - COMMISSION) - (1 - df_2026_clean["Prob_ML"]) * (df_2026_clean["odd_d"] - 1.0)

aprovados = df_2026_clean[
    (df_2026_clean["odd_d"] >= 3.20) & 
    (df_2026_clean["odd_d"] <= 4.20) & 
    ((df_2026_clean["odd_h"] <= 2.10) | (df_2026_clean["odd_a"] <= 2.10)) &
    (df_2026_clean["Prob_ML"] >= 0.75) &
    (df_2026_clean["ev_lay"] >= 0.03) &
    (df_2026_clean["liga_draw_rate"] <= 0.36)
].copy()

tot = len(aprovados)
grn = (aprovados["_draw_flag"] == 0).sum()
red = (aprovados["_draw_flag"] == 1).sum()
wr = (grn / tot) * 100.0
pnl_arr = np.where(aprovados["_draw_flag"] == 0, STAKE * (1 - COMMISSION), -(aprovados["odd_d"] - 1.0) * STAKE)
aprovados["pnl"] = pnl_arr
tot_pnl = pnl_arr.sum()
pf = pnl_arr[pnl_arr > 0].sum() / abs(pnl_arr[pnl_arr < 0].sum())

print(f"Total: {tot} | Greens: {grn} | Reds: {red} | WR: {wr:.2f}% | Lucro: R$ {tot_pnl:,.2f} | PF: {pf:.2f}")

aprovados["Month"] = aprovados["Date"].dt.strftime("%Y-%m")
mensal = []
for m, g in aprovados.groupby("Month"):
    t_m = len(g)
    g_m = (g["_draw_flag"] == 0).sum()
    r_m = (g["_draw_flag"] == 1).sum()
    wr_m = (g_m / t_m) * 100.0
    pnl_m = g["pnl"].sum()
    mensal.append({"Mês": m, "Jogos": t_m, "Greens": g_m, "Reds": r_m, "WR": f"{wr_m:.1f}%", "Lucro": f"R$ {pnl_m:,.2f}"})
print(pd.DataFrame(mensal).to_string(index=False))
```

---

### 3. O QUE VOCÊ DEVE RESPONDER:

1. Apresente os resultados consolidados de **2026 (Total de Entradas, Greens, Reds, Win Rate %, Lucro Líquido e Profit Factor)**.
2. Apresente a tabela com o desempenho **Mês a Mês de Janeiro a Agosto de 2026**.
3. Confirme se os números batem com:
   - **Total de Entradas:** 2.797 jogos
   - **Greens:** 2.284 jogos | **Reds:** 513 jogos
   - **Win Rate:** 81,66%
   - **Lucro Líquido (Stake R$ 100):** +R$ 79.755,00
   - **Profit Factor:** 1.58
```
