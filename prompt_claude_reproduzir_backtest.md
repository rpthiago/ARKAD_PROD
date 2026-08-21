# PROMPT DE REPRODUÇÃO INDEPENDENTE DE BACKTEST — LAY DRAW (CLAUDE AUDITOR)

> **Instruções para o usuário:** Copie todo o conteúdo abaixo e envie para o Claude. Ele contém o script completo e os parâmetros para que o Claude execute o backtest de forma independente e você compare os números.

---

```markdown
Você é um Auditor Independente de Engenharia Financeira Quantitativa e Machine Learning.

Sua tarefa é executar uma **REPRODUÇÃO CEGA E INDEPENDENTE DO BACKTEST DO MÉTODO LAY DRAW (Lay Empate)** sobre a base histórica oficial `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` e apresentar o resultado exato para comparar com os números do sistema ARKAD.

---

### 1. REGRAS DO MÉTODO LAY DRAW PARA O BACKTEST:

1. **Ativo/Mercado:** Lay Empate (Aposta Contra o Empate — Full Match).
2. **Critérios de Entrada / Filtros Quantitativos:**
   - **Faixa de Odd Lay:** `Odd_D_FT >= 3.20` e `Odd_D_FT <= 4.20` (Faixa Sweet Spot de responsabilidade baixa).
   - **Filtro de Favorito Obrigatório:** `Odd_H_FT <= 2.10` OU `Odd_A_FT <= 2.10` (elimina jogos equilibrados).
   - **Validação de Linhas:** Descartar partidas com datas nulas ou placares ausentes.
3. **Regras Financeiras de Liquidação da Betfair:**
   - **Green (Quando NÃO Empata, ou seja, Mandante vence ou Visitante vence):**
     $$\text{Lucro} = \text{Stake} \times (1 - 0.05) = +\text{R\$} 95,00 \quad (\text{para Stake fixa de R\$} 100)$$
   - **Red (Quando a partida termina em EMPATE):**
     $$\text{Prejuízo} = -(\text{Odd\_D\_FT} - 1.0) \times \text{Stake}$$
   - **Comissão da Bolsa:** $5\%$ sobre os ganhos brutos.

---

### 2. CÓDIGO PYTHON PARA VOCÊ EXECUTAR A REPRODUÇÃO:

```python
import pandas as pd
import numpy as np

# 1. Carregar base oficial
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)

# 2. Padronizar dados
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")
df["Odd_D_FT"] = pd.to_numeric(df["Odd_D_FT"], errors="coerce")
df["Odd_H_FT"] = pd.to_numeric(df["Odd_H_FT"], errors="coerce")
df["Odd_A_FT"] = pd.to_numeric(df["Odd_A_FT"], errors="coerce")

df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_D_FT"]).copy()
df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(int)

# 3. Aplicar Filtros Sweet Spot do Lay Draw
df_valid = df[
    (df["Odd_D_FT"] >= 3.20) & 
    (df["Odd_D_FT"] <= 4.20) & 
    ((df["Odd_H_FT"] <= 2.10) | (df["Odd_A_FT"] <= 2.10))
].copy()

COMMISSION = 0.05
STAKE = 100.0

df_valid["pnl"] = np.where(
    df_valid["_draw_flag"] == 0,
    STAKE * (1 - COMMISSION),
    -(df_valid["Odd_D_FT"] - 1.0) * STAKE
)

def get_stats(df_sub, name):
    tot = len(df_sub)
    grn = (df_sub["_draw_flag"] == 0).sum()
    red = (df_sub["_draw_flag"] == 1).sum()
    wr = (grn / tot) * 100 if tot > 0 else 0
    pnl = df_sub["pnl"].sum()
    grn_pnl = df_sub[df_sub["pnl"] > 0]["pnl"].sum()
    red_pnl = abs(df_sub[df_sub["pnl"] < 0]["pnl"].sum())
    pf = grn_pnl / red_pnl if red_pnl > 0 else 0
    return {
        "Amostra": name,
        "Total Jogos": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate %": f"{wr:.2f}%",
        "Lucro Líquido R$": f"R$ {pnl:,.2f}",
        "Profit Factor": f"{pf:.2f}"
    }

# Amostras a calcular:
# A) 2025 Completo
df_2025 = df_valid[(df_valid["Date"] >= "2025-01-01") & (df_valid["Date"] <= "2025-12-31")]

# B) 2026 Completo (01/01 a 20/08)
df_2026 = df_valid[(df_valid["Date"] >= "2026-01-01") & (df_valid["Date"] <= "2026-08-20")]

print("=== CONSOLIDAÇÃO DOS RESULTADOS ===")
res_2025 = get_stats(df_2025, "Ano 2025 Completo")
res_2026 = get_stats(df_2026, "Ano 2026 (Jan a Ago)")
print(pd.DataFrame([res_2025, res_2026]).to_string(index=False))

print("\n=== QUEBRA MÊS A MÊS EM 2026 ===")
df_2026["Month"] = df_2026["Date"].dt.strftime("%Y-%m")
mensal = []
for m, g in df_2026.groupby("Month"):
    mensal.append(get_stats(g, m))
print(pd.DataFrame(mensal).to_string(index=False))
```

---

### 3. O QUE VOCÊ DEVE RESPONDER:

1. Execute ou analise a lógica do script acima linha por linha.
2. Apresente as tabelas com os números exatos de **Total de Jogos, Greens, Reds, Win Rate %, Lucro Líquido R$ e Profit Factor** para:
   - **Ano de 2025 Completo (365 dias)**
   - **Ano de 2026 (Janeiro a Agosto)**
   - **Quebra detalhada mês a mês de Janeiro a Agosto de 2026**.
3. Confirme se há qualquer viés, erro de cálculo de odd/comissão ou divergência na formulação matemática.
```
