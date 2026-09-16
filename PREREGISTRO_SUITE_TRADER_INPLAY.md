# Pré-Registro Oficial — Suíte de Métodos Trader In-Play (Revisão v2)

> **Status:** EM OBSERVAÇÃO / STAKE ZERO (`stake: 0.0`)  
> **Congelado em:** 2026-09-15 (Revisão metodológica e empírica com dados reais da VPS em 2026-09-16)  
> **Autoridade:** [GEMINI.md](GEMINI.md) (Leis 1, 2, 3 e Hall of Shame) & [PREREGISTRO_TEMPLATE_inplay.md](PREREGISTRO_TEMPLATE_inplay.md) §5  
> **Regra Mãe:** Todo método opera em `stake: 0.0` com registro transparente no paper trail até acumular $N \ge 200$ sinais e ter o piso do IC95% estritamente acima de zero pós-FDR.

---

## 1. Fonte de Dados Canônica & Definições Técnicas

1. **Fonte Única na VPS:** `/home/ubuntu/betfair-collector/betfair_live_odds.csv`
   - Colunas: `capture_ts, market_type, competition, home, away, ko, min_to_ko, runner, selection_id, back, back_size, lay, lay_size, ltp, matched`.
   - **Cálculo do Minuto Oficial:** `minuto = int(-min_to_ko - 15)`. Proibido usar `abs(min_to_ko)`.
   - **Favorito Pré-Jogo:** Menor odd de Back de `MATCH_ODDS` na captura mais próxima de −5 minutos do kickoff (`min_to_ko` entre −25 e −10).
2. **Estado do Jogo Factual (`gols_por_ou`):**
   - O estado do jogo é obtido **estritamente pelas linhas de Over/Under batidas** (`OVER_UNDER_05` a `OVER_UNDER_35`), **NUNCA pelo menor lay do Correct Score** (o CS de menor lay indica o placar final mais provável, errando em 72,5% das capturas aos 10'–25').
   - Uma linha $L$ é considerada batida se `Over L` estiver com `back <= 1.02` ou se o mercado sumiu e não volta em nenhuma captura posterior do jogo.
   - Total de gols exato: quando $\max(\text{batidos}) + 0.5 == \min(\text{não batidos}) - 0.5$. Se o estado for indefinido na captura $\rightarrow$ **NÃO ENTRA** (Lei 3: sem defaults inventados).
3. **Execução de Entrada & Saída:**
   - **Entrada:** Odd e liquidez do runner na **primeira captura elegível da janela**. Grava-se `capture_ts` da entrada. Sem captura elegível na janela $\rightarrow$ sem aposta.
   - **Saída:** A odd de saída é a **primeira captura após o evento** com o runner presente no book (Back para fechar Lay, Lay para fechar Back). Grava-se `capture_ts` da saída. Se o mercado não reabrir antes do minuto-limite, aplica-se a regra de stop na captura onde o limite dispara. **Nunca estimar odd de saída.**
4. **Matemática do Cashout com Comissão Betfair (5%):**
   - **Lay Inicial (LTD):** Entrada Lay a $O_{in}$ com liability $L$ ($S_{in} = L / (O_{in}-1)$). Saída Back a $O_{out}$ com stake $S_{out} = (S_{in} \cdot O_{in}) / O_{out}$.
     $$\text{PnL Bruto} = S_{in} - S_{out}$$
     $$\text{PnL Líquido} = \text{PnL Bruto} \times 0.95 \quad \text{se } \text{PnL Bruto} > 0, \quad \text{senão PnL Bruto}$$
     $$\text{ROI} = \left(\frac{\text{PnL Líquido}}{L}\right) \times 100\%$$
   - **Back Inicial (Fav / Under):** Entrada Back a $O_{in}$ com stake $S_{in}$. Saída Lay a $O_{out}$ com stake $S_{out} = (S_{in} \cdot O_{in}) / O_{out}$.
     $$\text{PnL Bruto} = S_{out} - S_{in}$$
     $$\text{PnL Líquido} = \text{PnL Bruto} \times 0.95 \quad \text{se } \text{PnL Bruto} > 0, \quad \text{senão PnL Bruto}$$
     $$\text{ROI} = \left(\frac{\text{PnL Líquido}}{S_{in}}\right) \times 100\%$$

---

## 2. Medições Empíricas no Coletor (16/08 → 13/09/2026)

Antes de fixar as faixas, as odds in-play foram medidas no coletor oficial da VPS em streaming:

| Método | Mercado & Posição | Janela & Estado | p5 | p25 | Mediana | p75 | p95 | Faixa Medida Congelada |
|---|---|---|---|---|---|---|---|---|
| **M1: LTD Clássico** | Lay Draw (Match Odds) | 15'–25' (0-0, Fav $\le 1.45$) | 3.51 | 5.12 | **7.80** | 12.50 | 18.00 | **[3.50, 10.00]** |
| **M2: Fav em Desvantagem** | Back Fav (Match Odds) | 20'–45' (0-1, Fav $\le 1.35$) | 1.85 | 2.10 | **2.40** | 2.85 | 3.40 | **[2.00, 3.20]** |
| **M3: Scalping Janela Morta** | Back Under 1.5 HT | 33'–38' HT (0-0) | 1.32 | 1.60 | **1.82** | 2.25 | 3.35 | **[1.35, 2.20]** |
| **M4: Late Goal v2** | Back Over Limite (+0.5) | 82'–86' (diff = 1) | 1.56 | 1.87 | **2.10** | 2.38 | 2.84 | **[1.50, 2.60]** |

*Nota:* A hipótese inicial de que o Lay Draw de super favorito aos 15'–25' ficava em 3.00–4.20 era miragem (a mediana real é **7.80**, pois o mercado ainda projeta vitória do favorito). A faixa congelada agora cobre o mercado real.

---

## 3. As Regras Congeladas dos 4 Métodos

### Método 1: Lay the Draw Trader (LTD Clássico)
* **Critérios de Entrada:**
  - Placar atual: **0 - 0** (confirmado via `gols_por_ou == 0`).
  - Janela de minuto: **15' a 25'** (`-min_to_ko - 15`).
  - Favorito pré-jogo: Mandante com Odd Pré $\le 1.45$.
  - Odd de Lay Draw atual: **3.50 a 10.00** (faixa medida no coletor).
  - Liquidez: $\ge \$200$ disponíveis na odd de lay; spread $\le 0.15$.
* **Gatilho de Saída Green (Take Profit):**
  - **Gol do Favorito (1-0):** Cashout imediato (Back no Empate na 1ª captura pós-gol).
* **Gatilho de Saída Red (Stop Loss):**
  - **Tempo limite:** Aos **68' ainda em 0-0**, encerra em Back Draw na primeira captura disponível.
  - **Gol da Zebra (0-1):** Encerra imediatamente na 1ª captura pós-gol *(removido em 15/09: 'esperar reação', pois não há dados de 'reação' no coletor; stop é objetivo)*.

---

### Método 2: Swing Trade — Favorito em Desvantagem
* **Critérios de Entrada:**
  - Placar atual: **0 - 1** (Mandante atrás; total de gols $== 1$).
  - Janela de minuto: **20' a 45'** (1º Tempo).
  - Favorito pré-jogo: Mandante com Odd Pré $\le 1.35$.
  - Odd de Back no Favorito: **2.00 a 3.20**.
  - Liquidez: $\ge \$200$; spread $\le 0.10$.
  - *(Nota: 'posse de bola / dominância' removido em 15/09: sem dado no coletor Betfair)*.
* **Gatilho de Saída Green (Take Profit):**
  - **Gol de Empate (1-1):** Cashout total (Lay no Favorito na 1ª captura pós-gol).
* **Gatilho de Saída Red (Stop Loss):**
  - **Tempo limite:** Minuto **70'** se persistir 0-1.
  - **Segundo gol da Zebra (0-2):** Encerramento imediato na 1ª captura após o gol.

---

### Método 3: Scalping de Janela Morta (Under 1.5 HT / Under 2.5 FT)
* **Critérios de Entrada:**
  - Janela A (HT): Minuto **33' a 38'**, placar **0-0**, Back Under 1.5 HT @ [1.35, 2.20].
  - Janela B (FT): Minuto **55' a 62'**, placar **0-0 ou 1 gol**, Back Under 2.5 FT @ [1.35, 1.85].
  - Liquidez: $\ge \$200$; spread $\le 0.06$.
  - *(Nota: 'sem faltas perigosas ou escanteios' removido em 15/09: sem dado no coletor)*.
* **Gatilho de Saída Green (Take Profit):**
  - **Permanência de 5 a 8 minutos** (ou apito do intervalo no HT): Fechamento em Lay Under após queda de ticks.
* **Gatilho de Saída Red (Stop Loss):**
  - **Gol durante a janela:** Fechamento na 1ª captura pós-gol aceitando o stop loss de mercado.

---

### Método 4: Late Goal Trader (Unificado com Late Goal v2)
* **Critérios de Entrada:**
  - Janela de minuto: **82' a 86'** (unificado formalmente com `PREREGISTRO_late_goal_v2.md`).
  - Placar atual: Diferença de **exatamente 1 gol** (1-0, 0-1, 2-1, 1-2...).
  - Mercado: **Over Limite** (Over 1.5 se 1 gol; Over 3.5 se 3 gols).
  - Odd de Back: **1.50 a 2.60** (faixa medida no coletor; mediana 2.10).
  - Liquidez no Back: $\ge \$200$; Spread $\le 0.10$.
* **Gatilho de Saída:**
  - **Gol aos 80'+:** Green automático (+100% da aposta líquida).
  - **Apito final aos 90' sem gols:** Perda da stake de 1u liquidada pelo `placares_ft.csv` oficial.

---

## 4. Critérios de Decisão Estatística (3 Vias)

1. **APROVADO para Capital Real:**
   - $N \ge 200$ operações reais na VPS.
   - Piso do IC95% (bloco-dia, $\ge 6$ dias) **estritamente maior que 0.0%**.
   - Mínimo de 10 Reds observados no período.
   - Sobrevivência ao controle FDR (Benjamini-Hochberg).
2. **REPROVADO / ARQUIVADO:**
   - Teto do IC95% $< 0.0\%$, OU
   - ROI acumulado $< -10.0\%$ após $N \ge 80$ operações.
3. **INCONCLUSIVO:**
   - Permanece em observação com `stake: 0.0`.

---

## 5. Dado de Julgamento & Primeiro Olhar

- **Primeiro Olhar Declarado:** Capturas históricas do coletor de 16/08 a 13/09/2026 servem apenas para validação técnica da camada de dados.
- **Julgamento Oficial:** Exclusivamente partidas com KO $\ge$ **2026-09-16**.
- **Data do Snapshot de Decisão:** **2026-10-12** (junto com Late Goal v2 e Tríade).
