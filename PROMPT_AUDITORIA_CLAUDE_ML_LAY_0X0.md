# PROMPT DE AUDITORIA — CLAUDE (ARKAD)
## Objeto da Auditoria: Método Machine Learning para Lay Under 0.5 FT / Lay 0x0 (Estritamente 2026)

> **Contexto:** Este método foi desenvolvido pelo Antigravity/Gemini a pedido do usuário para resolver o problema de volume do Lay 0x0 / Over 0.5 FT (que no filtro manual estático produzia apenas 50 jogos/ano). O método utiliza Machine Learning (LightGBM) com foco em discrepâncias cross-market e foi testado **estritamente dentro do ano de 2026**, com validação temporal *Out-of-Sample* de Maio a Outubro/2026.
>
> **Sua função como Auditor Chefe do ARKAD:** Avaliar a solidez estatística, detectar potenciais vazamentos (*data leakage*), checar armadilhas do *Hall of Shame* do `GEMINI.md` e emitir um parecer conclusivo sobre se o método tem edge real, se deve ir para a **Página 03 (Watchlist Stake-Zero)** ou se deve ser arquivado.

---

### 1. Especificações Técnicas do Método

* **Mercado Alvo:** Lay Under 0.5 FT (ou Lay CS 0-0) na Betfair Exchange.
* **Odd Utilizada:** Odd real executável de Lay (`Odd_Under05_FT_Lay`), coletada no pré-jogo perto do KO.
* **Matemática do P&L (Comissão 5% - Lei 4):**
  * Green (houve pelo menos 1 gol): $+0,95$ unidade nominal (ou $+0,95 / (\text{odd}-1)$ sobre liability).
  * Red (placar 0-0): $-(\text{odd} - 1)$ sobre stake nominal (ou $-1,0$ unidade de liability).
  * Break-even Win Rate: $\text{BE} = \frac{\text{odd} - 1}{\text{odd} - 0,05}$.
* **Modelo:** LightGBM Classifier regularizado (árvores rasas para evitar overfitting):
  * `n_estimators=100`, `max_depth=3`, `num_leaves=7`, `learning_rate=0.03`, `min_child_samples=50`, `subsample=0.8`, `colsample_bytree=0.8`.
* **Features Selecionadas (Total: 37 features leak-free):**
  1. *Probabilidades Limpas de Mercado:* $p_H, p_D, p_A$ (1X2 normalizado sem overround).
  2. *Cross-Market Ratios (Discrepâncias de Preços Públicos):* 
     * $P(\text{Over 1.5}) / P(\text{Over 2.5})$, $P(\text{BTTS}) / P(\text{Over 2.5})$, $P(\text{HT Over 0.5}) / P(\text{FT Over 1.5})$.
  3. *Incerteza de Mercado:* Entropia de Shannon da distribuição 1X2 ($-\sum p \ln p$).
  4. *Disparidade de Forças:* $|p_H - p_A|$ e razão Empate/Favorito ($p_D / p_{\text{Fav}}$).
  5. *Odd Implícita de 0-0:* $P_{\text{implied}}(0\text{-}0) = 0,95 / (\text{Odd\_Lay} - 0,05)$.
  6. *Métricas Rolantes Ofensivas (Shift 1, r=5 jogos, leak-free):* Gols esperados conjuntos, chutes no alvo conjuntos ($H_{\text{SoTF}} + A_{\text{SoTF}}$), escanteios conjuntos.
  7. *Relação Estatística vs Preço:* $(\text{Gols Rolantes}) \times P_{\text{implied}}(0\text{-}0)$.
  8. *Governança de Ligas:* `liga_0x0_rate`, `liga_draw_rate`, `liga_o25_rate`.
* **Critério de Entrada:**
  * Teto de Odd Lay: $\text{Odd\_Under05\_FT\_Lay} \le 16.50$ (ou $\le 12.00$).
  * Expected Value Teórico Positivo: $EV \ge +2\%$ (onde $EV = P_{\text{model}}(\text{Over 0.5}) \times 0,95 - P_{\text{model}}(0\text{-}0) \times (\text{Odd} - 1)$).
  * Trava de Governança de Ligas: `liga_0x0_rate < 0.10` (exclui ligas com histórico crônico de 0-0 $\ge 10\%$, banindo divisões de acesso periféricas).

---

### 2. Resultados Experimentais (Estritamente 2026)

#### A. Calibração Probabilística no Teste Cego Inicial (3.440 jogos OOS Mai–Ago/2026):
* **Baseline Fechamento Betfair:** Log-Loss: `0.26882` | Brier Score: `0.06803` | AUC: `0.5920`
* **LightGBM Regularizado:** Log-Loss: **`0.25765`** | Brier Score: **`0.06762`** | AUC: **`0.6238`**
*(O modelo bateu a calibração da closing line da Betfair em log-loss).*

#### B. Desagregação Mês a Mês no OOS (Mai a Outubro de 2026):
*Com Teto Odd $\le 16.50$, $EV \ge +2\%$ e Governança de Ligas:*

| Período | Universo Base | N Jogos | Greens | Reds | Win Rate | Break-Even | Margem | ROI Nom. | P&L ($u$) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Maio/2026** | Feeds API FutPythonTrader | 130 | 120 | 10 | 92,3% | 91,7% | +0,59 pp | +5,23% | **+6,80u** |
| **Junho/2026** | Feeds API FutPythonTrader | 44 | 41 | 3 | 93,2% | 91,1% | +2,12 pp | +21,48% | **+9,45u** |
| **Julho/2026** | Feeds API FutPythonTrader | 58 | 54 | 4 | 93,1% | 91,0% | +2,08 pp | +23,28% | **+13,50u** |
| **Agosto/2026** | Feeds API FutPythonTrader | 4 | 4 | 0 | 100,0% | 91,6% | +8,38 pp | +95,00% | **+3,80u** |
| **Setembro/2026** | Feeds API FutPythonTrader | 42 | 40 | 2 | 95,2% | 91,2% | +4,01 pp | +51,90% | **+21,80u** |
| **Outubro/2026 (01 a 26/10)** | Coletor VPS Betfair | 919* | 896 | 23 | 97,5% | 88,9% | +8,58 pp | +73,86% | **+678,80u** |

*(Nota de Outubro: no recorte com filtro de favorito $\text{Fav} \le 1.80$. No universo amplo sem filtro de favorito, foram $N=1.446$ jogos, WR 94,1%, ROI +58,9%).*

---

### 3. Fatos Forenses e Red Flags Observados

1. **A Armadilha de Junho/Julho sem Governança:**
   * Quando o modelo rodou sem governança de ligas em Junho e Julho, ele foi NEGATIVO (-19,9% em Junho e -7,2% em Julho).
   * Motivo: As ligas europeias estavam em férias. O volume foi quase todo para ligas secundárias latinas (Argentina 2, Uruguai 1, Colômbia 2, Brasil C), onde a taxa natural de 0-0 sobe para 14% a 16%.
   * Ao travar `liga_0x0_rate < 0.10`, esses meses viraram positivos (+21,5% e +23,3%).
2. **Perfil dos Reds em Setembro/Outubro:**
   * Dos 3 Reds ocorridos nos feeds de Setembro/Outubro, 2 foram na **COLÔMBIA 2** (Tigres 0-0 e Real Soacha 0-0).
3. **Mudança de Fonte de Dados em 03/10:**
   * A API da FutPythonTrader deixou de alimentar o feed da Betfair em 03/10. De 03/10 a 26/10, os dados vieram diretamente do coletor próprio da VPS (`betfair_live_odds.csv` e `placares_ft.csv`).
4. **Bootstrap IC95%:**
   * No cohort de Mai–Ago ($N=236$), o Bootstrap de 10.000 iterações deu IC95% de `[-25,64%, +50,79%]`, com $P(\text{ROI} \le 0) = 22,3\%$. O intervalo ainda cruza o zero devido à liability alta de odds entre 10 e 16.

---

### 4. Perguntas de Auditoria para o Claude

Por favor, audite este método com base nas regras estritas do **GEMINI.md** e responda pontualmente:

1. **Risco Estrutural de Cauda (Odds 10–16):**
   * Com odds de Lay entre 10.0 e 16.5, cada Red custa entre 9u e 15.5u de liability. O edge observado (+1,4 pp a +4,0 pp de margem) é estatisticamente suficiente para absorver agrupamentos de reds (*red clusters*), ou isso pode ser uma nova versão da miragem de cauda que matou o Lay 0x0 antigo?
2. **Governança de Ligas vs Overfitting:**
   * O filtro `liga_0x0_rate < 0.10` e a exclusão de segundas divisões latinas (Colômbia 2, Argentina 2) é uma regra de governança fundamentada pela física do futebol ou representa seleção *ad-hoc* / *garden of forking paths*?
3. **Mudança de Base (API vs Coletor VPS):**
   * A Lei 7 diz: *"Uma base por método"*. A transição de Setembro (base API) para Outubro (coletor VPS) viola a Lei 7, ou a validação na odd de Lay real da Betfair do coletor é a autoridade máxima do paper forward?
4. **Viabilidade Operacional no Coletor VPS:**
   * Para operar isso ao vivo no KO-10 da VPS, o modelo precisa de features cross-market (1X2, Over 1.5, Over 2.5, BTTS). A liquidez desses mercados no KO-10 é confiável para calcular essas probabilidades sem spread excessivo?
5. **Veredito de Governança:**
   * Qual deve ser o status deste método no ARKAD?
     * **(A) Aprovado com dinheiro real** (justifique se os critérios do GEMINI.md foram satisfeitos);
     * **(B) Watchlist Stake-Zero na Página 03** (acumular forward sem dinheiro por mais 30-60 dias);
     * **(C) Arquivado / Reprovado** (identifique qual falha fatal do Hall of Shame ele comete).
