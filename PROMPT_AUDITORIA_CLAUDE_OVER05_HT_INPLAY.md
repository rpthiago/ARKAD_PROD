# PROMPT PARA AUDITORIA DO CLAUDE: OVER 0.5 HT IN-PLAY (MINUTO 10)

Copie e cole o texto abaixo integralmente na sessão do **Claude**:

---

```markdown
# AUDITORIA ADVERSARIAL — ARKAD: MÉTODO IN-PLAY OVER 0.5 HT (MINUTO 10 DE JOGO)

> **Contexto:** Após comprovarmos a eficiência do mercado pré-jogo no Lay 0x0 / Over 0.5 FT (e colocarmos o cohort de elite em observação stake-zero até Janeiro/2027), fomos investigar o mercado **IN-PLAY de Over 0.5 HT** por volta do **minuto 10 de jogo** (quando a partida chega aos 8-16 minutos em 0-0).
> 
> Realizamos uma extração direta de **3.297 partidas reais** coletadas pela VPS entre **27 de Setembro e 26 de Outubro de 2026** no arquivo `/home/ubuntu/betfair-collector/betfair_live_odds.csv` (mercado `FIRST_HALF_GOALS_05`, minutos `-18.0 <= mtk <= -7.0`), casadas com as liquidações oficiais da Betfair em `placares_ft.csv` (coluna autoritativa `ht05: Over 0.5 Goals / Under 0.5 Goals`).
>
> Você foi designado como **Auditor Adversarial Chefe**. Seu papel NÃO é validar o método cegamente, mas sim procurar falhas metodológicas, vieses de seleção (*garden of forking paths*), microestrutura da Betfair, liquidez e riscos de latência. Seja rigoroso e baseie-se estritamente nas regras e no Hall of Shame do `GEMINI.md`.

---

## 1. Os Dados Empíricos Coletados (Set/Out de 2026)

### A. O Mercado Geral In-Play aos 10 minutos (Sem Filtro)
Apostar em **Back Over 0.5 HT** em qualquer partida que chega aos 8-16 minutos em 0-0:
* **Amostra Total:** $N = 3.297$ partidas
* **Minuto Médio de Captura:** 11,0 minutos de jogo
* **Odd Back Média:** 1.50 (com liquidez média de \$1.301 no livro)
* **Greens (saiu $\ge 1$ gol até o HT):** 2.164 jogos (**65,64%**)
* **Reds (manteve 0-0 no HT):** 1.133 jogos (**34,36%**)
* **Break-Even Médio (com comissão 5%):** **68,45%**
* **Margem Líquida:** **−2,82 pp**
* **P&L Total (1u stake nominal):** **−142,08u** | **ROI: −4,31%**

*Teste do espelho:* Testamos também o outro lado (**Back Under 0.5 HT** aos 10 minutos):
* $N = 3.283$ | Win Rate: 34,42% vs Break-Even: 41,81% (Margem −7,39 pp) | **P&L: −517,00u** | **ROI: −15,75%**.
* Ambos os lados perdem, comprovando o overround/vig da casa de cerca de 7% a 8% no mercado in-play de HT da Betfair.

---

### B. Desempenho por Faixa de Odd In-Play aos 10 Minutos

| Faixa de Odd Back aos 10' | $N$ | Greens | Reds | Win Rate | Break-Even | Margem ($pp$) | P&L ($1u$) | ROI |
|---|---|---|---|---|---|---|---|---|
| **[1.15, 1.40)** | 835 | 607 | 228 | 72,69% | 77,11% | −4,42 pp | −48,01u | **−5,75%** |
| **[1.40, 1.60)** | 1.653 | 1.128 | 525 | 68,24% | 68,31% | −0,07 pp | −1,95u | **−0,12%** |
| **[1.60, 1.80)** | 664 | 358 | 306 | 53,92% | 61,02% | −7,11 pp | −78,40u | **−11,81%** |
| **[1.80, 2.10)** | 138 | 67 | 71 | 48,55% | 54,57% | −6,02 pp | −15,02u | **−10,88%** |

*Achado:* Quando a odd aos 10 minutos já ultrapassa 1.60, trata-se de jogo tipicamente *under* desde o pré-jogo; o mercado oferece péssimo valor e o apostador perde mais de −11% de ROI. Na faixa 1.40 a 1.60, o mercado empata (ROI −0,12%).

---

### C. Desempenho por Tier de Liga e Favoritismo Pré-Jogo

1. **Por Tier de Ligas:**
   * **Tier 1 (Elite Europeia e Sul-Americana, $N=292$):** WR 66,10% vs BE 72,52% | Margem −6,42 pp | **ROI −9,15%**. O mercado de elite comprime demais a odd in-play aos 10 min (odd média 1.35) e subestima o risco de 0-0 no HT.
   * **Tier 2 (Segundas Divisões Fortes, $N=166$):** WR 72,29% vs BE 67,80% | Margem **+4,49 pp** | **ROI +6,18%** (+10,26u).
   * **Tier 3 (Periféricas, $N=2.839$):** WR 65,20% vs BE 68,07% | Margem −2,87 pp | **ROI −4,42%**.

2. **Por Força do Favorito Pré-Jogo:**
   * $\text{Fav Pré} \le 1,40$ ($N=356$): WR 73,31% vs BE 75,46% | **ROI −2,78%**
   * $1,40 < \text{Fav Pré} \le 1,70$ ($N=539$): WR 68,27% vs BE 70,54% | **ROI −3,10%**
   * $\text{Fav Pré} > 2,00$ ($N=1.343$): WR 61,95% vs BE 65,81% | **ROI −6,27%**

---

### D. A Combinação Selecionada (Combo A)
* **Regra:** Liga Tier 1 ou Tier 2 + $\text{Fav Pré} \le 1,70$ + Odd In-Play aos 10 min entre $1,40$ e $1,75$.
* **Métrica:** $N = 54$ partidas em Outubro/2026.
* **Resultado:** **41 Greens / 13 Reds** (Win Rate: **75,93%** vs Break-Even: **68,11%**).
* **Margem:** **+7,82 pp**.
* **P&L Total:** **+6,42u** | **ROI: +11,89%**.

---

## 2. Perguntas Críticas para Sua Auditoria

Solicitamos que você avalie os seguintes pontos com base nos princípios do `GEMINI.md`:

1. **Risco de Fatiamento Ad-Hoc (*Garden of Forking Paths*):**
   No universo de 3.297 partidas, o ROI geral é de **−4,31%**, e na Tier 1 isolada é de **−9,15%**. O resultado do Combo A ($N=54$, ROI +11,89%) é uma ineficiência estrutural real de favoritos pressionando no 1T ou é apenas uma cauda de variância obtida após fatiar ligas, odds e favoritos no mesmo cohort já observado?

2. **Microestrutura da Betfair e Latência In-Play:**
   Uma entrada aos 10 minutos de jogo enfrenta o delay in-play da Betfair (5 a 8 segundos para correspondência de ordens) e risco de suspensão de mercado (gol prematuro, pênalti, VAR). Como esse atrito afeta a executabilidade real frente a um snapshot estático de coletor?

3. **Assimetria de Tiers (Por que a Tier 2 foi bem e a Tier 1 foi mal?):**
   Qual é a explicação mecânica plausível para as Segundas Divisões (Championship, 2. Bundesliga, Série B) terem apresentado +6,18% de ROI enquanto as Primeiras Divisões apresentaram −9,15%? O mercado de elite tem excesso de otimismo por gols no 1T?

4. **Comparação com o Hall of Shame do GEMINI.md:**
   O `GEMINI.md` registra explicitamente a falha do "Radar HT" (que assumiu odds in-play teóricas e colapsou para −2,33% com dados reais). Este estudo comete algum dos erros do Radar HT ou foi executado com o rigor exigido?

5. **Veredito e Recomendação:**
   Qual deve ser o destino operacional do **Combo Over 0.5 HT Minuto 10**?
   * (A) Reprovar sumariamente por amostra pequena ($N=54$) e vig alta do HT;
   * (B) Colocar em quarentena stake-zero na VPS com regras pré-registradas até $N \ge 250$;
   * (C) Outra abordagem. Justifique matematicamente.
```

---
