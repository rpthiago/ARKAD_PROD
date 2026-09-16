# Pré-Registro Oficial — Suíte de Métodos Trader In-Play

> **Status:** EM OBSERVAÇÃO / STAKE ZERO (`stake: 0.0`)  
> **Congelado em:** 2026-09-15  
> **Autoridade:** [GEMINI.md](GEMINI.md) — 5 Leis Inegociáveis e Hall of Shame  
> **Regra Mãe:** Todo método opera em `stake: 0.0` com registro transparente no paper trail até acumular $N \ge 200$ sinais e ter o piso do IC95% estritamente acima de zero.

---

## 1. Fundamentos e Filosofia do Trading In-Play no ARKAD

Ao contrário dos métodos *punter* (onde a aposta é colocada e mantida até os 90 minutos finais), os métodos **trader** buscam explorar **ineficiências temporais e comportamentais da multidão in-play**, encerrando o risco através de **Take Profit** (Cashout Verde) ou **Stop Loss** (perda controlada) antes do apito final.

### Leis de Execução In-Play:
1. **Mercados Líquidos Exclusivos:** Apenas mercados com book denso (Match Odds 1X2 e Over/Under principal). É terminantemente proibido tentar trade em mercados rasos (Correct Score, BTTS), pois o spread duplo consome qualquer lucro.
2. **Matemática do Cashout com Comissão Betfair (5%):**
   - **Para Lay de Entrada (ex.: LTD):** Entrada com odd $O_{in}$ e liability $L$. Saída em Back com odd $O_{out}$.
     $$\text{Stake de Fechamento} = \frac{L + S_{in}}{O_{out}} = \frac{S_{in} \cdot O_{in}}{O_{out}}$$
     $$\text{PnL Líquido} = \left(S_{in} - \text{Stake de Fechamento}\right) \times (1 - 0.05) \quad \text{se } O_{out} > O_{in}$$
   - **Para Back de Entrada (ex.: Fav em Desvantagem ou Under):** Entrada com odd $O_{in}$ e stake $S_{in}$. Saída em Lay com odd $O_{out}$.
     $$\text{Stake de Fechamento} = \frac{S_{in} \cdot O_{in}}{O_{out}}$$
     $$\text{PnL Líquido} = \left(\text{Stake de Fechamento} - S_{in}\right) \times (1 - 0.05) \quad \text{se } O_{out} < O_{in}$$
3. **Sem Lucros Presumidos:** O valor do cashout é derivado estritamente das odds reais do mercado no momento do evento, nunca de aproximações teóricas.

---

## 2. As Regras Congeladas dos 4 Métodos

### Método 1: Lay the Draw Trader (LTD Clássico com Saída no 1º Gol)

* **Mecanismo:** No pré-jogo, o empate de um super favorito custa caro (odd 6.00 a 8.00). Aos 15' a 25' de jogo em 0-0, a odd do empate derrete para a faixa de 3.00 a 4.20 devido à passagem rápida do tempo. Se o favorito abrir o placar, o mercado sofre choque de preço e a odd do empate salta instantaneamente para 7.00+, permitindo fechar com +50% a +70% de lucro sobre o risco sem correr o perigo do empate tardio nos acréscimos.
* **Critérios de Entrada:**
  - Placar atual: **0 - 0**.
  - Janela de minuto: **15' a 25'**.
  - Favorito pré-jogo: Mandante com Odd Pré $\le 1.45$ (menor odd de Match Odds).
  - Odd de Lay Draw atual: **3.00 a 4.20**.
  - Liquidez no mercado: $\ge \$200$ disponíveis na faixa de preço.
* **Gatilho de Saída Green (Take Profit):**
  - **Gol do Favorito (1-0):** Cashout imediato (Back no Empate). Meta de lucro: $+45\%$ a $+75\%$ sobre a stake base.
* **Gatilho de Saída Red (Stop Loss):**
  - **Tempo limite:** Se o jogo chegar aos **68' ainda em 0-0**, encerra a operação em Back Draw assumindo perda parcial (~$-50\%$ a $-65\%$).
  - **Gol da Zebra (0-1):** O trader observa 5 minutos; se o favorito não demonstrar reação imediata, fecha assumindo perda controlada.

---

### Método 2: Swing Trade — Favorito em Desvantagem (Back no Super Fav Perdendo)

* **Mecanismo:** Quando um super mandante sofre um gol isolado nos primeiros 40 minutos (0-1), o mercado sofre sobre-reação de pânico (overshoot de desespero do público comum), inflando a odd do favorito de 1.25 para 2.20+. Com mais de 50 minutos de jogo pela frente e pressão contínua, o gol de empate é altamente provável. Ao fechar no 1-1, o trader não precisa da virada para ter lucro verde.
* **Critérios de Entrada:**
  - Placar atual: **0 - 1** (Zebra vencendo).
  - Janela de minuto: **20' a 45'** (1º Tempo).
  - Favorito pré-jogo: Mandante com Odd Pré $\le 1.35$.
  - Odd de Back no Favorito: **2.10 a 3.20**.
  - Dominância de campo: Mandante mantém ritmo de jogo (posse de bola, presença no campo adversário).
* **Gatilho de Saída Green (Take Profit):**
  - **Gol de Empate (1-1):** A odd do favorito despenca para ~1.40–1.60. Executa-se o Cashout total (Lay no Favorito), travando lucro verde de $+40\%$ a $+60\%$ sobre a stake em todos os placares.
  - *Opcional:* Deixar Freebet no favorito para a virada (lucro zero no empate, lucro máximo na virada).
* **Gatilho de Saída Red (Stop Loss):**
  - **Tempo limite:** Minuto **70'** se persistir 0-1 e a pressão diminuir significativamente.
  - **Segundo gol da Zebra (0-2):** Encerramento imediato da posição com red.

---

### Método 3: Scalping de Janela Morta (Under 1.5 HT / Under 2.5 FT)

* **Mecanismo:** Certas fases do jogo exibem descontinuidade de intensidade tática — especialmente os minutos finais do 1º tempo (33' a 40'), onde as equipes reduzem o ritmo físico para garantir o 0-0 no vestiário, e o início do 2º tempo (55' a 62'), fase típica de substituições e reestruturação tática. O tempo avança rapidamente e as odds do Under caem de 4 a 6 ticks em poucos minutos.
* **Critérios de Entrada:**
  - Janela A (HT): Minuto **33' a 38'**, placar **0-0**, Back Under 1.5 HT.
  - Janela B (FT): Minuto **55' a 62'**, placar **0-0** ou **1-0/0-1**, Back Under 2.5 FT.
  - Condição de dinâmica: Sem faltas perigosas na entrada da área ou escanteios sucessivos nos últimos 5 minutos.
* **Gatilho de Saída Green (Take Profit):**
  - **Permanência de 5 a 8 minutos** (ou apito do intervalo para Under HT): Fechamento em Lay Under após a queda de **3 a 6 ticks**, travando de $+8\%$ a $+15\%$ sobre a stake.
* **Gatilho de Saída Red (Stop Loss):**
  - **Gol durante a permanência:** Fechamento no primeiro tick de reabertura aceitando o prejuízo do gol.

---

### Método 4: Late Goal Trader (Over Limite nos Minutos Finais 78'–85')

* **Mecanismo:** Em jogos equilibrados com 1 gol de diferença no terço final (80'+), o time derrotado adianta todos os blocos e o jogo quebra a estrutura tática. Ocorre aumento exponencial de transições rápidas (contra-ataques 3x2 ou pressão desesperada na área). A odd do Over Limite (gols atuais + 0.5) situa-se na faixa de 1.80 a 2.40.
* **Critérios de Entrada:**
  - Placar atual: Diferença de **exatamente 1 gol** (1-0, 0-1, 2-1, 1-2) ou **empate com super favorito buscando a vitória**.
  - Janela de minuto: **78' a 84'**.
  - Mercado: **Over Limite** (Over 1.5 se 1-0; Over 2.5 se 1-1; Over 3.5 se 2-1).
  - Faixa de odd de Back: **1.80 a 2.50**.
  - Liquidez: $\ge \$200$ no Back; Spread $\le 0.10$.
* **Gatilho de Saída Green (Take Profit):**
  - **Gol aos 80'+:** Green automático e instantâneo! O Over bate e liquida $+100\%$ do retorno líquido. Não é necessário executar cashout manual.
* **Gatilho de Saída Red (Stop Loss):**
  - O jogo encerra sem novos gols (perda nominal da stake de 1u).

---

## 3. Critérios Estatísticos de Julgamento (Decisão de 3 Vias)

Todo sinal capturado por estes 4 métodos é registrado em modo `OBSERVACAO_STAKE_ZERO` nos arquivos de telemetria da VPS e do ARKAD.

Para cada método isoladamente:
1. **APROVADO para Capital Real:**
   - Amostra mínima: $N \ge 200$ sinais executáveis ao vivo.
   - Piso do Intervalo de Confiança (IC95% por bloco-dia, $\ge 6$ dias) **estritamente maior que 0.0%**.
   - Mínimo de 10 Reds observados (garantindo que o método sobreviveu aos eventos adversos).
   - Sobrevivência à correção de taxa de falsas descobertas (Benjamini-Hochberg FDR).
2. **REPROVADO / ARQUIVADO:**
   - Teto do IC95% $< 0.0\%$, OU
   - ROI acumulado $< -10.0\%$ após $N \ge 80$ operações.
3. **INCONCLUSIVO:**
   - Qualquer situação intermediária. Mantém-se em observação stake zero sem alocação financeira real.

---

## 4. O que NUNCA fazer

1. Nunca operar sem ter a odd real da Betfair no momento da entrada.
2. Nunca presumir que o cashout pagará valores teóricos de livro — o spread ao vivo manda no fechamento.
3. Nunca transformar um trade em aposta punter desesperada (se chegou ao ponto de stop loss, **encerre** a operação sem torcida).
4. Nunca operar mercados de Correct Score em trading rápido: a liquidez rasa destruirá o P&L no fechamento.
