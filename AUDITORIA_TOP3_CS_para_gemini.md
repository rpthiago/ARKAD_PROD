# AUDITORIA — Lay 2x2 e Lay 0x3 com ranking TOP 3 por menor odd

> Executada em 2026-09-13. Reproduzível: `auditar_top3_cs.py` (base + ledger) e
> `auditar_top3_cs_operacional.py` (coletor). Tabelas em `varredura_over/auditoria_top3_cs_2026-09-13*.csv`.

## Método da auditoria

- **Regras:** as próprias funções `avaliar_jogos_lay_2x2_grade` / `avaliar_jogos_lay_0x3_grade`, chamadas dia a dia.
  `top_n=3` → MANTIDOS; `top_n=None` → QUALIFICADOS; DESCARTADOS = diferença. Nenhuma regra reinterpretada.
- **Base:** Betfair apicomunidade, 2024-03-16 → 2026-09-05, 52.963 jogos com odd de **lay** real e placar FT.
  É a mesma fonte do feed diário (nomes idênticos).
- **Forward:** `metodos_aprovados/forward_5metodos_ledger.csv` (sinais efetivamente gerados, 01/08 →), placar
  oficial da Betfair (VPS) → base Betfair → b365.
- **P&L:** LIABILITY = 1u, comissão 5%. GREEN = 0,95/(odd−1); RED = −1. Break-even = (odd−1)/(odd−0,05).
- **Bootstrap:** bloco-dia, 10.000 iterações, mínimo 6 dias. IC95 e P(ROI ≤ 0).
- **Coletor (VPS):** 522.245 capturas pré-KO dos runners "2 - 2" e "0 - 3", 4.985 jogos, 17/08 → 14/09.

---

## 1. Tabela consolidada — Qualificados vs TOP 3 mantidos vs Descartados

### Lay 2x2 (odd 8-20; Under 2.5 ≤2,00 ou fav ≤1,55/1,60; blacklist SER/IRL/TUR/SCO)

| período | grupo | N | G/R | WR | BE | gap | ROI | PnL | IC95 ROI | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|
| completo | qualificados | 5.464 | 5163/301 | 94,5% | 94,4% | +0,1pp | +0,13% | +7,2u | [−0,5; +0,8] | 0,345 |
| completo | **TOP 3** | 1.877 | 1782/95 | 94,9% | 94,1% | **+0,9pp** | **+0,99%** | +18,6u | [−0,1; +2,0] | 0,040 |
| completo | descartados | 3.587 | 3381/206 | 94,3% | 94,6% | −0,3pp | −0,32% | −11,4u | [−1,1; +0,5] | 0,783 |
| 2026 | qualificados | 2.330 | 2205/125 | 94,6% | 93,7% | +0,9pp | +1,04% | +24,2u | [+0,1; +2,0] | 0,014 |
| 2026 | **TOP 3** | 599 | 577/22 | 96,3% | 92,6% | **+3,7pp** | **+4,06%** | +24,4u | [+2,2; +5,6] | 0,000 |
| 2026 | descartados | 1.731 | 1628/103 | 94,0% | 94,1% | 0,0pp | −0,01% | −0,2u | [−1,1; +1,1] | 0,501 |
| fwd 01/08+ | qualificados | 416 | 389/27 | 93,5% | 94,8% | −1,3pp | −1,40% | −5,8u | [−3,4; +0,6] | 0,910 |
| fwd 01/08+ | **TOP 3** | 92 | 88/4 | 95,7% | 94,6% | +1,1pp | +1,15% | +1,1u | [−3,5; +4,7] | 0,277 |
| fwd 01/08+ | descartados | 324 | 301/23 | 92,9% | 94,9% | −2,0pp | −2,13% | −6,9u | [−4,4; 0,0] | 0,971 |

Por posição no ranking (base completa): **rank 1 +2,2pp · rank 2 +0,8pp · rank 3 −0,8pp** · rank 4 −0,8 · rank 5 −1,0 · rank 6+ −0,2.

### Lay 0x3 (odd 14-35; Under 2.5 ≤2,10; visitante ≥1,85)

| período | grupo | N | G/R | WR | BE | gap | ROI | PnL | IC95 ROI | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|
| completo | **qualificados** | 3.800 | 3721/79 | 97,9% | 96,2% | **+1,7pp** | **+1,76%** | +67,0u | [+1,2; +2,3] | 0,000 |
| completo | TOP 3 | 1.534 | 1489/45 | 97,1% | 96,0% | +1,1pp | +1,13% | +17,4u | [+0,2; +2,1] | 0,014 |
| completo | **descartados** | 2.266 | 2232/34 | 98,5% | 96,4% | **+2,1pp** | **+2,19%** | +49,6u | [+1,6; +2,7] | 0,000 |
| 2026 | qualificados | 2.134 | 2113/21 | 99,0% | 96,0% | +3,0pp | +3,19% | +68,0u | [+2,8; +3,6] | 0,000 |
| 2026 | TOP 3 | 508 | 504/4 | 99,2% | 95,2% | +4,0pp | +4,19% | +21,3u | [+3,2; +4,9] | 0,000 |
| 2026 | descartados | 1.626 | 1609/17 | 99,0% | 96,2% | +2,8pp | +2,87% | +46,7u | [+2,4; +3,3] | 0,000 |
| fwd 01/08+ | qualificados | 128 | 125/3 | 97,7% | 96,6% | +1,1pp | +1,12% | +1,4u | [−2,0; +3,5] | 0,203 |
| fwd 01/08+ | TOP 3 | 66 | 64/2 | 97,0% | 96,4% | +0,6pp | +0,64% | +0,4u | [−4,2; +3,9] | 0,321 |
| fwd 01/08+ | descartados | 62 | 61/1 | 98,4% | 96,8% | +1,6pp | +1,64% | +1,0u | [−1,6; +3,4] | 0,146 |

Por posição no ranking (base completa): **rank 1 +0,6pp** · rank 2 +1,5 · rank 3 +1,1 · rank 4 +1,4 · rank 5 +0,8 · **rank 6+ +2,5pp**.

### Veredito do item 2 (kept vs discarded)

- **2x2: o TOP 3 seleciona a fatia melhor.** A regra ampla é zero (+0,13%, IC cruza zero); o TOP 3 é +0,99% e os descartados −0,32%. O efeito está concentrado no **rank 1** (+2,2pp); o rank 3 já é negativo.
- **0x3: o TOP 3 descarta a fatia melhor.** Os descartados (+2,19%, IC [+1,6; +2,7]) rendem **mais por aposta** que os mantidos (+1,13%) e têm 1,5x o volume. A regra ampla (+1,76%, N=3.800, P(≤0)=0,000) é o resultado mais forte de toda a auditoria. O rank 1 é a **pior** posição do ranking. No 0x3 a trava TOP 3 funciona como teto de liability, não como filtro de alpha — e custa edge.

---

## 2. O "paradoxo do preço" — testado por faixa de odd

A hipótese era: odd de lay menor ⇒ P(placar) maior ⇒ menor margem sobre o break-even. Os dados dizem o contrário nos dois métodos:

| Lay 2x2 · faixa | N | P(2-2) real | break-even | WR | gap | ROI |
|---|---|---|---|---|---|---|
| 8-11 | 249 | 3,21% | 89,8% | 96,8% | **+7,0pp** | +7,82% |
| 11-14 | 354 | 5,65% | 92,2% | 94,4% | +2,1pp | +2,32% |
| 14-18 | 1.346 | 5,42% | 94,2% | 94,6% | +0,3pp | +0,37% |
| 18-22 | 3.515 | 5,69% | 95,0% | 94,3% | **−0,7pp** | −0,72% |

| Lay 0x3 · faixa | N | P(0-3) real | break-even | WR | gap | ROI |
|---|---|---|---|---|---|---|
| 14-18 | 276 | **0,00%** | 94,0% | 100,0% | **+6,0pp** | +6,41% |
| 18-22 | 442 | 1,81% | 95,2% | 98,2% | +3,0pp | +3,18% |
| 22-27 | 1.055 | 2,09% | 96,0% | 97,9% | +1,9pp | +1,97% |
| 27-36 | 2.027 | 2,42% | 96,9% | 97,6% | +0,7pp | +0,72% |

A frequência real do placar quase não muda com a odd (2x2: 3,2% → 5,7%; 0x3: 0% → 2,4%), mas o break-even sobe
com ela (89,8% → 95,0%; 94,0% → 96,9%). **A odd baixa é onde o mercado superestima o placar** — é a ponta mal
precificada. Ordenar por menor odd seleciona essa ponta. O paradoxo não se sustenta empiricamente.

**Ressalva no 0x3:** por faixa, odd baixa é melhor; por *posição diária*, o rank 1 (odd mediana 24, faixa 22-27
que rende +1,9pp) rende só +0,6pp. O "menor do dia" carrega algo além da odd absoluta (≈ −1,3pp contra a própria
faixa). Não identifiquei a causa; fica como pergunta aberta.

---

## 3. Assimetria matemática e o forward do 0x3

Greens necessários para pagar 1 red (liability 1u, comissão 5%): **(odd−1)/0,95**.

| método | odd mín | odd mediana (TOP 3) | odd máx |
|---|---|---|---|
| Lay 2x2 | 8 → 7,4 greens | 17,5 → **17,4** | 20 → 20,0 |
| Lay 0x3 | 14 → 13,7 | 25 → **25,3** | 35 → 35,8 |

**Ledger do forward (o que foi de fato sinalizado):**

| | N | G/R | WR | BE médio | gap | greens somam | reds somam | PnL |
|---|---|---|---|---|---|---|---|---|
| Lay 2x2 Top 3 | 95 | 91/4 | 95,8% | 94,5% | +1,3pp | +5,28u | −4,00u | **+1,28u** |
| Lay 0x3 Top 3 | 67 | 64/3 | 95,5% | **96,3%** | **−0,7pp** | +2,49u | −3,00u | **−0,51u** |

*(a auditoria pediu 68G/3R; o ledger de hoje tem 64G/3R liquidados, 13 pendentes e 31 sem placar — a diferença é
o momento da contagem.)*

**Decomposição da "discrepância":** não há discrepância — WR 95,5% é **abaixo** do break-even de 96,3% que a odd
mediana 25 exige. Cada green rende 0,039u; 64 greens = +2,49u; 3 reds = −3,00u. A odd 25 exige **25,3 greens por
red**; 3 reds exigiriam 76 greens e houve 64. Um WR de 95% parece alto, mas para lay a odd 25 ele é **perdedor por
construção** — o número que importa é WR − BE, e ele foi −0,7pp.

**Tem EV+?** Na base, o TOP 3 do 0x3 é +1,13% com IC95 [+0,2; +2,1] — positivo, mas **1 red a mais por 100 apostas
inverte o sinal**. No forward (N=67, IC [−4,2; +3,9]) o −0,51u está dentro do ruído: não confirma nem nega.

---

## 4. Executabilidade, concorrência de horários e look-ahead

Medido no coletor (capturas de 6h antes até o KO):

| | Lay 2x2 | Lay 0x3 |
|---|---|---|
| jogos com captura ≥3h antes **e** no KO | 2.202 | 721 |
| odd mediana 3h antes → KO | 17,5 → 17,0 | 25,0 → 24,0 |
| \|Δodd\| mediana | 0,5 | 2,0 |
| odd **desce** até o KO / sobe | 53% / 25% | 53% / 31% |
| do TOP 3 "da manhã", quantos ainda estão no TOP 3 no KO | **53%** | **65%** |
| dias em que o TOP 3 é idêntico manhã × KO | **8%** | **27%** |

**O TOP 3 do dia não é um objeto estável.** Um operador que entra de manhã opera um conjunto diferente do que o
backtest estático avalia; no 2x2, metade do TOP 3 muda até o KO.

**Odd do sinal (feed) vs odd real no KO**, nos sinais do ledger casados no coletor:

| | casados | feed | coletor 3h antes | coletor no KO | KO − feed (mediana) | KO > feed | sinais cuja odd no KO **saiu da faixa** |
|---|---|---|---|---|---|---|---|
| 2x2 | 71/124 | 17,50 | 17,50 | 17,50 | +0,00 | 44% | **10 (14%)** |
| 0x3 | 66/111 | 25,00 | 27,00 | 26,50 | +0,50 | 50% | **9 (14%)** |

O feed é um snapshot que fica entre a manhã e o KO. Não há look-ahead grosseiro (mediana ≈ 0), mas **~14% dos
sinais não são executáveis dentro da faixa pré-registrada no momento do KO**, e o backtest não desconta isso.

**Desempate por horário:** estruturalmente só atua quando duas odds são **iguais** (`odds_unicas` no código); não
altera o ranking por odd e não pode ter sido calibrado em resultado porque não muda quem entra, só a ordem de
preenchimento em empate. É diversificação, não filtro.

---

## 5. Liquidez e spread na Betfair (coletor, pré-KO)

| Lay 2x2 · janela | N | spread p50 | p75 | p90 | lay_size p50 | p25 |
|---|---|---|---|---|---|---|
| KO (0-15 min) | 7.384 | **13,3%** | 20,0% | 28,6% | R$192 | R$118 |
| 1h antes | 11.397 | 15,6% | 22,6% | 33,3% | R$182 | R$114 |
| 3h antes | 21.379 | 16,7% | 24,1% | 37,0% | R$176 | R$115 |
| 6h antes | 9.758 | 17,9% | 25,8% | 40,7% | R$169 | R$112 |

| Lay 0x3 · janela | N | spread p50 | p75 | p90 | lay_size p50 | p25 |
|---|---|---|---|---|---|---|
| KO (0-15 min) | 2.533 | **17,6%** | 25,9% | 52,4% | R$160 | R$105 |
| 1h antes | 3.960 | 21,7% | 39,3% | 141,9% | R$147 | R$107 |
| 3h antes | 7.557 | 23,8% | 44,0% | 150,0% | R$147 | R$110 |
| 6h antes | 3.302 | 26,1% | 47,4% | 172,7% | R$145 | R$111 |

Perto do KO, por faixa: 2x2 odd 12-16 spread 10,7% / R$240; 16-22 14,3% / R$180. 0x3 14-18 15,4% / R$180 …
28-36 20,0% / R$140. Há ≥R$100 disponível no melhor preço de lay em **76-89%** das capturas.

**Leitura:** para lay, a odd gravada **é** a do lado executável, então o spread não é slippage — é o custo de
quem quer sair. A liquidez suporta liability de R$100-500 (stake R$4-30 na odd 17-25) sem mover preço. O que
**não** suporta: entrar de manhã (spread p90 de 150% no 0x3) ou escalar para milhares.

---

## 6. Bootstrap bloco-dia (10.000) e classificação

| método (TOP 3) | período | N | ROI | IC95 | P(ROI ≤ 0) |
|---|---|---|---|---|---|
| Lay 2x2 | base completa | 1.877 | +0,99% | [−0,1; +2,0] | 0,040 |
| Lay 2x2 | 2026 | 599 | +4,06% | [+2,2; +5,6] | 0,000 |
| Lay 2x2 | forward 01/08+ | 92 | +1,15% | [−3,5; +4,7] | 0,277 |
| Lay 0x3 | base completa | 1.534 | +1,13% | [+0,2; +2,1] | 0,014 |
| Lay 0x3 | 2026 | 508 | +4,19% | [+3,2; +4,9] | 0,000 |
| Lay 0x3 | forward 01/08+ | 66 | +0,64% | [−4,2; +3,9] | 0,321 |
| Lay 0x3 **sem ranking** | base completa | 3.800 | +1,76% | [+1,2; +2,3] | 0,000 |

**Sobre o 2026:** os filtros de ambos os métodos ("Filtros de Elite do Backtest Oficial") foram calibrados sobre
esta mesma base. O número de 2026 é **in-sample** e não vale como evidência de robustez; o forward (01/08+) é o
único dado fora da amostra, e nele os dois ICs cruzam zero com N < 100.

### Classificação

| método | status | por quê (dado) |
|---|---|---|
| **Lay 2x2 Top 3** | 🟡 **EM VALIDAÇÃO FORWARD** | backtest positivo com odd de lay real (+0,99%, IC toca zero em −0,1); mecanismo consistente (faixa de odd baixa é a mal precificada; rank 1 +2,2pp); forward N=92, +1,1pp, IC [−3,5; +4,7]. Não é 🟢: o forward não descolou de zero. Não é 🔴: não há fabricação, não há seleção de cauda — o ranking seleciona a ponta mal precificada de forma explicável |
| **Lay 0x3 Top 3** | 🟡 **EM VALIDAÇÃO FORWARD** | backtest positivo (+1,13%, IC [+0,2; +2,1]); forward N=66, −0,51u no ledger, IC [−4,2; +3,9]. Não é 🟢: gap forward −0,7pp. Não é 🔴: a regra ampla por trás dele é o resultado mais forte da auditoria (+1,76%, N=3.800, P(≤0)=0,000) |

**Nenhum dos dois é "Garden of Forking Paths" no sentido de seleção de cauda por acaso**: o mecanismo é o mesmo
em ambos (o mercado superestima 2-2 e 0-3 na ponta de odd baixa), aparece por faixa de odd e não por posição
arbitrária, e sobrevive em 30 meses. O que **é** frágil: (a) os filtros de qualificação são in-sample; (b) o
forward é curto; (c) ~14% dos sinais saem da faixa até o KO; (d) o TOP 3 do dia muda em 47% (2x2) / 35% (0x3)
dos casos entre manhã e KO.

### Recomendações práticas

1. **Manter os dois na página 02 e na automação, em stake-zero / stake mínimo**, com o gatilho de escala do
   bloco de gestão (piso do IC95 > 0 e N ≥ 100). Nenhum dos dois passa hoje.
2. **0x3: pré-registrar a regra ampla (sem TOP 3) como forward separado.** Não substitui o TOP 3 já registrado —
   corre em paralelo. Justificativa empírica: descartados +2,19% vs mantidos +1,13%, 2,5x o volume, rank 1 é a pior
   posição. Se o objetivo do TOP 3 é limitar liability diária, isso se resolve com sizing, não descartando os
   melhores jogos.
3. **2x2: manter o TOP 3 como está.** O edge mora no rank 1-2; um "TOP 2" seria melhor no backtest, mas mudar
   agora é decisão pós-resultado.
4. **Executar no KO, não de manhã.** Spread cai pela metade e a odd é a que o backtest usou. Sinal cuja odd no KO
   saiu da faixa **não entra** — registrar como "fora da faixa no KO" no ledger (hoje ~14%).
5. **Não usar o 2026 (+4%) como argumento** para nada. É a amostra de calibração.

### Perguntas abertas (sem resposta nos dados)

- Por que o rank 1 do 0x3 rende menos que a própria faixa de odd (+0,6pp vs +1,9pp)?
- Qual foi o histórico de varredura que fixou "Under ≤2,10 / visitante ≥1,85 / 14-35"? Sem ele, não dá para
  penalizar o p por número de tentativas.
