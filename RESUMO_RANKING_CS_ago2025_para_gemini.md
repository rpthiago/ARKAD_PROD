# Resumo — 7 mercados de Correct Score × ranking por menor odd, janela ago/2025 → set/2026

> Cole abaixo da linha no Gemini. Relatório completo (30 meses, mecanismo por posição, coletor):
> `AUDITORIA_RANKING_CS_7MERCADOS_para_gemini.md`. Reproduzível: `auditar_ranking_cs.py`.
> Tabela integral (3 janelas × 4 cortes × 7 métodos): `varredura_over/auditoria_ranking_cs_2026-09-13.csv`.

---

**Janela:** 01/08/2025 → 05/09/2026 (a mesma do pré-registro do Lay 0x0). **Base:** Betfair apicomunidade, odd de
**lay** real (`Odd_CS_*_Lay`), placar FT. **P&L:** liability 1u, comissão 5% (GREEN 0,95/(odd−1), RED −1).
**Ranking:** posição por menor odd de lay entre os qualificados do dia. **Bootstrap:** bloco-dia, 10.000.
Os números de 2026 da varredura do Antigravity reproduziram exatamente; esta janela e o multi-ano são o acréscimo.

## Tabela — ago/2025+

| método | corte | N | reds | red% | WR | BE | WR−BE | odd | ROI liab | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Goleada Visitante** (AO Away; U2.5 ≤2,10; 15-60) | Todos | 5.315 | 206 | 3,9% | 96,1% | 96,9% | −0,83pp | 36,2 | −0,86% | [−1,4; −0,3] | 1,000 |
| | TOP 3 | 1.028 | 60 | 5,8% | 94,2% | 95,6% | −1,42pp | 24,3 | −1,50% | [−3,0; 0,0] | 0,979 |
| | TOP 2 | 718 | 48 | 6,7% | 93,3% | 95,4% | −2,05pp | 23,0 | −2,17% | [−4,1; −0,3] | 0,989 |
| | TOP 1 | 376 | 25 | 6,7% | 93,4% | 95,1% | −1,71pp | 21,3 | −1,82% | [−4,6; +0,7] | 0,914 |
| **Goleada Mandante** (AO Home; U2.5 ≤2,10; 15-60) | Todos | 7.494 | 302 | 4,0% | 96,0% | 96,4% | −0,47pp | 31,3 | −0,48% | [−1,0; 0,0] | 0,971 |
| | TOP 3 | 1.095 | 54 | 4,9% | 95,1% | 94,9% | +0,21pp | 20,2 | +0,22% | [−1,2; +1,5] | 0,361 |
| | TOP 2 | 753 | 33 | 4,4% | 95,6% | 94,7% | +0,93pp | 19,3 | +0,97% | [−0,6; +2,4] | 0,107 |
| | TOP 1 | 385 | 19 | 4,9% | 95,1% | 94,4% | +0,65pp | 17,9 | +0,67% | [−1,7; +2,9] | 0,270 |
| **0x1 Super Fav Mand** (H ≤1,90; 5-15) | Todos | 237 | 20 | 8,4% | 91,6% | 93,3% | −1,78pp | 14,4 | −1,90% | [−5,7; +1,6] | 0,842 |
| | TOP 3 | 229 | 19 | 8,3% | 91,7% | 93,3% | −1,62pp | 14,3 | −1,73% | [−5,6; +1,9] | 0,815 |
| | TOP 2 | 208 | 16 | 7,7% | 92,3% | 93,3% | −0,99pp | 14,3 | −1,06% | [−5,1; +2,6] | 0,694 |
| | TOP 1 | 147 | 12 | 8,2% | 91,8% | 93,2% | −1,39pp | 14,1 | −1,49% | [−6,6; +2,9] | 0,744 |
| **1x0 Super Fav Visit** (A ≤1,90; 5-15) | Todos | 70 | 8 | 11,4% | 88,6% | 92,7% | −4,16pp | 13,5 | −4,47% | [−14,2; +3,4] | 0,835 |
| | TOP 1 | 56 | 6 | 10,7% | 89,3% | 92,6% | −3,28pp | 13,3 | −3,54% | [−13,2; +4,3] | 0,740 |
| **0x0 Quantitativo** (H ≤1,50 ou A ≤1,40; 10-20) | Todos | 596 | 34 | 5,7% | 94,3% | 94,0% | +0,25pp | 16,5 | +0,29% | [−1,7; +2,2] | 0,385 |
| | TOP 3 | 480 | 28 | 5,8% | 94,2% | 93,9% | +0,30pp | 16,0 | +0,34% | [−1,8; +2,4] | 0,369 |
| | TOP 2 | 401 | 24 | 6,0% | 94,0% | 93,8% | +0,26pp | 15,8 | +0,31% | [−2,1; +2,6] | 0,387 |
| | TOP 1 | 253 | 11 | 4,3% | 95,7% | 93,5% | +2,13pp | 15,2 | +2,31% | [−0,6; +4,8] | 0,053 |
| **0x2 Zebra** (H ≤1,45; 5-25) | Todos | 125 | 2 | 1,6% | 98,4% | 95,7% | +2,72pp | 22,4 | +2,86% | [+0,2; +4,6] | 0,018 |
| | TOP 3 | 105 | 1 | 1,0% | 99,0% | 95,6% | +3,43pp | 22,1 | +3,61% | [+1,3; +4,8] | 0,004 |
| | TOP 2 | 91 | 1 | 1,1% | 98,9% | 95,6% | +3,33pp | 22,0 | +3,50% | [+0,9; +4,8] | 0,012 |
| | TOP 1 | 64 | 1 | 1,6% | 98,4% | 95,5% | +2,95pp | 21,7 | +3,12% | [−0,3; +5,0] | 0,070 |
| **2x0 Zebra** (A ≤1,45; 5-25) | Todos | 144 | 4 | 2,8% | 97,2% | 93,1% | +4,08pp | 14,7 | +4,39% | [+1,1; +6,8] | 0,006 |
| | TOP 3 | 128 | 4 | 3,1% | 96,9% | 93,0% | +3,84pp | 14,5 | +4,14% | [+0,5; +6,9] | 0,013 |
| | TOP 2 | 114 | 4 | 3,5% | 96,5% | 92,9% | +3,56pp | 14,3 | +3,83% | [−0,1; +6,9] | 0,028 |
| | TOP 1 | 74 | 3 | 4,1% | 95,9% | 92,7% | +3,27pp | 13,8 | +3,49% | [−2,2; +7,8] | 0,083 |

## O que muda em relação ao multi-ano (30 meses)

- **Goleada Visitante, Goleada Mandante, 0x1, 1x0:** mesmo sinal, mesma direção. O 1x0 fica pior nesta janela
  (−4,5%, 8 reds em 70). O Goleada Mandante TOP 2 é o único positivo (+0,97%), com IC cruzando zero e −0,47% no
  multi-ano.
- **0x0 Quantitativo TOP 1:** melhor número dele em qualquer janela (+2,31%, P(≤0)=0,053), sem descolar de zero, a
  0,6 apostas/dia. Na mesma janela, o Lay 0x0 XGB pré-registrado (liga_rate <0,08, mkt_prob <0,10, lay 10-20) dá
  N=1.129, +1,29%, IC95 [+0,29; +2,30], p=0,0056.
- **0x2 e 2x0:** 125/144 dos 140/166 da base inteira estão nesta janela — a coluna de lay desses placares só passa a
  existir na base em meados de 2025. "ago/2025+" e "completo" são quase o mesmo dado. Reds observados: 2 e 4.

## Mecanismo (do relatório completo, 30 meses)

O ranking por menor odd de lay seleciona alpha quando a odd baixa reflete o mercado superestimando o placar (2x2,
0x3 por faixa de odd), e atrai o desastre quando reflete um motivo real para o placar sair. Nos Any Other, a odd de
lay cai porque o favorito é mais forte, e a goleada fica mais provável (AO Away: 3,3% → 5,4% do rank 5+ para o rank
1) enquanto o break-even cai só 2,3pp. Confirmado independentemente no 0x3: rank 1 tem visitante 2,78 / mandante
3,80 / P(0x3) 3,57%; rank 5+ tem 3,82 / 2,48 / 1,32%.

## Coletor (17/08 → 14/09, capturas pré-KO)

Any Other: spread p90 de 262-300% no KO (>500% de manhã), deriva mediana de 5-6 pontos de odd, 26-28% dos runners
saem da faixa 15-60 até o apito. 0-1/1-0/0-0/0-2/2-0: spread mediano 13-17%, R$177-263 no melhor lay, 80-90% das
capturas com ≥R$100 disponível, 10-23% saem da faixa.

## Classificação (pela régua do bloco de gestão: piso do IC95 > 0 e N ≥ 100)

| método | status |
|---|---|
| Goleada Visitante | 🔴 EV negativo em todos os cortes e janelas |
| Goleada Mandante | 🔴 EV negativo no Todos em todas as janelas; cortes positivos não descolam |
| 0x1 Super Fav Mandante | 🔴 EV negativo em todos os cortes e janelas |
| 1x0 Super Fav Visitante | 🔴 sem sinal e ~1 jogo/semana |
| 0x0 Quantitativo | 🟡 sem descolar; mesmo mercado do 0x0 XGB pré-registrado, que descola |
| 0x2 Zebra | 🟡 piso > 0 no Todos e TOP 3, mas 1-2 reds e sem histórico anterior a 2025 |
| 2x0 Zebra | 🟡 piso > 0 no Todos e TOP 3, mas 4 reds e sem histórico anterior a 2025 |

## Perguntas em aberto

1. Para 0x2 e 2x0, quantos reds observados o Gemini considera mínimo para o IC do bootstrap ser informativo?
2. O 0x0 Quantitativo deve abrir forward próprio, ou ser tratado como variante do 0x0 XGB já em validação?
3. Nos Any Other, existe alguma faixa de odd ou de favoritismo em que a taxa real de goleada fique abaixo do
   break-even? (Na base, por posição, não apareceu.)
