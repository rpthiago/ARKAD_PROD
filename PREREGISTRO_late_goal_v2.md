# Pré-registro — Late Goal v2 (faixa de odd MEDIDA no coletor)

**Congelado em:** 2026-09-15. **Substitui** o pré-registro de 08/09 (worklog), encerrado por **inviabilidade**: em 8 dias,
547 capturas na regra (min 82–86, diferença de 1 gol, Back Over da próxima linha) e **0 na coorte primária** — a faixa
suposta 3,00–5,50 apareceu em 12 de 547 (2,2%). Erro do Hall of Shame: odd in-play assumida sem medir.

## 1. O que o coletor mediu (08/09 → 15/09, 547 capturas, `late_goal_log.csv`)

| | p5 | p25 | mediana | p75 | p95 |
|---|---|---|---|---|---|
| odd de BACK do Over no instante | 1,56 | 1,87 | **2,10** | 2,38 | 2,84 |

Liquidez no back: mediana 333 (≥800 em só 28%; ≥200 em 66%). Spread lay−back mediana 0,06 (≤0,10 em 74%).
Over 1.5 (placar 1-0/0-1): odd mediana 2,14 · Over 3.5 (2-1/1-2): 2,08.

## 2. Mecanismo (revisto)

A hipótese de 08/09 era "o mercado subestima o gol tardio do time que está perdendo" → Over caro. **Medido:** o
mercado precifica o gol tardio a ~2,10 (≈48% implícito com comissão) — não está caro. A v2 testa a hipótese
residual e falsificável: **na faixa onde o mercado realmente opera, a taxa real de gol após o min 82 com diferença
de 1 supera o break-even da odd?** Se WR ≈ BE, não há edge e o método morre de vez.

**O que já morreu nessa direção:** under-limite (−3,1%), Idea1 Back Under min 10 (−3,0% em 1.254, fechado 15/09),
Lay Draw 60' em favorito empatando (−4,9%), Late Goal v1 (inviável).

## 3. A regra (congelada)

| campo | valor |
|---|---|
| gatilho | minuto **82–86**, diferença de **exatamente 1 gol** (qualquer placar), Back **Over da próxima linha** (1.5 em 1-0/0-1; 3.5 em 2-1/1-2; etc.) |
| odd de back (real, do coletor, primeira captura elegível) | **1,50–2,60** |
| liquidez no back | **≥ 200** |
| spread lay−back | **≤ 0,10** |
| uma aposta por jogo | sim |
| sub-dimensão gravada, NÃO célula | linha (Over 1.5 / Over 3.5), quem está perdendo (mandante/visitante) |

P&L: stake 1u, comissão 5% (como o liquidador grava: `pnl_stake1`). BE = 1/(1+0,95·(odd−1)).

## 4. Critério (3 vias)

- APROVA: N ≥ 250 **e** piso do IC95 (bloco-dia, ≥ 6 dias) > 0 **e** reds ≥ 5.
- REPROVA: teto do IC95 < 0. **Morte antecipada:** N ≥ 80 com ROI < −10% (já automática no liquidador).
- INCONCLUSIVO: resto.

## 5. Dado de julgamento

- **Primeiro olhar (declarado, não julga):** as 265 capturas de 08–15/09 que já caem na regra v2:
  **WR 46,0% vs BE 51,1% (−5,1pp), ROI −9,73%, IC95 [−23,4; +0,1]** (8 dias). Por linha: Over 1.5 N=157 −4,3%
  [−20,0; +8,8]; Over 3.5 N=108 **−17,6% [−33,6; −6,5]** (teto < 0 já no primeiro olhar).
- **Julgamento:** só jogos com KO ≥ 2026-09-16. Snapshot: **2026-10-12** (junto com Over grid, H-extra, Lay 2x2
  após gol, CS coletor). Se em 12/10 a primária tiver N < 80, encerra por fluxo.

## 6. O que NÃO fazer

- Não mexer na faixa depois de ver resultado (ela é a medida, não uma escolha). Não somar Over 1.5 com Over 3.5
  para "salvar" — são sub-dimensões gravadas, não células. Não olhar antes de 12/10.
