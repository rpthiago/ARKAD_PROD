# ESTUDO — Under-no-limite v2 revisitado com xG — Claude, 2026-09-28

Regra ao vivo (`alerta_under_vps.py`, VPS): back na linha de **Under viva** entre os minutos **75-85**,
odd **1,40-2,20**, liquidez ≥ 300. Tese original: *medo do gol tardio* — o mercado pagaria demais pelo
risco de gol nos últimos 15 minutos.

O xG permite, pela primeira vez, perguntar **se o jogo estava perigoso** — e usar só informação disponível
no minuto 80 (o xG do **1º tempo**, sem vazamento do que vem depois).

Fontes: base b365 2024-2026 (45.515 jogos com xG de 1º tempo real **e** minutos de gol consistentes — 100%
de consistência entre a lista de minutos e o placar) + os **637 alertas reais** disparados pela VPS em set/2026.

## 1. O xG do 1º tempo prevê o gol tardio? Sim, e é pequeno

Taxa de gol depois do minuto 80 no universo: **40,3%**.

| xG do 1º tempo | N | gol depois do 80' | gols já marcados até o 80' |
|---|---|---|---|
| < 0,5 | 6.423 | **36,1%** | 1,35 |
| 0,5–1,0 | 15.049 | 39,5% | 1,84 |
| 1,0–1,5 | 12.252 | 40,5% | 2,36 |
| 1,5–2,5 | 10.119 | 43,1% | 3,02 |
| > 2,5 | 1.672 | **45,0%** | 3,97 |

Correlação +0,0455 (p=2,9e-22). Spread de **8,9 pp** entre o extremo morto e o extremo aberto.

**Controlando pelo estado do jogo** (quantos gols já saíram, que é o que define a linha):

| gols até o 80' | N | gol tardio | xG_1T < 1,0 | xG_1T ≥ 1,5 | Fisher |
|---|---|---|---|---|---|
| 0 | 5.131 | 35,6% | 34,6% | **43,2%** | **p=0,0035** |
| 1 | 10.732 | 39,6% | 38,8% | 41,1% | p=0,114 |
| 2 | 11.617 | 40,5% | 39,3% | **43,8%** | **p=0,0001** |
| 3 | 9.207 | 40,3% | 39,2% | 42,2% | **p=0,015** |

O efeito não é artefato do placar: dentro do mesmo estado, jogo que produziu mais no 1º tempo tem mais gol
tardio. Regressão com preço pré-jogo e estado: β(xG_1T) = **+0,0488, p=0,0033** (SE agrupado por liga).

**Para o método isso é uma má notícia**: xG alto → mais gol tardio → o back no Under morre mais.
A direção do filtro seria "não entrar em jogo que já produziu muito no 1º tempo".

## 2. Mas o mercado já cobra por isso? (o teste que decide)

Nos 637 alertas reais, 113 puderam ser cruzados com xG de 1º tempo:

| xG do 1º tempo | N | odd média de entrada | implícita | greens reais |
|---|---|---|---|---|
| < 1,0 | 43 | **1,92** | 52,7% | 65,1% |
| 1,0–1,5 | 36 | **1,94** | 51,8% | 50,0% |
| > 1,5 | 34 | **1,96** | 51,3% | 64,7% |

Correlação entre xG do 1º tempo e odd de entrada: **+0,171 (p=0,070)**. A odd sobe com o perigo, mas
pouquíssimo: **4 centavos** entre o jogo mais morto e o mais aberto, quando a diferença de risco medida no
universo é de ~9 pp de probabilidade. Em tese sobraria espaço — porém:

- Os greens reais por faixa não seguem o xG: 65,1% / 50,0% / 64,7%. **Não há padrão**; com 43/36/34 jogos
  isso é ruído puro.
- Só 113 dos 637 alertas têm xG. O gatilho dispara majoritariamente em ligas sem coleta de estatística.

## 3. O retrato financeiro do método hoje

| | |
|---|---|
| alertas liquidados (set/2026) | 637 |
| odd média paga | 1,90 → implícita **53,3%** |
| greens reais | **53,2%** |
| ROI gravado | **−1,79%** |

O mercado pagou 53,3% e o evento aconteceu 53,2% das vezes. **É um empate técnico com o preço** — e o que
resta é a comissão. Note a distância entre a taxa-base do universo (59,7% sem gol depois do 80') e os 53,2%
dos alertas: o gatilho seleciona justamente os jogos em que o gol tardio é mais provável do que a média, e o
preço sabe disso.

## 4. Conclusão

1. **A tese do "medo do gol tardio" não se confirma no preço.** O Under no minuto 80, nas condições do
   gatilho, está precificado quase exatamente certo (53,3% implícito vs 53,2% real, N=637).
2. **O xG do 1º tempo é um preditor real, mas fraco** (8,9 pp entre extremos) e — decisivo — **não convertível
   em filtro** com os dados de hoje: nos alertas reais não há relação entre xG e resultado, e a cobertura é
   de 18% (113 de 637).
3. **Se algum dia valer a pena testar**, a forma correta é pré-registrar "não entrar quando xG_1T ≥ 1,5",
   medir em stake zero e exigir N≥200 **com xG disponível** — o que, no ritmo atual (113 em 9 dias de
   alertas, mas só nas ligas com coleta), levaria meses.
4. **Ressalva sobre o ROI de −1,79%**: o Hall of Shame registra que a liquidação de métodos UNDER pelo
   coletor grosso infla sistematicamente o resultado (chegou a mostrar +26% onde o real era −18%). O log
   traz `market_id` e `selection_id`; antes de qualquer decisão sobre este método, vale re-liquidar os 637
   alertas pelo **status oficial da Betfair** (WINNER/LOSER), que é o padrão definido em 02/09.

**Diretriz sugerida**: manter arquivado. O estudo com xG não reabre o caso — apenas explica por que ele
fechou: o preço do minuto 80 já embute o que o 1º tempo mostrou.
