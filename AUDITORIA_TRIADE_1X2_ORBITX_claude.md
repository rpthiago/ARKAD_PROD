# Auditoria — Tríade 1X2 (Lay Draw / Lay Home / Lay Away) via Bet365 ➔ OrbitX

**Veredito: reprovado. Nenhum dos três métodos sobrevive quando se usa a odd de lay real.**

Não é questão de calibragem de filtro. O estudo mede uma odd que não existe.

---

## 1. O erro que decide tudo: a odd de lay é sintética

O estudo não observou nenhuma odd de lay. Ele construiu uma: `odd_lay = 1,075 × odd_Bet365`.
Com isso, o filtro "Odd Lay OrbitX entre 4,50 e 10,0" **não é um filtro sobre a exchange** — é um segundo
filtro sobre a mesma odd da Bet365 (equivale a "odd de empate da Bet365 entre 4,19 e 9,30"). A exchange
nunca entra na conta. O que o estudo testa, no fim, é se a odd da Bet365 multiplicada por uma constante
escolhida é batível.

**O 1,075x é verificável e está errado.** As duas bases da mesma API — `..._Bet365.csv` e
`..._Betfair.csv` — cobrem os mesmos jogos, e a segunda traz `Odd_H_Lay`, `Odd_D_Lay`, `Odd_A_Lay`
reais. Cruzando por (Data, Home, Away): **50.248 jogos casados** (99,0% da base Betfair), de 16/03/2024
a 31/07/2026.

| seleção | n | spread mediano real | média | p75 | p90 | % acima de 1,075 |
|---|---:|---:|---:|---:|---:|---:|
| Empate | 47.779 | **1,114x** | 1,159x | 1,185 | 1,293 | **69%** |
| Casa | 49.068 | **1,103x** | 1,138x | 1,180 | 1,294 | 63% |
| Fora | 48.525 | **1,149x** | 1,204x | 1,267 | 1,440 | **74%** |

E o erro não é uniforme — ele **cresce exatamente na faixa onde os métodos operam**:

| faixa da odd de lay (empate) | n | spread mediano |
|---|---:|---:|
| 3,0 – 4,0 | 26.344 | 1,091x |
| 4,0 – 4,5 | 8.744 | 1,139x |
| **4,5 – 6,0** | 7.904 | **1,179x** |
| **6,0 – 8,0** | 2.289 | **1,305x** |
| **8,0 – 10,0** | 710 | **1,354x** |
| 10 – 15 | 469 | 1,500x |
| 15 – 30 | 179 | **1,933x** |

O Lay Draw opera em 4,50–10,0 (spread real 1,18 a 1,35) e o Lay Away em 5,00–25,0 (1,18 a 1,93).
Pelo próprio teste de estresse do estudo, o Lay Draw já cai para +1,68% a 1,120x — e a 1,18x+ vira
negativo. **O erro de medida é correlacionado com a seleção**, então não é ruído: é viés sistemático
na direção que infla o resultado.

Este é o terceiro caso idêntico no ARKAD. O Lay 0x1 dava +3,22% com a odd do log e **−2,05% com a odd
real** (a coluna `_Lay` guardava odd de back, fator 1,39x). Os logs de CS gravavam odd b365 ~13 quando
a executável era 17–20. Mesmo mecanismo, mesma direção.

## 2. Os três métodos, com a odd de lay REAL

Mesma base, mesmos filtros do estudo. Lay com liability 1u: ganha `(1−c)/(odd−1)`, perde 1.
IC95 por bootstrap de blocos-dia (2.000 reamostragens), que respeita a correlação entre jogos do mesmo dia.

| método | odd usada | N | WR | break-even | ROI c=3% | ROI c=6,5% | ROI c=14% | IC95 (c=3%) |
|---|---|---:|---:|---:|---:|---:|---:|---|
| **Lay Draw** | simulada 1,075x | 5.130 | 84,19% | 83,06% | +1,36% | +0,74% | −0,59% | [+0,13%, +2,55%] |
| | **real** | 4.639 | 83,40% | 84,27% | **−1,05%** | −1,61% | −2,81% | **[−2,31%, +0,19%]** |
| **Lay Home** | simulada 1,075x | 2.546 | 86,06% | 84,24% | +2,20% | +1,62% | +0,37% | [+0,64%, +3,81%] |
| | **real** | 2.073 | 85,34% | 85,17% | **+0,34%** | −0,21% | −1,37% | **[−1,49%, +2,08%]** |
| **Lay Away** | simulada 1,075x | 4.459 | 92,42% | 89,81% | +2,91% | +2,53% | +1,72% | [+1,96%, +3,84%] |
| | **real** | 3.683 | 92,18% | 91,33% | **+0,94%** | +0,63% | −0,05% | **[−0,08%, +1,89%]** |

Com a odd simulada eu reproduzo a direção do estudo. Com a odd real, **o ROI dos três cai de 1,9 a 2,4
pontos percentuais e os três IC95 passam a incluir zero** — mesmo na comissão mais generosa (3%).

Repare também que o N cai (5.130→4.639, 2.546→2.073, 4.459→3.683): parte dos jogos selecionados pelo
preço sintético nem entra na janela quando se usa o preço de verdade. A janela foi calibrada sobre um
preço que não existe.

### Dando a melhor chance possível: só ligas líquidas

Restringindo às 41 ligas cujo spread mediano do empate é ≤ 1,10 (filtro **post-hoc**, favorável ao método):

| método | N | WR | break-even | ROI c=3% | ROI c=6,5% | IC95 (c=3%) |
|---|---:|---:|---:|---:|---:|---|
| Lay Draw | 1.793 | 83,21% | 84,24% | −1,27% | −1,83% | [−3,18%, +0,64%] |
| Lay Home | 787 | 86,53% | 85,39% | +1,47% | +0,93% | [−1,54%, +4,20%] |
| Lay Away | 1.491 | 92,82% | 91,44% | +1,52% | +1,21% | [−0,03%, +2,99%] |

Os três IC continuam incluindo zero, já antes de qualquer correção para teste múltiplo.

## 3. A comissão de 3,0% está em conflito com o que foi medido

O estudo adota **3,0%**. Em duas apostas reais no OrbitX via BET-IBC, a cobrança foi de **14%**
(ganho esperado de 7, retenção de 0,98 → 0,98/7 = 14,0%). Antes de qualquer decisão, essa divergência
precisa ser resolvida com o extrato — e note que **mesmo a 3% nada é aprovado**, então a comissão é a
segunda linha de defesa, não a primeira.

Para referência: a 14%, Lay Draw dá −2,81%, Lay Home −1,37% e Lay Away −0,05%.

---

## 4. Respostas diretas

**1. O desacoplamento introduz viés?** Sim, e é o viés central. Filtrar pela Bet365 e *simular* a
execução por um múltiplo fixo não é desacoplamento — é substituir a variável de execução por uma função
determinística da variável de seleção. Como o múltiplo real varia com a odd (1,09x em odd 3–4, 1,35x em
odd 8–10) e com a liga (1,06x na Argentina 1, 1,97x na Espanha 4), o erro anda junto com o filtro.
Não há look-ahead clássico (usar resultado), mas há algo pior na prática: um preço de execução
otimista por construção, justamente onde o método concentra apostas.

**2. Lay Away com teto em 25,0 é edge ou seleção ad-hoc?** Com a odd real, +0,94% com IC
[−0,08%, +1,89%] — não é aprovável. O teto de 25,0 é suspeito pelo mecanismo: a faixa 15–30 tem spread
mediano de **1,93x**, então o teto remove exatamente os jogos onde a simulação de 1,075x erra mais.
A melhora de +0,50% (scan amplo anterior) para +2,20% vem do corte, não de descoberta. É o padrão
clássico de garimpo: o parâmetro foi escolhido depois de ver o resultado. Para ser levado a sério,
precisa de pré-registro do teto e FDR sobre toda a grade testada — não só desta célula.

**3. Cobertura Bet365 vs liquidez na exchange.** Medido no período comum (16/03/2024 a 31/07/2026):
dos **106.135** jogos da Bet365, apenas **50.695 (47,7%)** têm contraparte na base Betfair.
**52% dos jogos do radar não teriam livro.** E das 90 ligas com ≥150 jogos casados, **só 12 têm spread
mediano ≤ 1,075** — ou seja, a premissa do estudo vale em 13% das ligas. O filtro de liquidez não é
opcional: sem ele o radar aponta majoritariamente jogos inexecutáveis. O critério que funciona é
whitelist por liga medida (as mais baratas: Argentina 1, England 2, Italy 2, Spain 2, Brazil 1, Italy 1,
Spain 1 — todas ~1,06x), nunca "a Bet365 cobre".

**4. Veredito do portfólio: não aprovo.** Nenhum dos três tem IC95 que exclua zero com preço real, em
nenhum dos cenários testados, nem restringindo a ligas líquidas. **Não colocar nos blocos do Telegram.**
Avisar jogo é o passo que transforma hipótese em dinheiro perdido.

---

## 5. O que mudaria o veredito

1. **Refazer o estudo lendo `Odd_*_Lay` da base Betfair**, nunca um múltiplo. A base existe e cruza em
   99% dos jogos — não há motivo para simular.
2. **Resolver a comissão** com extrato do OrbitX: 3% ou 14%?
3. Se algo sobreviver aos dois passos acima, aí sim **pré-registrar** filtro e janela de odd, aplicar
   **FDR sobre toda a grade** varrida, e mandar para forward com stake zero antes de qualquer sinal.

O achado lateral do estudo (back na dupla chance dá −2,35% a −3,14%) está correto e é consistente com o
que já sabíamos: os dois lados do mesmo mercado perdem, e a perda é o overround. Mas ele não sustenta a
conclusão de que o edge "sobrevive no lay" — o que o lay faz é trocar um custo de ~7% (overround) por um
custo de comissão mais spread. Quando o spread real entra na conta, a troca deixa de ser vantajosa.

---

*Scripts da auditoria: cruzamento das duas bases da API por (Data, Home, Away); ROI por liability com
`(1−c)/(odd−1)` no green e −1 no red; IC95 por bootstrap de blocos-dia com 2.000 reamostragens,
semente 20261008.*
