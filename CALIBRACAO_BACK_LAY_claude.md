# Calibração back→lay no acervo Betfair — 31/10/2026

**Pergunta:** com a conta Betfair morta, sobrou só o preço de **back** da exchange (`bfe_d` do
football-data, grátis e vivo). Dá para converter em preço de **lay** com erro conhecido, em vez de
assumir um spread — que é como se fabrica edge?

**Resposta: sim, para o Lay Draw, dentro de limites declarados.** Para o Lay 0x0, não.

## Dados

Acervo congelado: 41.455.325 linhas. Extraídas **171.363** capturas pré-jogo (`min_to_ko` entre 0 e
40, positivo = antes do apito) de dois runners: `MATCH_ODDS / The Draw` e `CORRECT_SCORE / 0 - 0`.
Uma captura por jogo, a mais próxima do **KO−10** (instante de decisão da produção). Após descartar
livro degenerado (811 linhas com `back ≈ 1,01`, o artefato de livro vazio): **12.187** jogos de draw
e **12.001** de CS 0-0, de 17/08 a 30/10/2026.

## 1. O `bfe_d` grátis é mesmo o back da Betfair — confirmado

Casamento por data + times normalizados contra `football_data_odds.csv`: 1.319 jogos (10,8% do
acervo, 30 ligas).

| | valor |
|---|---:|
| correlação `bfe_d` vs `back` do acervo | **0,9987** |
| diferença mediana | **+0,000** |
| MAE | 0,023 |
| p90 \|diferença relativa\| | 1,73% |

Não é proxy: é o mesmo número. A limitação é **cobertura** — o football-data tem 39 ligas, o acervo
tinha tudo que a Betfair listava (Israeli Liga Alef, Georgian Liga 3 etc.).

## 2. O spread responde à liquidez, de forma monótona

Razão `lay/back` mediana, por `lay_size`:

| lay_size | Draw | CS 0-0 |
|---|---:|---:|
| < R$50 | **349,6** (livro vazio — artefato) | 21,6 |
| R$50–100 | 1,0889 | 1,2143 |
| R$100–200 | 1,0698 | 1,1905 |
| R$200–500 | 1,0500 | 1,1613 |
| ≥ R$500 | **1,0282** | **1,0968** |

Monótono e saturando — a mesma assinatura de mecanismo real que o piso de liquidez mostrou na
auditoria do HT. A faixa `< R$50` é lixo e tem que ser excluída, não mediada.

## 3. O viés agregado — bootstrap por bloco de dia, treino/teste no tempo

Calibra a razão mediana por faixa de odd **só na primeira metade** (até 20/09) e aplica na segunda.
O que importa não é o erro por aposta, é o **viés da estimativa agregada**, em pontos percentuais do
WR de equilíbrio.

| | viés | IC95 |
|---|---:|---|
| Draw, **sem piso** | **−1,280 pp** | [−1,736, −0,906] |
| Draw, `lay_size ≥ R$200` | **−0,021 pp** | [−0,101, +0,048] |
| CS 0-0, sem piso | −1,661 pp | [−2,049, −1,325] |
| CS 0-0, `lay_size ≥ R$200` | −0,103 pp | [−0,195, −0,009] |

**Sem piso, o viés é de −1,3 a −1,7 pp e no sentido perigoso:** o lay previsto sai baixo, o WR de
equilíbrio sai baixo, e o edge medido sai **inflado** — na ordem de grandeza exata do edge do 0x0,
que é 1,1 pp. Usar o back direto como se fosse lay custa MAE de 2,44 pp (Draw) / 2,90 pp (CS), que é
o erro já catalogado duas vezes no projeto (fatores 1,39x e 1,56x).

## 4. O problema, e a solução: o feed grátis não traz liquidez

O piso é o que torna a conversão válida, e `football_data_odds.csv` **não publica `lay_size`**.
Única saída testável: a liquidez é previsível pelo que o feed **dá** (liga + odd)?

Aprende no primeiro período quais células (liga × faixa de odd) têm liquidez confiável, aplica no
segundo:

| critério — só liga + odd | N | viés | IC95 |
|---|---:|---:|---|
| células conhecidas (N≥20 no treino) | 1.221 | **+0,034 pp** | **[−0,022, +0,096]** |
| P(liq ≥ R$200) ≥ 50% | 1.041 | −0,030 pp | [−0,089, +0,018] |
| P(liq ≥ R$200) ≥ 70% | 732 | −0,043 pp | [−0,133, +0,019] |
| P(liq ≥ R$200) ≥ 90% | 385 | −0,012 pp | [−0,042, +0,021] |
| *referência: `lay_size` real* | 995 | +0,022 pp | [−0,007, +0,059] |

Todos contêm zero, e o screen por liga×faixa é **tão bom quanto o piso real**. A P(liq)≥70% sobram
60% do universo e **92,5% deles são de fato líquidos**. O viés de −1,28 pp vinha da cauda de ligas
obscuras e odds extremas, que o recorte por célula elimina — e liga e odd são observáveis de graça.

## Veredito e limites

**O Lay Draw volta a ser mensurável para frente, com dado grátis.** A conversão é não-enviesada
(±0,1 pp) contra um edge de ordem 1 pp. O erro por aposta (MAE 0,60 pp com piso) **não** permite
decidir uma aposta isolada, mas por ser centrado em zero ele cai com √N na estimativa agregada.

Limites que têm que ser declarados em qualquer uso:

1. **Vale só nas células que o acervo caracteriza** (liga × faixa de odd, N≥20). Essa lista é
   **fechada para sempre** — o acervo congelou em 30/10, então liga nova nunca poderá ser calibrada.
2. **Preço estimado, não real.** Serve para medir edge agregado; não substitui odd executada.
3. **Não resolve o Lay 0x0**: o football-data não tem Correct Score nenhum. A calibração do CS 0-0
   acima só caracteriza o mercado.
4. **Sem o recorte por célula, a conversão fabrica ~1,3 pp de edge falso.** Não usar cru.

Reprodução: `extrai_back_lay.py`, `calibra_back_lay.py`, `erro_agregado.py`,
`liquidez_previsivel.py` (rodam no backup local do acervo, sem depender de conta).
