# PROMPT PARA O GEMINI — auditar a divergência no Lay 0x0 XGBoost em setembro/2026

Você vai auditar uma divergência entre dois cálculos do MESMO método, no MESMO mês, com a MESMA regra congelada.
Não é para defender nenhum dos dois: é para achar onde os dois universos de jogos diferem e qual deles era executável.

## A regra (congelada, pré-registro 2026-08-02 — não alterar nada nela)
`liga_0x0_rate < 0,08` · `mkt_prob_0x0 < 0,10` (= 1/`Odd_CS_0x0`, odd de BACK do **b365**) · odd de LAY do 0-0 na
Betfair entre 10,0 e 20,0 · `EV > 0,02`, com `EV = p·0,95 − (1−p)·(odd_lay−1)` e `p` do XGBoost treinado só em
`Date < dia`. Liquidação: placar 0-0 = RED (−1u de liability); qualquer outro = GREEN (+0,95/(odd−1)).

## Os dois resultados

| | Antigravity | Claude |
|---|---|---|
| período | 01/09 a 20/09 | 01/09 a 21/09 |
| sinais | **75** | **70** (63 em 01–18/09 + 7 em 19–21/09) |
| greens / reds | 72G / **3R** | 66G / **4R** |
| WR | 96,0% | 94,3% |
| P&L (liability 1u) | **+1,43u** | **+0,04u** |
| ROI sobre liability | **+1,91%** | **+0,06%** |

Reds do Claude: 04/09 New York City × Nashville (odd 16,0) · 06/09 Málaga × Levante (13,0) ·
09/09 Hradec Kralove × Plzen (16,5) · **14/09 Gaziantep × Fenerbahçe (18,0, TURKEY 1, casado por semelhança, 0-0)**.
Os três primeiros são iguais aos do Antigravity. O quarto é a principal diferença de resultado.

Dados do Claude: `varredura_over/repro_0x0_setembro_2026.csv` (01–18/09, 140 candidatos com a coluna `passa_xgb`)
e `varredura_over/repro_0x0_19a21set.csv` (19–21/09).

## As três armadilhas que o Claude encontrou e que a auditoria tem que tratar explicitamente

### 1. O universo do dia muda dependendo de QUANDO você olha (decisão vs retrospectiva)
`get_daily_dataframe("bet365", dia)` devolve no máximo **100 jogos** por dia (paginação na origem). Nos sábados e
domingos o dia tem 250-500 jogos. Já a base consolidada do b365 publica esses mesmos dias **dias depois**, completa.
Consequência medida em 19/09:

| fonte | jogos do dia | sinais que a regra aprova |
|---|---|---|
| endpoint diário na manhã (o que existia para apostar) | 100 | **1** (Tottenham × Aston Villa) |
| base consolidada, olhando em 24/09 | 399 featurizados | **5** |

Os 4 sinais extras (Pardubice, Elgin City, Dundee, Alverca) **nunca estiveram disponíveis** na manhã de 19/09: eles só
existem porque a base encheu depois. Contá-los num backtest é legítimo; contá-los como "jogos que eram para entrar"
não é — e é a diferença entre 5 picks no log e 75 no relatório. **Diga qual universo você usou em cada um dos 75 jogos.**

### 2. Linha duplicada quando a base consolidada já tem o dia
Concatenar `hist` + feed diário duplica o mesmo jogo, porque as duas fontes trazem `Odd_CS_0x0` levemente diferente.
No teste do Claude, Tottenham × Aston Villa apareceu **duas vezes** (EV +0,145 e +0,061) e FK Pardubice também.
Se os 75 do Antigravity não passaram por `drop_duplicates` de (Data, Home, Away), parte deles é o mesmo jogo contado
duas vezes — o que inflaria greens e P&L. **Diga se houve dedup e quantas linhas ele removeu.**

### 3. O endpoint respondeu números diferentes para a MESMA data
Em duas chamadas no mesmo dia (23/09), `get_daily_dataframe("bet365", "2026-09-19")` devolveu **100** jogos numa e
**494** noutra. Se ele oscila, dois cálculos honestos chegam a contagens diferentes. **Verifique se a resposta é
estável e, se não for, qual critério você usou (primeira chamada? maior resposta? cache local?).**

## Pontos específicos a conferir (um por um, com o jogo na mão)

1. **14/09 Gaziantep × Fenerbahçe** (TURKEY 1, lay 18,0, terminou 0-0): ele estava na sua lista? Se não, por quê —
   nome não casou, dia fora da janela, ou `EV ≤ 2%`? Se estava e você o marcou GREEN, confira o placar na fonte oficial.
2. **05/09**: Claude achou 19 sinais, você relatou 21. Liste os 21 com odd e EV para achar os 2 extras.
3. **19 e 20/09**: você os contou usando os picks gerados ao vivo (1 + 1) ou recalculando? Claude, recalculando,
   acha 5 e 2. Qual entrou nos 75?
4. **Fonte da odd de 0x0**: confirme que `mkt_prob_0x0` veio de `Odd_CS_0x0` do **b365**. Usar `Odd_CS_0x0_Back` da
   Betfair afrouxa o filtro: a odd da Betfair é 1,18-1,27× a do b365 (N=79, 18-20/09) e a taxa de aprovação de
   `mkt < 0,10` sobe de 69,6% para 84,8% (+15,2 pp). Isso sozinho criaria dezenas de sinais que a regra não aprova.
5. **Placares**: use a cadeia oficial (status da Betfair na VPS → base Betfair → b365). Diga a fonte de cada red.
6. **Casamento de nomes**: informe, por jogo, se a odd de lay foi obtida por nome exato ou por semelhança
   (critério: cada lado ≥ 0,60 e média ≥ 0,80). Dos 24 sinais que o Claude achou por semelhança, 23 foram green e
   **1 foi red** (o Gaziantep) — não é um filtro de greens.

## O que entregar

1. **Tabela linha a linha dos seus 75 jogos** com: `Data, Liga, Home, Away, odd_lay, p, EV, fonte da Odd_CS_0x0
   (b365/Betfair), casamento (exato/fuzzy), universo (endpoint diário do dia / base consolidada), placar, fonte do
   placar, GREEN/RED`. Sem essa tabela não há como fechar a diferença.
2. **Duas contas separadas, declaradas**: (a) *executável* — só jogos que estavam no feed da manhã do próprio dia;
   (b) *retrospectiva* — tudo que a base mostra hoje. As duas são úteis, mas só (a) responde "quantos jogos eu
   poderia ter feito em setembro".
3. **Recontagem de setembro** em cada uma das duas contas, com N, greens, reds, WR, BE médio, margem em pp,
   P&L em liability 1u e ROI.
4. Se, depois disso, os números continuarem diferentes dos do Claude, aponte o jogo específico em que discordamos —
   não a metodologia em geral.

## Contexto que não deve ser esquecido
- Setembro tem N≈70. O pré-registro pede **1.476** apostas para decidir. Nenhuma das duas contas aprova ou reprova
  o método; o objetivo aqui é ter um número honesto, não um veredito.
- O forward ao vivo (ledger de picks realmente enviados) está em **10 apostas, 9G/1R, ROI −4,58%** sobre liability.
- Regra do Thiago: uma base por método. O 0x0 se valida na base da API (b365 + feed Betfair); o coletor da VPS não
  entra nesta conta.
