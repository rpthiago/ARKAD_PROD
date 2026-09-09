# DECISÃO — Contratar uma API de estatísticas AO VIVO (chutes, escanteios, xG)

**Data:** 2026-09-08 · **Status:** decisão aberta, teste de cobertura definido abaixo

---

## 1. Por que isso apareceu

Três métodos travaram no mesmo ponto — falta de estatística ao vivo, não falta de ideia:

- **Radar HT:** o filtro de pressão (chutes no alvo ≥3 ou escanteios ≥4) não existe em nenhum feed da
  VPS. Hoje depende de o usuário conferir no Sofascore e marcar à mão (`radar_ht_marcar.py`).
- **Under-limite / Late Goal:** o gatilho só enxerga preço e placar. Não dá para condicionar a
  pressão, cartão vermelho, posse tardia — hipóteses que ficaram sem teste.
- **NORTE do projeto:** "só existe edge de 2 formas: (a) modelo com informação que a odd não tem
  (stats/xG), ou (b) viés comportamental in-play". Sem stats ao vivo, a via (a) está fechada in-play.

## 2. O requisito real — e por que ele é duro

O universo do ARKAD **não é o de ligas grandes**. Cruzei os 403 sinais oficiais do under-limite com a
competição de cada jogo no coletor:

```
  9 (2,2%)  Venezuelan Primera Division      6 (1,5%)  Czech 2 Liga
  9 (2,2%)  English Sky Bet League 1         6 (1,5%)  Portuguese Primeira Liga
  8 (2,0%)  Colombian Primera A              5 (1,2%)  Egyptian 2nd Division
  7 (1,7%)  Swiss Super League               5 (1,2%)  German 3 Liga
  7 (1,7%)  French Ligue 2                   5 (1,2%)  English National League
  7 (1,7%)  Argentinian Primera Nacional     5 (1,2%)  Romanian Liga II
  7 (1,7%)  Czech 1 Liga                     5 (1,2%)  Greek Super League
  6 (1,5%)  Ecuadorian Serie A               ...
```

**As 22 maiores ligas somam só 33% dos sinais.** A maior isolada tem 2,2%. É cauda longa pura —
segunda divisão egípcia, Liga II romena, National League inglesa, Primera B colombiana.

Isso inverte a pergunta: não é "qual API tem os melhores dados", é **"qual API tem dado AO VIVO na
cauda longa"**. Uma API excelente que cobre as 50 maiores ligas resolve ~10% do problema.

## 3. O que existe no mercado (set/2026)

| Provedor | Preço | Cobertura | Stats ao vivo | xG ao vivo |
|---|---|---|---|---|
| **API-Football** (api-sports.io) | US$ 19/mês base + €15/mês add-on de estatísticas | 1.200+ ligas, update ~15s | chutes, chutes no alvo, dentro/fora da área, escanteios, faltas, posse, cartões, defesas | não anunciado |
| **Sportmonks** | €29/mês base · xG Basic +€15 · **xG Advanced €199-399** | 2.200+ ligas | eventos e stats in-play | **só no Advanced**, e "principais competições europeias" |
| **Goalserve** | sob consulta (trial grátis) | 400+ ligas, update 3-5s | escanteios, ataques perigosos, estado de jogo, coordenadas de bola | não |
| **TheStatsAPI** | trial 7 dias | ~150 principais + até 1.196 estendidas | posse, chutes, chutes no alvo, escanteios, cartões | "maioria das competições de 1ª divisão" |
| **Big Balls Data** | free tier 1.000 req/dia | 63 ligas | sim | **só big five** |

### O achado que decide metade da questão

**xG ao vivo na cauda longa não existe a nenhum preço.** Todos os provedores limitam xG a primeira
divisão europeia / big five. Nas ligas onde os sinais do ARKAD caem — Egyptian 2nd Division,
Romanian Liga II, Colombian Primera B — não há xG ao vivo comercial. O add-on Advanced da Sportmonks
(€199-399/mês) compraria xG ao vivo para talvez 5 das 22 ligas da lista acima.

**Chutes e escanteios ao vivo, sim, existem em cobertura ampla** — e são exatamente o que o filtro de
pressão do Radar HT pede (`SoT ≥ 3` ou `Corners ≥ 4`, sem xG). Essa é a compra realista.

## 3b. Candidato testado na mão (08/09) — `free-api-live-football-data` (RapidAPI)

Testado com chave real. **É um wrapper do FotMob** (ids batem: `leagueid=47` = Premier League).

**O que ele devolve** (`/football-get-match-all-stats?eventid=`) — painel completo, verificado:

```
Ball possession · Expected goals (xG) · Total shots · Shots on target · Shots off target
Blocked shots · Shots inside box · Shots outside box · Hit woodwork · Touches in opposition box
Big chances · Big chances missed · Accurate passes · Corners · Yellow cards
```

**Tem xG** — o que a API-Football não anuncia. E `xG` + `touches_opp_box` + `big_chance` são
exatamente as features de "informação que a odd não tem".

**Cobertura da cauda longa — testada time a time (8 de 10 achados):**

| Time testado | Liga retornada | ✔️ |
|---|---|---|
| Universitario de Vinto | Primera División (Bolívia) | ✔️ |
| Deportivo Pereira | Primera A (Colômbia) | ✔️ |
| Petrolul Ploiești | Superliga (Romênia) | ✔️ |
| Ceramica Cleopatra | Premier League (Egito) | ✔️ |
| Excursionistas | Primera B Metropolitana (Argentina) | ✔️ |
| Mushuc Runa | Serie A (Equador) | ✔️ |
| Woking | National League (Inglaterra) | ✔️ |
| Ad Carmelita (CR), Sigma Olomouc B | — | ✖️ |

**Preço:** Basic **grátis = 100 requisições/MÊS** (verificado no header `X-RateLimit-Requests-Limit`).
Pro **US$ 9,99/mês = 20.000 requisições/mês**.

**A conta que muda a decisão:** o uso do ARKAD **não é polling contínuo**. O coletor já sabe qual
jogo está no minuto 45 ou 80 — a API é chamada **uma vez, no momento do gatilho**:

```
Radar HT   : 1 chamada por jogo no intervalo   ~197/dia  = ~5.900/mês
Under/Late : 1 chamada por sinal disparado      ~58/dia  = ~1.740/mês
                                                  total  ≈ 7.600/mês
```

**Cabe folgado nos 20.000 do plano de US$ 9,99** — um terço do preço da API-Football (~US$ 36).

**O que ainda NÃO foi verificado (o portão):**
1. **As stats populam DURANTE o jogo?** O único jogo ao vivo na hora do teste era sub-19 e o endpoint
   de stats falhou nele (provavelmente FotMob não tem stats de sub-19). **Sem isso confirmado, a API
   pode ser só pós-jogo** — inútil para gatilho ao vivo, útil só para backtest.
2. **xG ao vivo na cauda longa** provavelmente não existe (é limitação do FotMob, não da API). Chutes
   e escanteios ao vivo são o que importa para o Radar HT — xG seria bônus.
3. **Casamento de nomes** Betfair ↔ FotMob (ex.: "Atletico Nacional Medellin" vs "Atlético Nacional").
   Resolve-se com um mapa em cache, construído uma vez.

⚠️ **A chave usada no teste foi colada em texto** — trocar na conta RapidAPI antes de subir para a
VPS, e guardar em `alerta.env` (que já fica fora do git).

**Quota restante do teste: 73 de 100 neste mês.**

## 4. A recomendação

**Comece por API-Football: US$ 19 + €15 do add-on de estatísticas ≈ US$ 36/mês.** É o único da lista
que combina cauda longa (1.200+ ligas), stats ao vivo com update de 15s e preço de projeto solo.
Sportmonks é a alternativa se a cobertura falhar no teste; o xG Advanced **não** se justifica hoje.

**Mas não assine antes do teste de cobertura abaixo.** A página de marketing diz "1.200+ ligas";
o que importa é se `Colombian Primera B` tem `shots_on_goal` populado **no minuto 44**, não no dia
seguinte. Foi exatamente esse tipo de suposição não verificada que produziu os três últimos vereditos.

## 5. Teste de cobertura — o portão antes de pagar

Usar o free tier / trial de cada candidato. Rodar **durante uma janela de jogos ao vivo**, não em
cima de dados históricos.

1. Pegar a lista de ligas do item 2 (está no coletor: `betfair_live_odds.csv`, campo `competition`).
2. Para cada liga, achar um jogo ao vivo e chamar o endpoint de estatísticas **três vezes**: minuto
   ~30, ~44 (intervalo) e ~80.
3. Registrar por liga: (a) o jogo existe no provedor? (b) `shots_on_target` e `corners` vêm
   preenchidos? (c) vêm **durante** o jogo ou só depois do apito? (d) qual a defasagem em segundos
   contra o placar da Betfair?
4. **Critério de aprovação:** ≥ 70% das ligas da lista com chutes e escanteios preenchidos **ao vivo**,
   defasagem ≤ 60s. Abaixo disso, a API não resolve o problema do ARKAD — resolve o de outra pessoa.

Saída do teste: uma tabela liga × provedor. É ela que decide, não o preço.

## 6. Aviso que precisa estar escrito antes de gastar

**Estatística ao vivo é insumo, não edge.** O mercado in-play já precifica chutes e escanteios — a
auditoria do Radar HT mostrou isso: o favorito que pressionou fica com odd mais **curta**, e por isso
o filtro de odd e o filtro de pressão se excluíam. Comprar o feed não cria vantagem por si.

O que o feed compra, concretamente:
- **Automatizar o que hoje é manual** (o checklist do Radar HT vira coluna do log, sem viés de memória).
- **Testar hipóteses que hoje não têm dado** (pressão, cartão vermelho, posse tardia no under).
- **Alimentar modelo in-play** — a via (a) do NORTE.

O que o feed **não** compra: velocidade. Com polling de 15-60s e o atraso do próprio streaming da
Betfair, não há como ganhar do bot que lê o feed oficial em milissegundos. Qualquer método que dependa
de reagir antes do mercado está morto na largada nesta infraestrutura. O uso viável é **modelo**, não
**corrida**.

## 7. Custo x o que já está em jogo

US$ 36/mês = US$ 432/ano. Comparar com: a carteira hoje tem **um** candidato a edge (Lay 0x0 XGBoost,
N=46 forward, ainda sem CLV confirmado) e três métodos em stake-zero caminhando para reprovação.
A compra se justifica se destravar teste de hipótese — não como aposta em que o dado por si vire lucro.

**Sequência sugerida:** (1) rodar o teste de cobertura no free tier — custo zero, ~1 fim de semana;
(2) se passar, assinar 1 mês e instrumentar o Radar HT com o feed; (3) só então avaliar Sportmonks/xG.

---

### Fontes

- [Sportmonks — xG Data API](https://www.sportmonks.com/football-api/xg-data/) · [planos](https://www.sportmonks.com/football-api/plans-pricing/) · [cobertura](https://www.sportmonks.com/football-api/coverage/)
- [API-Football — pricing](https://www.api-football.com/pricing) · [documentação v3](https://www.api-football.com/documentation-v3)
- [Goalserve — soccer API, cobertura e preços](https://www.goalserve.com/en/sport-data-feeds/soccer-api/coverage)
- [TheStatsAPI — match stats](https://www.thestatsapi.com/football/match-stats) · [cobertura](https://www.thestatsapi.com/coverage)
- [Big Balls Sports Data — football API](https://bigballsdata.com/football-api)
