# ANÁLISE — RapidAPI FotMob/Opta: xG e stats do 1º tempo ao vivo

**Data:** 2026-09-08 · Resposta ao prompt "Descoberta de API com xG e Stats do 1º Tempo ao Vivo"

---

## 0. O teste que faltava: onde a cobertura acaba

O teste original foi feito em **Betis x Real Madrid** — La Liga. Isso prova que o endpoint existe,
não que ele serve ao ARKAD. Rodei o mesmo endpoint descendo a escada das ligas onde os sinais
realmente caem:

| Liga | `football-get-match-firstHalf-stats` | xG | Chutes / SoT / Escanteios |
|---|---|---|---|
| **La Liga / Premier League** | ✔️ funciona | ✔️ **sim** (2,35 · xGOT · open play · set play) | ✔️ |
| **Ecuadorian Serie A** (`eventid 1000009104`) | ✔️ funciona | ✖️ **ausente** | ✔️ posse, 4/5 chutes, 3/1 no alvo, toques na área, big chances |
| **Argentinian Primera B Metropolitana** (`eventid 5114038`, Excursionistas x Ituzaingó — jogo real do nosso log) | ✖️ **`Request Failed`** | ✖️ | ✖️ **nada**, nem no `all-stats` |

**O gradiente é de três degraus, e ele define tudo:**

1. **Elite europeia** → tudo, inclusive xG e xGOT ao vivo.
2. **Meio de tabela mundial** (Equador Serie A, e provavelmente Colômbia A, Chile, Brasil B) →
   **chutes, chutes no alvo, escanteios, posse, big chances, toques na área. Sem xG.**
3. **Cauda longa** (3ª divisão argentina, e provavelmente Egyptian 2nd, Romanian Liga II,
   Colombian Primera B) → **não existe estatística nenhuma.**

Isso qualifica a afirmação "o obstáculo foi quebrado": **foi quebrado no degrau 2, não no degrau 3.**
E lembrando o mapa dos sinais: as 22 maiores ligas somam só 33% do fluxo do under-limite; boa parte do
resto vive no degrau 3, onde não há dado a nenhum preço.

**A boa notícia:** o filtro de pressão do Radar HT (`SoT ≥ 3` **ou** `Corners ≥ 4`) **não precisa de
xG**. Ele é automatizável em tudo que estiver no degrau 2 — hoje ele é 100% manual.

---

---

## 0b. VARREDURA EXECUTADA (08/09) — 32 ligas, cota grátis gasta até o fim

Rodada com `varredura_cobertura.py`; resultado bruto em `cobertura_ligas.csv`. Uma liga por vez:
busca um jogo encerrado de um time que jogou recentemente e chama `firstHalf-stats`.

**Resultado, ponderado pelo nº de sinais do under-limite:**

| Degrau | Sinais | % do testado |
|---|---|---|
| **1 — xG + chutes + escanteios** | 69 | **39%** |
| **2 — chutes/escanteios, sem xG** | 29 | 17% |
| 3 — nada | 39 | 22% |
| nome não resolvido (inconclusivo) | 38 | 22% |

**≥56% do fluxo tem estatística utilizável.** E o achado que contraria a pesquisa de fornecedores:

**Degrau 1 (TEM xG do 1º tempo):** Colombian Primera A · English League One · Swiss Super League ·
French Ligue 2 · Portuguese Primeira · Polish Ekstraklasa · Greek Super League · Japanese J League ·
Brazilian Serie A **e Serie B** · Belgian Pro League · German Bundesliga 2.

O marketing da Sportmonks dizia "principais competições europeias". O FotMob entrega xG do 1º tempo
até em **Colômbia Primera A, League One inglesa e Brasileirão Série B**. Isso é muito melhor do que
a pesquisa de ontem indicava e **muda a viabilidade da hipótese 2**.

**Degrau 2 (chutes e escanteios, sem xG):** Venezuelan Primera · Ecuadorian Serie A · Chilean Primera ·
Danish 1st Division · Slovakian Super League. → **o filtro de pressão do Radar HT é automatizável aqui.**

**Degrau 3 (nada):** Czech 1 e 2 Liga · German 3 Liga · English National League · Ecuadorian Serie B ·
Colombian Primera B · Indonesian Super League · Swedish Superettan.

**Duas fontes de erro declaradas:**
1. **22% caiu em "nome não resolvido"** (San Martin De San Juan, Union Santa Fe, Midtjylland, Manisa FK,
   CSM Satu Mare, Proxy Work Club, Tianjin Jinmen Tiger). São times que o FotMob certamente tem —
   é falha de casamento de nome, **não de cobertura**. Já é a medida do trabalho da letra (A):
   **~22% dos nomes da Betfair não resolvem direto.**
2. **Degrau 3 pode ter falso negativo:** o script pega o primeiro jogo *encerrado* que a busca
   devolve, que pode ser amistoso ou copa sem stats. Czech 1 Liga aparecendo sem dado é suspeito.
   Reconferir com quota nova.

**Cota grátis esgotada (3 requisições restantes, reset em ~30 dias).** Qualquer verificação adicional
depende do plano Pro (US$ 9,99).

---

## (A) Casamento de entidades Betfair ↔ FotMob

**Viável, e mais barato do que o desenho proposto.** O `football-matches-search?search=<time>` já
devolve tudo que é preciso: `id` do jogo, `leagueId`, `leagueName`, `matchDate` em UTC, `homeTeamId`,
`awayTeamId` e o placar. Testado: buscar "Excursionistas" devolveu o jogo exato de 07/09 contra o
Ituzaingó, com `matchDate 2026-09-07T23:00:00Z`.

**Não faça o mapeamento por jogo — faça por liga.** Buscar time a time custa ~1 requisição por jogo
(~197/dia ≈ 5.900/mês). Puxar a agenda por liga custa **1 requisição por liga por dia** (~40 ligas =
1.200/mês, 5× mais barato) e já traz todos os `eventid` do dia.

Arquitetura sugerida (cron 06:00 UTC, como você propôs):

```
1. lê as ligas/jogos do dia no coletor Betfair (grátis, já existe)
2. 1 chamada por liga -> pega todos os eventid do dia
3. casa por: nome normalizado (NFKD + minúsculas + só alfanumérico, que já é o _cn() da VPS)
   + KO dentro de ±30 min + os DOIS nomes de time batendo
4. grava em fotmob_map.csv: (ko, home, away) -> eventid, leagueId, tem_stats
5. o que não casar vai para revisao_manual.csv — não inventa correspondência
```

O campo `tem_stats` é o que importa: marcado uma vez por liga, evita gastar requisição em jogo do
degrau 3 para sempre. Risco real e conhecido: acento e alias ("Atletico Nacional Medellin" ×
"Atlético Nacional"). Resolve com o `_cn()` + fallback de similaridade, com revisão manual da sobra.

## (B) Onde existe alpha — e o argumento estrutural contra

Antes das hipóteses, o problema de fundo, que vale para as três:

> **Onde o xG existe, o mercado já o tem. Onde o mercado talvez seja lento, o xG não existe.**

O xG da Opta é o mesmo insumo que os market makers da Betfair consomem — em La Liga não há nenhuma
assimetria a explorar contra quem lê o mesmo número antes de nós. E na cauda longa, onde o mercado é
raso e possivelmente mal precificado, o degrau 3 não entrega dado algum. **O xG cai na faixa errada
do espectro.** Isso não mata a ideia, mas move o alvo: o que sobra explorável é o **degrau 2**, com
**chutes e escanteios**, não com xG.

**Hipótese 1 (Lay favorito caro com xG zero).** O mecanismo é plausível, mas colide com o achado da
própria auditoria do Radar HT: o mercado **comprime a odd quando o time pressiona**, ou seja, ele
já reage a chute e escanteio. Se reage no sentido bom, reage no sentido ruim — a odd do favorito
apático provavelmente já subiu. Testável, mas eu esperaria pouco. Versão que sobrevive: rodar com
**SoT em vez de xG**, no degrau 2, onde o fluxo de traders é mais fino.

**Hipótese 2 (Over in-play com xG acumulado alto a 0-0).** É a mais interessante das três, por um
motivo específico: ela é uma afirmação de **modelagem** (o hazard de gol condicionado a chances
criadas), não de direção. Chances criadas até o minuto 60-70 são um preditor de hazard futuro
genuinamente diferente do que o preço sozinho carrega. Restrição: precisa de xG (degrau 1) ou de um
proxy — `big_chance` + `SoT` + `touches_opp_box`, que existem no degrau 2. **É a que eu testaria.**

**Hipótese 3 (o mercado já precifica).** Parcialmente certa, mas mal formulada. Não é "a Betfair é
rápida demais" — vocês nunca vão competir em velocidade mesmo, com polling de 60s e o atraso do
streaming. É: **o dado barato para nós é barato para eles também.** A pergunta certa não é
"conseguimos ler o xG a tempo?", é **"o xG do intervalo adiciona informação além do preço que já
está na tela?"**.

E essa pergunta tem um teste barato e definitivo, que não precisa de nenhuma aposta.

## (C) O teste de baixo consumo — mas 3-5 req/dia é lento demais

3-5 requisições/dia nunca chegam a N. O desenho certo separa dois estágios:

**Estágio 0 — varredura de cobertura (GRÁTIS, esta semana, ~40 requisições).**
Uma requisição por liga da lista do ARKAD, num jogo já encerrado, no `firstHalf-stats`. Saída: tabela
liga → degrau 1 / 2 / 3. **É isso que decide se vale pagar os US$ 10.** Cabe no que sobra da cota
grátis (66 requisições restantes) e não depende de jogo ao vivo.

**Estágio 1 — o instrumento de informação (só se o estágio 0 passar; US$ 9,99/mês).**
Não é um método de aposta. É medir se o dado do 1º tempo **prevê o movimento do preço**:

```
para cada jogo em liga do degrau 1 ou 2, no intervalo:
  grava  P_HT   = preço Betfair no intervalo (já temos, coletor)
         X_HT   = xG / SoT / big chances / escanteios (1 requisição)
  depois grava  P_FIM = preço de fechamento / resultado oficial
mede: X_HT tem poder preditivo INCREMENTAL sobre o resultado, dado P_HT?
      (regressão logística: resultado ~ log(P_HT) + X_HT; o coeficiente de X_HT é != 0?)
```

Se o coeficiente de `X_HT` não for significativo, **a hipótese 3 está certa e o assunto morre por
US$ 10** — sem construir método nenhum, sem stake, sem meses de forward. Se for significativo,
aí sim existe base para pré-registrar um método.

Consumo: 1 requisição por jogo no intervalo, só em ligas cobertas. ~10-20/dia = 300-600/mês.
Sobra folga enorme nos 20.000 do plano Pro.

## (D) Veredito

**Nenhuma das duas opções como colocadas.** A opção 1 (arquivar) descarta a única via da letra (a) do
NORTE — "modelo com informação que a odd não tem" — que é justamente onde mora o único edge candidato
do portfólio. A opção 2 (script piloto medindo CLV do xG) está certa no espírito, mas gasta engenharia
antes de saber se existe dado nas ligas certas.

**Sequência que eu recomendo:**

1. **Esta semana, custo zero:** varredura de cobertura (estágio 0). ~40 requisições da cota grátis.
   Se o resultado for "degrau 3 domina o fluxo do ARKAD", **acaba aqui** — e acabou de graça.
2. **Se passar:** US$ 9,99 e construir o **instrumento**, não o método. O `fotmob_map.csv` + o log de
   `X_HT` versus `P_HT`. Prazo até veredito: ~1 mês de coleta.
3. **Prioridade permanece o Lay 0x0.** Isto aqui é trabalho de *coleta de dado*, roda sozinho e não
   disputa atenção com o único edge candidato que temos. Não abrir um quarto método em stake-zero
   enquanto três esperam veredito.

**Sobre a relação sinal/ruído justificar a engenharia:** justifica no estágio 0 (grátis) e no
estágio 1 (US$ 10 e um logger). **Não** justifica construir método in-play com xG antes disso — pelo
argumento estrutural da letra (B), o cenário mais provável é que o xG só exista onde ele não serve.

⚠️ **Trocar a chave da RapidAPI** — ela foi colada em texto puro. Nova chave em `alerta.env`, que já
está fora do git.
