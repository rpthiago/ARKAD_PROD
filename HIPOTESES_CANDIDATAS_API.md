# LISTA DE HIPÓTESES CANDIDATAS — dados FotMob/Opta via RapidAPI

**Montada em 2026-09-08.** Nenhuma foi testada ainda. **Esta lista é o pré-registro do lote:**
as hipóteses entram aqui *antes* de rodar, com mecanismo declarado; rodam juntas; e o corte de
significância é **Benjamini-Hochberg sobre o conjunto inteiro**, não hipótese a hipótese.

> **Por que a disciplina importa mais agora, não menos:** testar ficou barato (949 requisições
> mataram duas teses numa tarde). Barato para falsificar é barato para garimpar. Hoje mesmo, de
> ~12 especificações testadas, duas deram p<0,05 e **nenhuma sobrevive ao FDR**. Sem esta lista,
> a ferramenta vira fábrica de falso positivo.

---

## 0. O que temos de fato (inventariado em 847 arquivos de cache)

**Campos por jogo, com recorte de 1º tempo e de jogo completo** — 37 no total. Os que importam:

| Grupo | Campos |
|---|---|
| **xG decomposto** | `expected_goals`, `expected_goals_open_play`, `expected_goals_set_play`, `expected_goals_non_penalty`, `expected_goals_on_target` (xGOT) |
| Finalização | `total_shots`, `ShotsOnTarget`, `ShotsOffTarget`, `blocked_shots`, `shots_inside_box`, `shots_outside_box`, `shots_woodwork` |
| Ameaça real | `big_chance`, `big_chance_missed_title`, `touches_opp_box` |
| Território | `BallPossesion`, `opposition_half_passes`, `own_half_passes`, `accurate_passes`, `accurate_crosses`, `long_balls_accurate` |
| Defesa | `keeper_saves`, `clearances`, `interceptions`, `tackles`, `shot_blocks` |
| Disciplina/duelos | `yellow_cards`, `red_cards`, `fouls`, `Offsides`, `duel_won`, `aerials_won`, `ground_duels_won` |

**Cobertura:** xG em ~58% dos jogos casados (ligas do degrau 1); o resto do painel em ~100% dos
casados. `expected_goals_set_play` presente em 271 de 273 com xG.

**Do lado Betfair, de graça, já no coletor:** preço e tamanho de back/lay, `ltp`, e — crucial para
a H2 — **`matched` (volume casado)**, que mede a espessura do mercado.

**O que NÃO existe:** shotmap, timeline, momentum, escalações, árbitro. Logo **não há estatística
em minuto arbitrário** — só o corte do 1º tempo e o acumulado.

---

## 1. As hipóteses

### H1 — xG no lugar de gols como feature do Lay 0x0
- **Mecanismo:** gol é uma realização ruidosa do processo de criação de chances. Uma média móvel
  de xG estima o processo com muito menos variância do que a média de gols, com o mesmo N. Se o
  modelo do 0x0 melhora a estimativa de λ, melhora `P(0-0)` — que é literalmente o que ele prevê.
- **Teste:** re-treinar o XGB com features de xG rolling (a favor e contra, casa e fora) e comparar
  AUC/log-loss **out-of-sample** contra o modelo atual. Aprova se melhora com IC excluindo zero.
- **Dado:** xG por time por jogo, histórico. **Custo: ~7.000 requisições** (1 por jogo; ~1 temporada
  das ligas do degrau 1 onde o 0x0 opera).
- **⚠️ Regra:** o 0x0 atual está **congelado e em forward**. Isto gera um modelo **NOVO**, com
  pré-registro próprio, que **não pode tocar** no forward em curso.
- **Prior: o melhor da lista** — não procura edge novo, melhora o único que temos.

### H2 — Dado rico × mercado fino (a interação)
- **Mecanismo:** hoje sabemos que *onde o xG existe, o mercado já o tem*. Mas isso foi medido
  contra ligas de elite. O FotMob dá xG para **Colômbia Primera A, Brasileirão B, League One** —
  ligas onde o volume da Betfair é uma fração do de uma Premier League. Dinheiro afiado se
  concentra onde há liquidez; mercado fino é precificado por modelo mais fraco. **A assimetria não
  está no dado, está em quem modela o dado.**
- **Teste:** montar um modelo de xG rolling, medir o desacordo `modelo − preço`, e regredir o
  resultado sobre `desacordo × log(volume casado)`. **A hipótese vive se o termo de interação for
  significativo** — o desacordo prevê mais onde o mercado é mais fino.
- **Dado:** xG histórico (mesmo pull da H1) + `matched` do coletor.
- **Custo: 0 adicional** se a H1 rodar (reaproveita o mesmo pull).
- **Prior: a melhor ideia nova.** É a única que enfrenta a contradição estrutural do item 4 do
  Hall of Shame em vez de ignorá-la. Risco: volume da Betfair pode ser proxy de "liga pequena", e
  liga pequena pode ter xG de pior qualidade — confundidor a controlar.

### H3 — xG de bola parada não é igual a xG de bola rolando
- **Mecanismo:** `expected_goals_set_play` é muito menos repetível que `open_play` — depende de um
  escanteio específico, de altura, de um zagueiro. Um time cujo xG recente vem de bola parada é
  **mais fraco** do que o xG agregado sugere. Quem usa xG total (a maioria) não faz essa distinção.
- **Teste:** para prever gols futuros, `xG_open_play` rolling deve ter coeficiente maior que
  `xG_set_play` rolling. Se os coeficientes forem iguais, a decomposição não agrega.
- **Dado:** `expected_goals_open_play` / `_set_play`. **Custo: 0 adicional** (vem no mesmo payload).
- **Prior: bom.** Refinamento real e pouco explorado publicamente, e sai de graça.

### H4 — Superação de xG regride (finalização é sorte, o mercado ancora em gols)
- **Mecanismo:** `gols − xG` acumulado é dominado por variância de finalização. Times que vêm
  superando xG tendem a regredir; o mercado ancora no resultado recente (gols), não no processo.
- **Teste:** `(gols − xG)` das últimas N partidas prediz **under-performance** futura, controlando
  pelo preço. Sinal esperado: **negativo**.
- **Dado:** mesmo pull. **Custo: 0 adicional.**
- **Prior: morno.** É o achado mais documentado da literatura pública de xG — ou seja, o mais
  provável de já estar no preço. **Só vale testado junto com a H2** (talvez viva no mercado fino).

### H5 — Território estéril: domínio sem entrada na área
- **Mecanismo:** `opposition_half_passes` alto com `touches_opp_box` baixo = time que circula a
  bola no campo adversário sem criar ameaça. Narrativa e placar dizem "dominou"; o processo diz
  "não ameaçou". Se o mercado lê posse como ameaça, erra.
- **Teste:** a razão `touches_opp_box / opposition_half_passes` prediz gols futuros além de posse e
  xG. Se o xG já captura tudo, o coeficiente morre — e isso é uma resposta.
- **Dado:** `touches_opp_box`, `opposition_half_passes`, `BallPossesion`. **Custo: 0 adicional.**
- **Prior: morno-bom.** Provavelmente parcialmente redundante com xG, mas é barato descobrir.

### H6 — Defesa aguentando por milagre (`keeper_saves` vs xG concedido)
- **Mecanismo:** muitas defesas do goleiro com xG concedido alto = a defesa não está bem, está
  sendo salva. Isso não se repete. O mercado vê "poucos gols sofridos".
- **Teste:** `keeper_saves` e `xG_contra` recentes predizem **mais** gols sofridos à frente,
  controlando por gols sofridos.
- **Dado:** `keeper_saves` + xG do adversário. **Custo: 0 adicional.**
- **Prior: morno.** Muito correlacionado com H4 pelo lado defensivo — entra como variante, não como
  hipótese independente, para não inflar a contagem do FDR à toa.

### H7 / H8 / H9 — a mesma troca de feature nos alvos da tríade (Lay Home, Lay Draw, Lay Away)
- **Adicionadas em 2026-09-08, ANTES de olhar o resultado do lote.** Motivo: se o xG estima o
  processo de gols melhor que os próprios gols, o ganho não é específico do 0-0 — vale para
  qualquer alvo. A tríade (Draw/Home/Away) está hoje como *promissora, não aprovada*, e é o
  segundo conjunto de métodos vivos do portfólio.
- **Mecanismo:** idêntico ao da H1 — média móvel de xG tem muito menos variância que média de
  gols no mesmo N, então estima melhor a força ofensiva e defensiva que gera o resultado 1X2.
- **Teste primário:** igual ao da H1 — log-loss **fora da amostra**, na metade de validação,
  do modelo com features de xG contra o modelo com features de gols. Alvos: `mandante vence`
  (H7), `empate` (H8), `visitante vence` (H9).
- **Diagnóstico obrigatório (não é o p primário):** as features de xG sobrevivem **controlando
  pelo preço pré-jogo**? Para 1X2 o preço é preditor forte e já embute qualidade de time —
  ser "feature melhor" não é o mesmo que "bater o mercado". Um ganho de log-loss que evapora
  quando o preço entra no modelo significa: **melhora o modelo, não gera edge.**
- **Custo: 0 requisição adicional** — mesmo pull da H1.
- **Prior:** menor que o da H1. O 0-0 é evento raro numa cauda onde gol é ruído puro; o 1X2 é
  o mercado mais líquido e mais eficiente que existe. Espero ganho de log-loss real e ganho
  sobre o preço próximo de zero.

**⚠️ Consequência no FDR: M passa de 6 para 9.** O limiar de Benjamini-Hochberg da própria H1
fica mais duro. Isso é intencional — três alvos novos são três chances novas de falso positivo.

---

## 2. O que NÃO entra — e por quê

| Descartada | Motivo |
|---|---|
| Qualquer coisa in-play em minuto arbitrário (under-limite, Late Goal) | **Não existe timeline na API.** Só HT e FT. Impossível, não improvável |
| Stats do 1º tempo para decidir no intervalo | Já testado hoje: não bate nem a odd **pré-jogo** (xG p=0,951; SoT p=0,051 com sinal invertido). O `xg-ht` segue medindo contra o preço in-play, que é barra mais dura |
| Escalação, desfalque, árbitro | Endpoints não existem |
| Preencher lacunas de liga pequena | Medido: FotMob recuperou **1 de 292** (0%). Mesma fronteira de cobertura |
| Filtro de stat sem checar o flag | **6ª Lei.** Vira filtro de "liga grande" |
| `big_chance` +0,348 (p=0,026) de hoje | Achado de garimpo, não sobrevive ao FDR de ~12 testes. **Se entrar, entra como hipótese pré-registrada nova, testada em dado que não foi usado para descobri-la** |

---

## 3. Protocolo do lote

1. **Congelar esta lista** antes de rodar (H1-H9 = **M=9** hipóteses primárias).
2. Puxar o histórico **uma vez** (~7.000 requisições) — serve H1 a H6 inteiras.
3. **Split temporal**: descobrir/ajustar na primeira metade, **validar na segunda**, que não pode
   ser olhada antes.
4. **Benjamini-Hochberg com M=9** sobre os p-valores primários. Um p de 0,03 isolado **não** aprova.
5. Sobreviventes vão para **pré-registro individual + forward stake-zero**, como qualquer método.
   Nenhum vira código na VPS antes disso.
6. **CLV continua sendo o juiz final** de qualquer coisa que chegue a virar método.

**Orçamento:** ~7.000 de 20.000 requisições/mês. Sobra folga para o `xg-ht` (~3.000/mês) e para
reteste.

---

## 4. Recomendação de ordem

**H1 + H2 primeiro, juntas.** Elas compartilham o pull de dados, atacam o único edge existente e a
única contradição estrutural que ainda não foi enfrentada. H3 sai de graça no mesmo payload.
H4-H6 entram no mesmo lote porque não custam requisição adicional — mas com prior honesto de que
H4 provavelmente já está no preço.

**O que eu não faria:** abrir uma sétima hipótese in-play. O placar do in-play neste projeto é
0 aproveitamentos em ~8 tentativas, e a API não traz o dado que faltava para ele.
