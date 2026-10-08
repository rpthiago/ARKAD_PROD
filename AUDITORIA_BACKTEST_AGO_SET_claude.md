# Auditoria — backtest do fluxo Bet365 ➔ OrbitX (01/08 a 24/09/2026)

**Veredito curto:** o script está correto, o ponto do Thiago sobre o regime da base está certo e eu
corrijo parte da auditoria anterior. Mas os dois métodos "aprovados" **não são estatisticamente
distinguíveis de zero** (p = 0,22 e p = 0,19), e a 14% de comissão os dois desaparecem.

---

## 1. Primeiro, onde eu estava errado

O Thiago levantou que a base mudou de regime em 2026. **Está certo, e é verificável.** Medindo o
spread *interno* da Betfair (lay ÷ back, que não depende da Bet365) nos jogos casados:

| ano | n | odd de lay mediana | spread vs b365 | **spread interno (lay/back)** |
|---|---:|---:|---:|---:|
| 2024 | 15.761 | 4,00 | 1,1250 | **1,0735** |
| 2025 | 22.950 | 3,95 | 1,1250 | **1,0725** |
| 2026 | 11.537 | 3,85 | 1,1111 | **1,0441** |

O livro de 2026 é materialmente mais apertado — snapshot mais perto do apito. Isso não é opinião, é a
própria exchange consigo mesma.

E isso muda o meu resultado anterior. Refazendo o mesmo teste (Super Fav ≤ 1,40, empate b365 ≥ 5,00,
trava lay ≤ 7,50, c = 5%), **por ano**:

| período | N | WR | break-even | ROI |
|---|---:|---:|---:|---:|
| 2024 | 662 | 83,69% | 84,27% | −0,72% |
| 2025 | 950 | 82,53% | 84,23% | −2,00% |
| **2026 (até 31/07)** | 408 | 84,80% | 84,69% | **+0,20%** |

Meu −1,14% agregado era puxado por 2024-25. No regime de 2026 o método é **zero**, não negativo.
Isso enfraquece a parte da minha auditoria que tratava o resultado como evidência contra — e eu
retiro essa leitura. Não transforma em evidência a favor: +0,20% com N=408 é ruído em torno de zero.

## 2. O script: limpo nos pontos mecânicos

Auditei `backtest_fluxo_agosto_setembro.py` linha a linha e verifiquei o que dava para verificar:

| item | resultado |
|---|---|
| condição de green do Lay Draw (`!=`) | correta |
| condição do Lay Home (`Goals_H <= Goals_A`) | correta |
| condição do Lay Away (`Goals_H >= Goals_A`) | correta |
| liability `(1−c)/(odd−1)` no green, −1 no red | correta |
| break-even `(odd−1)/(odd−c)` | correto |
| duplicação de chave no merge | **0 duplicadas** nas duas bases (4.180 e 8.652 linhas) |
| look-ahead / vazamento | **não encontrei** — o filtro usa só odd pré-jogo e o teto usa a odd de lay observável antes de entrar |

Não há vazamento. O desenho do fluxo é honesto: seleciona pela Bet365, confirma no preço real da
exchange, e o teto é uma decisão tomável antes de comprometer dinheiro.

Duas ressalvas, nenhuma delas um bug:

**O número de manchete está na unidade errada.** `+44,52 u` e `+77,34 u` usam a convenção de stake 1u,
mas o capital em risco por entrada é (odd − 1) ≈ 4 a 6u. É a mesma convenção que fez o Lay 0x0 parecer
+34% quando o edge real era 1,1pp. Os números honestos são os que o próprio script já imprime:
**+1,57% e +3,51% por liability**. Sugiro aposentar a linha nominal do relatório.

**A liquidação não é "pelo placar oficial".** Os gols vêm da base Bet365 (as colunas de gols da Betfair
não entram no merge). Não é errado — é uma fonte só, o que é bom — mas a descrição não bate.

## 3. O problema decisivo: nada disso é distinguível de zero

O script não calcula significância. Calculando:

| método | N | edge (WR − BE) | erro-padrão do WR | z | p (unilateral) | N necessário p/ significância |
|---|---:|---:|---:|---:|---:|---:|
| Lay Draw ≤ 7,00 | 426 | +1,39 pp | 1,84 pp | **0,76** | **0,224** | **2.854** |
| Lay Home ≤ 8,00 | 271 | +2,24 pp | 2,58 pp | **0,87** | **0,193** | **1.382** |

Um edge de 1,39 pp medido com erro-padrão de 1,84 pp não é um edge medido — é um número compatível
com zero, com +3 pp e com −1 pp. Nos 55 dias entraram ~7,7 jogos por dia no cohort do Lay Draw; para
chegar aos 2.854 jogos necessários seriam **cerca de 370 dias**. É esse o tamanho do problema.

**E foram 13 células testadas**, não duas: tetos [6,50 / 7,00 / 7,50 / 8,00 / 10,00] no Draw,
[12 / 15 / 20 / 25] no Away e [6 / 7 / 8 / 10] no Home. Escolher as duas melhores de 13 e reportar só
elas, sem FDR, é exatamente o procedimento que produz p ≈ 0,2 por acaso. O teto do Lay Draw, aliás,
já migrou de 7,50 (proposta anterior) para 7,00 — parâmetro reajustado entre auditorias, no mesmo
período de dados.

## 4. A comissão decide sozinha

Derivando a odd representativa do break-even informado e recalculando:

| método | odd repr. | c = 3% | c = 5% | **c = 14%** |
|---|---:|---:|---:|---:|
| Lay Draw | 5,20 | +1,71% | +1,32% | **−0,45%** |
| Lay Home | 3,78 | +3,02% | +2,47% | **0,00%** |

A 14% — a comissão que você mediu em duas apostas reais no OrbitX via BET-IBC — o Lay Draw fica
negativo e o Lay Home fica **exatamente** em zero. Toda a tese depende de os 3% serem verdadeiros.
Essa é a pergunta mais barata de responder e a de maior impacto: um extrato resolve.

---

## 5. Respostas diretas

**1. Vazamento, look-ahead ou cálculo indevido?** Não encontrei vazamento nem look-ahead, e a
matemática de liability e comissão está certa. Os defeitos são de *relato*: unidade inflada na
manchete, ausência de teste de significância e de FDR sobre as 13 células, e a descrição da
liquidação.

**2. É a melhora da base ou variância de início de temporada?** As duas coisas, e dá para separar
parcialmente. A melhora da base é real e medida (spread interno 1,073 → 1,044). Mas ela explica o
resultado sair de negativo para **zero**, não de zero para positivo. O que sobra acima de zero
(+1,39 pp) está dentro de 0,76 erro-padrão — ou seja, indistinguível de variância. Início de
temporada europeia tem ainda o agravante de elencos e forma não assentados, que é justamente quando
o favoritismo de mercado erra mais em ambas as direções.

**3. O corte manual se sustenta como disciplina de mesa?** Sim, e é a melhor parte do desenho — mas
como **controle de risco**, não como gerador de edge. Ele garante que você nunca aceita um preço pior
que o planejado, o que elimina a classe de erro que produziu três falsos edges no ARKAD. Mantenha.

**4. Aprovo micro-stake de US$ 2 a 5?** **Condicionalmente, e não como método aprovado.**

Não aprovo como operação. Aprovo como **forward pago pré-registrado**, se e somente se:

- **A comissão for confirmada em 3% por extrato, antes da primeira entrada.** Se for 14%, não comece:
  o próprio backtest diz que não há o que capturar.
- **As regras forem congeladas hoje, por escrito**: Lay Draw com teto 7,00 e Lay Home com teto 8,00,
  filtros de radar como estão, sem reajuste de teto durante o teste. Se um teto mudar, o relógio
  zera — foi o que aconteceu entre 7,50 e 7,00.
- **O N-alvo for declarado antes**: 1.400 entradas no Lay Home e 2.850 no Lay Draw para um veredito
  estatístico. Com isso você entra sabendo que são ~12 meses, e não vai concluir nada em 3 semanas.
- **O acompanhamento semanal de ΔWR servir só para detectar desastre, não para decidir.** Com N
  semanal de ~50, o ΔWR semanal oscila ±5 pp por acaso. Use-o como alarme de quebra (ex.: parar se o
  acumulado cair abaixo de −3% por liability), nunca como confirmação.
- **Registrar a odd executada, não a planejada**, em toda entrada.

A US$ 2–5 de risco por entrada, o custo de aprender é de ordem de uma a duas centenas de dólares ao
longo do ano. Isso é um preço razoável por uma resposta que a base não consegue dar — desde que você
entre com a expectativa correta: **o valor esperado hoje é zero**, e o objetivo é medir, não lucrar.

---

## 6. Uma pendência da auditoria anterior que fica em aberto

No documento anterior eu reportei que a regra congelada do Lay Draw (`liga_draw_rate < 0,23`) dava
−3,49% com odd de lay real. Aquele número usou **todos os anos**, e portanto carrega o mesmo viés de
regime que acabei de corrigir aqui. Antes de qualquer conclusão sobre o método de produção, aquilo
precisa ser refeito só em 2026 e com a definição exata de `liga_draw_rate` do código de produção.
Continua sendo, na minha opinião, a pergunta mais importante em aberto.

*Base: `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` × `..._Betfair.csv` por (Data, Home, Away),
50.248 jogos de 16/03/2024 a 31/07/2026. Significância por teste z de proporção contra o break-even;
ROI por liability `(1−c)/(odd−1)` no green e −1 no red.*
