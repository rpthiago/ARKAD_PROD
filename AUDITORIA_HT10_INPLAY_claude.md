# Auditoria adversarial — Over 0.5 HT in-play no minuto 10

**Auditor:** Claude (Opus 5) · **Data:** 26/10/2026 · **Objeto:** Combo A (Tier 1/2 + Fav Pré ≤1,70 +
odd in-play 1,40–1,75), N=54, ROI reportado +11,89%.

## Veredito: reprovar o Combo A. Manter o instrumento.

Reextraí o universo direto de `/home/ubuntu/betfair-collector/betfair_live_odds.csv` (4.163 jogos com
captura em `mtk ∈ [−18,−7]`, 27/09–26/10) e reliquidei pelo `ht05` do `placares_ft.csv`. O trabalho de
universo de vocês reproduz e está correto. O Combo A não sobrevive.

**O teste que decide:** permutação sobre a grade inteira de fatiamento, 2.000 sorteios, jogos e odds
fixos, só o desfecho embaralhado.

| | margem |
|---|---:|
| melhor célula **observada** nos dados reais | **+16,86pp** |
| melhor célula sob H0 (sem edge em lugar nenhum) — **mediana** | **+18,10pp** |
| melhor célula sob H0 — p95 | +27,19pp |
| **P(melhor sob H0 ≥ observado)** | **0,589** |

A melhor célula que os dados reais produzem é **pior que a mediana** do que o puro acaso produz numa
busca do mesmo tamanho. O espaço varrido tem **691 células elegíveis** (7 subconjuntos de tier × 9
cortes de favorito × 10 faixas de odd × 2 períodos, exigindo N≥30 e ≥5 reds).

O Combo A isolado dá **P = 0,0185**, que parece aprovado. É exatamente a armadilha: ele foi
*escolhido* entre 691 candidatos, então o P que vale é 0,589.

---

## 1. O que está certo no estudo — e é bastante

**A janela está correta.** `mtk ∈ [−18,−7]` é mesmo o minuto 7 ao 18, mediana **10,6**. Confirmei a
convenção medindo no arquivo: `capture_ts=2026-10-25 20:30:36`, `ko=20:00`, `min_to_ko=-31.0` — 30,6
minutos de relógio após o apito, sem offset. Vale registrar que a "fórmula canônica" do repositório
(`minuto = -min_to_ko - 15`, em `trader_inplay_engine.py:10`) **só vale do segundo tempo em diante**:
o `−15` desconta a pausa do intervalo, que no minuto 11 ainda não aconteceu. Vocês acertaram a janela;
a fórmula do repositório é que está incompleta.

**O teste do espelho está certo e é a melhor coisa do estudo.** Reproduzi:

| | N | WR | BE | margem | ROI |
|---|---:|---:|---:|---:|---:|
| Back Over 0.5 HT | 2.361 | 63,96% | 66,21% | **−2,25pp** | −5,13% |
| Back Under 0.5 HT | 2.361 | 36,04% | 41,19% | **−5,15pp** | −10,16% |

Os dois lados perdem; a soma é o overround de **~7,4pp**. Isso é uma medição real e reutilizável: o
mercado de HT in-play da Betfair cobra 7,4pp, e qualquer método ali tem que vencer isso primeiro.

**Vocês gravaram liquidez.** Depois do histórico do projeto nisso, é mérito. `back_size` mediano no
lado Over: **R$ 451**, p25 R$ 175, p10 R$ 93.

**E não repetiram o erro do Radar HT** (pergunta 4). O Radar HT morreu por assumir odd in-play
teórica; aqui a odd é coletada, real, com tamanho ao lado. A falha deste estudo é outra, e é de
inferência, não de dado.

## 2. O universo: negativo, e reproduz

| | N | WR | BE | margem | ROI |
|---|---:|---:|---:|---:|---:|
| meu, todos os casados | 3.734 | 67,46% | 70,40% | −2,94pp | −5,39% |
| meu, só os que ainda estavam 0-0 | 2.361 | 63,96% | 66,21% | −2,25pp | −5,13% |
| **reportado por vocês** | 3.297 | 65,64% | 68,45% | −2,82pp | −4,31% |

## 3. 37% do universo já tinha gol na hora da captura

Usando o estado factual pela linha Over 0.5 FT (a regra do próprio
`trader_inplay_engine.py`: *"Linhas O/U batidas, NUNCA pelo menor lay de CS"*):

- **1.373 de 3.734 jogos tinham `Odd_Over05_FT_Back ≤ 1,05`** na captura — ou seja, o gol já havia
  saído. Nesses, o WR é 73,49% e a odd média de HT é 1,30.
- Sobram **2.361** jogos realmente em 0-0.

O N de vocês (3.297) cai entre 2.361 e 3.734, o que indica que o filtro "chega aos 8-16 minutos **em
0-0**" não foi aplicado de forma factual. Isso não salva nem afunda o método — o universo é negativo
nas duas versões — mas significa que o objeto medido não é o objeto descrito, e misturar jogo já 1-0
com jogo 0-0 contamina toda a tabela por faixa de odd.

## 4. O achado de tier inverte de sinal quando se redesenha os baldes

Pergunta 3 pedia a explicação mecânica de a Tier 2 ir bem e a Tier 1 ir mal. A resposta honesta é
que a premissa não sobrevive a um mapeamento de tier diferente e igualmente defensável:

| | minha classificação | a de vocês |
|---|---:|---:|
| Tier 1 | **+2,45pp** (N=153) | −9,15% (N=292) |
| Tier 2 | **+1,42pp** (N=235) | +6,18% (N=166) |
| Tier 3 | **−3,05pp** (N=1.973) | −4,42% (N=2.839) |

A minha dá **monótona** (elite melhor, periférica pior); a de vocês dá **serra** (negativo, positivo,
negativo). Um mecanismo real não muda de forma quando se troca quais ligas entram em qual balde — e a
diferença aqui é só isso, porque a classificação de tier não tem definição canônica no projeto. A
serra é a assinatura de parâmetro ajustado a ruído, e já foi o que derrubou o teto-10 do Lay Zebra.

Não vou inventar uma explicação mecânica para um padrão que não é estável.

## 5. A fronteira de odd do combo é parâmetro livre

Por faixa, no meu universo 0-0:

| faixa | N | margem | ROI | reds |
|---|---:|---:|---:|---:|
| [1,15, 1,40) | 80 | −6,82pp | −10,33% | 26 |
| **[1,40, 1,60)** | 1.345 | **+1,58pp** | +0,58% | 427 |
| [1,60, 1,80) | 653 | −6,83pp | −13,29% | 307 |
| [1,80, 2,10) | 131 | −5,23pp | −11,80% | 68 |

A única faixa perto de zero é 1,40–1,60. O combo usa **1,40–1,75**, que invade a faixa de −6,83pp. O
corte em 1,75 não coincide com nenhuma fronteira reportada — é escolha livre, e entra na grade de 691
células.

## 6. A latência in-play é de mão única e come a margem inteira

Pergunta 2. Modelei com Poisson a partir dos próprios dados: captura mediana no minuto 10,6, WR até o
HT de 63,96%, ~36,4 min restantes → **λ = 0,0280 gol/min**.

| atraso até conseguir preço | P(gol no atraso) | WR efetivo | margem |
|---|---:|---:|---:|
| 0 (o snapshot) | — | 63,96% | −2,25pp |
| 1 min | 2,76% | 62,93% | **−3,28pp** |
| 2 min | 5,45% | 61,88% | **−4,33pp** |
| 3 min | 8,07% | 60,79% | −5,41pp |
| 5 min | 13,08% | 58,53% | −7,67pp |

O ponto importante é a **direção**: você só deixa de entrar quando o gol sai antes de você pegar
preço, e esses jogos eram **todos GREEN**. O mercado suspende, liquida, e a aposta que você queria
não existe mais. Então o viés nunca é a seu favor: **cada minuto de atraso tira ~1,05pp do WR.**

Um snapshot estático de coletor não mede isso. E para um operador manual — alerta chega, abre a
Betfair, confere, aposta — 1 a 2 minutos é o cenário realista, não o otimista.

## 7. Liquidez: o único filtro com mecanismo, e ainda assim negativo

| piso em `back_size` | N | margem | ROI |
|---|---:|---:|---:|
| sem piso | 2.361 | −2,25pp | −5,13% |
| ≥ R$ 100 | 2.090 | −1,76pp | −4,55% |
| ≥ R$ 200 | 1.707 | **−0,97pp** | −3,32% |
| ≥ R$ 500 | 1.097 | −0,93pp | −3,39% |

O piso de liquidez melhora a margem de forma monótona e satura — comportamento de mecanismo real, e é
o mesmo achado que o projeto já validou em outros mercados. Mas satura em **−0,97pp**, ainda negativo.
É a melhor pista do estudo e não chega a zero.

---

## Respostas pontuais

**1. Garden of forking paths.** É cauda de variância, e dá para dizer com número: **P = 0,589** na
permutação sobre a grade de 691 células. Sob H0 a melhor célula de uma busca desse tamanho rende
+18,10pp na mediana; vocês acharam +16,86pp. O Combo A não é um achado, é o máximo esperado de uma
busca. Nota técnica: ele passa a regra de guarda (13 reds), então o problema não é N=54 — é
multiplicidade.

**2. Microestrutura e latência.** Afeta de mão única e na escala do efeito: 1 min de atraso = −1,05pp
de WR, 2 min = −2,08pp. A margem do universo vai de −2,25pp para −4,33pp. Um snapshot de coletor
superestima sistematicamente qualquer método de back in-play, porque os greens que você perde são
justamente os que o gol chegou primeiro. Qualquer forward precisa gravar o **momento do alerta** e o
**momento da entrada** para medir isso em vez de modelar.

**3. Assimetria de tiers.** Não há explicação mecânica a dar, porque o padrão não é estável: com uma
classificação de tier igualmente razoável o sinal **inverte** e fica monótono a favor da elite. Antes
de explicar, é preciso fixar uma definição canônica de tier no projeto e pré-registrá-la. Sem isso,
"tier" é mais um grau de liberdade da grade.

**4. Comparação com o Radar HT.** Não comete o erro do Radar HT. Aquele assumiu odd in-play teórica;
este usou odd real coletada com tamanho. Tecnicamente é um nível acima. O erro aqui é de inferência —
fatiar 691 células e reportar a melhor sem corrigir — e o espelho de vocês, que é correto, já dizia a
resposta: os dois lados perdem, o mercado não está errado.

**5. Veredito — (C), uma terceira via.**

**(A) para o Combo A: reprovado.** Não por N=54, mas por P=0,589 na grade.

**Não (B):** não mandar para quarentena stake-zero. Uma quarentena observa uma hipótese; aqui a
hipótese é "a melhor de 691 células", cuja distribuição nula tem mediana +18pp. Acumular 250 jogos
numa célula escolhida assim não valida nada — só gasta dois meses e produz um número que vai ser
reinterpretado. A aritmética de potência até fecha (para detectar +7,8pp com 80% de poder seriam
N≈280, então o N≥250 de vocês é numericamente sensato) — mas potência só serve quando a estimativa
pontual não é ruído, e esta é.

**O que fica, e vale:** o estudo entregou um instrumento, não um método. A medição de que o **HT
in-play da Betfair cobra 7,4pp de overround** é sólida, reprodutível e reutilizável — é o número que
qualquer ideia futura nesse mercado tem que vencer. E o piso de liquidez é a única pista com
mecanismo (−2,25pp → −0,97pp, monótona, saturando). Registrem as duas coisas.

**Se for para pré-registrar algo**, que seja **uma** hipótese, escolhida por mecanismo e declarada
antes de olhar — e com estas três exigências, que este estudo não tem:
1. estado do jogo pela linha O/U batida, não pela odd (hoje 37% do universo já tinha gol);
2. piso de liquidez no lado em que se aposta, declarado antes;
3. gravar `ts_alerta` e `ts_entrada` para **medir** a latência em vez de modelá-la.

Com overround de 7,4pp e a melhor estimativa honesta de edge em ~0, a minha recomendação é não
pré-registrar nada aqui agora. O mercado de HT in-play no minuto 10 está eficiente dentro do vig, e
isso é o achado.

---

### Reprodução

- `aud_ht10.py` — universo, espelho, faixas de odd, piso de liquidez
- `aud_ht10_combo.py` — grade de 691 células e permutação (2.000 sorteios)
- `aud_ht10_lat.py` — dose-resposta por tier e custo da latência
- `extrai_ht10.py` — extração na VPS (streaming, 1ª captura elegível da janela)

Comissão 5%. Back: stake 1u = risco 1u, green `(odd−1)×0,95`, red `−1`. BE `1/odd`. Liquidação pelo
`ht05` do `placares_ft.csv` (autoritativo Betfair, 98,1% de cobertura). Estado do jogo pela linha
Over 0.5 FT. Regra de guarda de 5 reds por célula.
