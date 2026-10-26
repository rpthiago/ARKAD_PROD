# Prompt para o Gemini — o favorito do Lay Draw é favorito na tabela?

Gemini, o Thiago quer a **sua** conclusão sobre isto, independente da minha. Eu descrevo abaixo o
método, os números e onde eu acho que posso estar errado. **Não tome a minha leitura como dada** —
se discordar, diga onde e por quê.

---

## 1. A pergunta do Thiago

Ele observou nos alertas ao vivo do Lay Draw que, às vezes, o favorito a 1,40 é **time de meio de
tabela**. Perguntou: dá para saber se o favorito por odd também era favorito na classificação? E se
isso explica parte da perda do método?

## 2. Como eu construí a tabela

Não existe fonte de classificação no repositório, então derivei da própria base:

- Base: `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` (52.116 jogos com placar).
- Para cada partida, calculo **pontos por jogo (PPG)** de cada time usando **somente jogos com
  `Date` estritamente anterior**, na mesma `League` + `Season`. Zero vazamento por construção.
- Métrica de posição: **percentil do favorito** dentro da liga-temporada, entre os times com ≥ 5
  jogos. `0,0 = líder`, `1,0 = lanterna`.
- Exijo ≥ 5 jogos de histórico para o time e ≥ 6 times elegíveis na liga, senão o jogo fica sem
  tabela.
- Usei PPG em vez de posição literal porque é robusto a cobertura parcial e a ligas com grupos
  (ITALY 3 tem 3 grupos; uma "tabela" única ali seria errada).

**Cobertura:** no universo Lay Draw de 2026, 332 de 399 jogos têm tabela utilizável (~83%). No
universo todo (2024-26), 976 de 1.312 (~75%). Os sem tabela são copas, início de temporada e ligas
pequenas.

## 3. Universo avaliado

Regra congelada de produção, lida de `sinais_ko_core.avaliar` (método `M_DRAW`):

```
governança: eh_jogo_ignorado(home, away, liga)
min(Odd_H_Back, Odd_A_Back) <= 1.40
4.50 <= Odd_D_Lay <= 7.00
spread do empate: Odd_D_Lay / Odd_D_Back <= 1.15
em COPA: só favorito mandante (Odd_H_Back <= 1.40)
```

**Não apliquei o piso de liquidez (`liq_lay >= R$ 200`)** porque a base da API não tem coluna de
liquidez. Isso é uma limitação real do teste — o universo aqui é mais largo que o do robô.

**Regra do projeto que vale aqui:** só 2026 conta. As odds e o sistema mudaram; 2024-2025 é
inadmissível como evidência. (Eu violei isso na primeira passada e o Thiago me corrigiu. Os números
abaixo são **só 2026**.)

## 4. Os números — somente 2026

### 4.1 O favorito por odd é o favorito da tabela?

| | 2026 |
|---|---:|
| jogos com tabela utilizável | 332 |
| favorito por odd **também** tem PPG maior que o adversário | **99,1%** (326/329) |
| percentil mediano do favorito | **0,11** (topo da tabela) |
| favorito **fora** do terço de cima | **13,7%** dos sinais |

### 4.2 Desempenho por faixa de percentil do favorito

| faixa | N | empate real | empate implícito na odd | diferença | odd lay média | ROI/liability | reds |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0,0–0,2 (topo) | 230 | 13,9% | 15,8% | −1,9pp | ~6,1 | **+2,17%** | 32 |
| 0,2–0,4 | 63 | 22,2% | 15,7% | +6,5pp | ~6,1 | −7,63% | 14 |
| 0,4–0,6 | 21 | 23,8% | 15,7% | +8,1pp | ~6,1 | −9,56% | 5 |
| 0,6–1,0 (baixo) | 18 | 16,7% | 15,6% | +1,1pp | ~6,2 | −1,40% | 3 |

**A odd de lay é praticamente constante (6,06–6,17) em todas as faixas** — o mercado precifica o
favorito a 1,40 igual, seja líder ou meio de tabela.

### 4.3 Agregados e testes

| célula | N | WR | BE | gap | z | ROI | reds |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2026 todos com tabela | 332 | 83,73% | 84,26% | −0,52pp | −0,26 | −0,63% | 54 |
| fav no terço de cima | 285 | 84,56% | 84,22% | +0,34pp | +0,16 | +0,40% | 44 |
| fav fora do terço de cima | 47 | 78,72% | 84,47% | −5,74pp | −1,09 | −6,86% | 10 |

- **Teste de tendência** (permutação de 20.000 sorteios sobre a correlação percentil × empate,
  só 2026): correlação = **+0,0603**, **P = 0,1356**.
- **Decaimento dentro de 2026**, célula "fora do terço de cima":
  1ª metade −16,70% (N=17, 5 reds) · 2ª metade −1,27% (N=30, 5 reds).

## 5. A minha leitura — e o que eu retirei

**O que eu concluí:** a direção é a que o Thiago suspeitou (favorito de meio de tabela rende pior, e
o mercado não cobra pela diferença), mas **nada é significativo em 2026** e a amostra fora do terço
de cima é de 47 jogos. Recomendei **gravar** o percentil da tabela em cada sinal e decidir depois,
em vez de cortar o filtro agora.

**O que eu retirei:** na primeira passada eu disse ao Thiago que a relação era **monótona** (taxa de
empate subindo 16,1% → 18,0% → 20,3% → 29,0%) e usei isso como argumento de que este filtro era
melhor que o teto 10 do Lay Zebra e que o Stop Red. **Com só 2026, a monotonicidade desaparece** —
a última faixa volta para 16,7%. A escada era efeito de misturar 2024-25. Retirei o argumento.

## 6. Onde eu posso estar errado (ataque aqui primeiro)

1. **PPG pode ser a métrica errada.** Talvez posição literal, PPG em casa/fora separado, ou força de
   ataque/defesa sirvam melhor. O favorito a 1,40 mandante contra lanterna é diferente de visitante
   contra meio de tabela, e eu não separei isso.
2. **O percentil mistura ligas de tamanhos muito diferentes.** Percentil 0,3 numa liga de 10 times
   não é a mesma coisa que numa de 24.
3. **Não apliquei o piso de liquidez** — o universo é mais largo que o do robô. Pode ser que os
   jogos de fav meio-de-tabela sejam justamente os de livro raso, e aí o filtro de liquidez já os
   remova e esta análise seja redundante.
4. **Exigir ≥ 5 jogos de histórico corta 17% dos sinais de 2026**, e esses 17% não são aleatórios
   (início de temporada, copas). Pode haver viés de seleção na direção do resultado.
5. **Agrupei faixas de percentil em quartos escolhidos por mim.** Outra divisão pode dar outra
   figura — e o fato de a figura mudar com a divisão já seria informação contra o achado.
6. **A célula do topo (+2,17%, N=230, z=+0,79)** pode ser o achado de verdade, e eu a tratei como
   secundária por não ser significativa. Talvez o enquadramento correto não seja "excluir meio de
   tabela" e sim "só entrar com líder".

## 7. Contexto necessário (resultados de hoje, 23/10)

- O `forward_ko_ledger.csv` começa em 16/09 mas as travas entraram em **08/10 e 13/10**. Lido cru, o
  Lay Draw dá −14,48u em 292 jogos; reclassificado contra a regra atual, **211 dos 292 jogos estavam
  fora da regra** e o método em produção é **−0,49u em 81 jogos** — que com erro-padrão de 4,02pp
  significa **não medido**, não aprovado. (Esse erro foi meu; você e o Thiago o pegaram.)
- Separando a amostra limpa pela data de deploy do filtro: antes de 08/10 **+5,54%** (37 jogos),
  depois **−5,76%** (44 jogos). Padrão de filtro post-hoc.
- Na base FRESH3, só 2026, a regra limpa do `M_DRAW` dá **−1,32% em 344 jogos** (esse corte também
  exigia o spread do lado da zebra; aqui sem isso o universo é 399, dos quais 332 com tabela,
  ROI −0,63%).
- O **Stop 1 Red** foi testado e não cria valor em nenhum método: permutação P=0,328 no Lay Draw e
  P=0,144 no Lay Home; dose não-monótona (stop de 2 reds é pior que não parar); e sem o dia 10/10 o
  ganho vai de +0,67u para −0,95u.
- O **Lay Home/DC X2** segue sendo o único método com gap positivo: +5,7pp, ROI +6,69% por entrada
  na amostra limpa (N=58).
- Quem aposta é o Thiago, na mão — o robô só alerta.

## 8. O que eu quero de você

1. **A métrica de tabela está certa?** PPG com corte de 5 jogos e percentil intra-liga é a forma
   adequada, ou tem uma melhor? Se tem, qual, e por quê.
2. **A sua conclusão sobre o achado**, com só 2026: é ruído, é sinal fraco que merece coleta, ou é
   forte o suficiente para entrar como filtro já? Justifique com teste, não com narrativa.
3. **O enquadramento certo:** "excluir fav fora do terço de cima" (corta 14% do volume) ou "só
   entrar com fav no topo da tabela" (corta 31%)? Ou nenhum dos dois?
4. **Algum dos 6 pontos da seção 6 derruba a análise?** Se sim, refaça o corte que você acha
   correto e mostre o número.
5. **Alguma leitura que eu não vi** nos dados acima.

## 9. Reprodução

- `auditoria_lay_zebra.py` — regra limpa do `M_DRAW` na FRESH3, ano a ano
- `auditoria_stop_red.py` — reclassificação do ledger, stop cronológico, permutação
- `stop_manual_por_metodo.py` — stop isolado por método com permutação
- `lay_draw_tabela.py` e `lay_draw_tabela_teste.py` — construção da tabela sem vazamento, cortes por
  faixa de percentil e teste de tendência por permutação

Comissão 5%, ROI sempre por liability (green `(1−c)/(odd−1)`, red `−1`), IC95 por bootstrap de
blocos-dia, regra de guarda de 5 reds por célula.
