# AUDITORIA ADVERSARIAL — "Lay Away Fortaleza 1X" e o chaveamento com o Lay Draw — Claude, 2026-09-24

Base única (Lei 7): `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` + `metodos_aprovados/.cache_base_betfair.csv`
(dedup por Data+Home+Away → **54.558 jogos**, 16/03/2024 → 18/09/2026) e, para o teste na operação real, o ledger das
06:00 (`metodos_aprovados/forward_5metodos_ledger.csv`) cruzado com os feeds diários arquivados. Coletor não usado.
Convenção: lay com liability 1u (GREEN = (1−c)/(odd−1), RED = −1, BE = (odd−1)/(odd−c)). Bootstrap bloco-dia 10.000×.

## Resposta 1 — Os números reproduzem? Sim, quase exatamente. Mas um IC foi reportado errado.

| cenário | N | G/R | WR | BE | margem | P&L | ROI | IC95 |
|---|---|---|---|---|---|---|---|---|
| Over25≥1,80, **comissão 5%** | 370 | 346/24 | 93,51% | 90,86% | +2,65 pp | +11,01u | +2,98% | **[−0,02, +5,68] p=0,026** |
| Over25≥1,80, comissão 3,5% | 370 | 346/24 | 93,51% | 90,73% | +2,78 pp | +11,56u | +3,12% | [+0,12, +5,83] p=0,022 |

O relatório apresenta **[+0,12%, +5,69%] como sendo o IC a 5% de comissão** — esse é o IC de 3,5%. A 5% de comissão,
que é o que você paga na Betfair BR, **o piso do IC é −0,02%: encosta em zero.** A diferença entre "exclui o zero" e
"encosta no zero" é exatamente a diferença entre aprovar e não aprovar pela Lei 4/8.

Semestres: 6/6 positivos, confirmado (+6,25% / +3,21% / +4,37% / +1,57% / +1,06% / +2,42%) — mas com N de 20 a 119 por
semestre e com **tendência de queda** (2024-H1 +6,25% → 2026-H1 +1,06%), o oposto de "estável".

Duelo nos 297 jogos sobrepostos: confirmado. Lay Draw −2,03% (240G/57R) vs Lay Away 1X +1,72% (278G/19R), diferença
+11,14u. Refiz também com a banda de **produção** do Lay Draw (4,5–10,0, e não 2,5–8,5 como no relatório): 296 jogos,
Draw −2,23%, Away +2,07%. A conclusão do duelo não depende da banda. Sobreposição 80,3% / 80,0%: confirmada.

## Resposta 2 — Mecanismo vs. data mining: **é data mining, e tenho três provas**

### (a) FDR sobre a grade que foi realmente pesquisada: 0 sobreviventes
Testei 61 combinações com N≥100 (Over25 em 6 níveis × faixa de lay em 4 × favoritismo em 3):
**13 têm p<0,05 sem correção e ZERO passam BH-FDR q=0,05.** O corte proposto tem p=0,026; o melhor da grade,
p=0,011. Procurar em 61 células e achar 13 com p<0,05 é aproximadamente o que o acaso produz (esperado ~3 por acaso,
mas as células são fortemente correlacionadas entre si, o que infla a contagem). Pela Lei 4 ("FDR contra tudo já
testado"), nada aqui foi aprovado.

### (b) O filtro de copas não faz o que o relatório diz — e custa dinheiro
Os números de copa citados (−12,78% em N=73 e −4,12% em N=69) são da **regra antiga e ampla** (lay 2,0–15,0). Dentro da
**regra nova**, copa tem **7 jogos** e seleção **26** — e os dois são positivos:

| recorte (regra nova) | N | ROI |
|---|---|---|
| só COPA | 7 | +8,34% |
| só SELEÇÃO | 26 | +3,76% |
| **liga nacional (proposto)** | 370 | +2,98% (p=0,026) |
| **todos os tipos juntos** | 403 | **+3,12% (p=0,016)** |

Incluir copas e seleções **melhora** o resultado e o p-valor. O bloqueio foi desenhado olhando os reds da regra antiga e,
na regra nova, remove jogos lucrativos. É o padrão do Hall of Shame: filtro retrospectivo de competição.

### (c) A "estabilidade em thresholds vizinhos" é de sinal, não de significância
O sinal não inverte (bom), mas o edge evapora em toda direção que importa:

| eixo | valores | ROI (p) |
|---|---|---|
| favoritismo | H≤1,30 · **1,40** · 1,45 · 1,50 | +0,08% (0,48) · **+2,98% (0,026)** · +1,49% (0,11) · +0,56% (0,30) |
| linha de gols | ≥1,70 · 1,75 · **1,80** · 1,85 · 1,90 · 2,00 | +1,77% (0,10) · +2,28% (0,05) · **+2,98%** · +3,67% (0,019) · +2,83% (0,084) · +2,53% (0,20) |
| faixa de lay | 3,0–15 · 4,0–15 · **4,5–15** · 5,0–15 · 4,5–12 · 4,5–20 | +1,16% (0,23) · +2,81% (0,03) · **+2,98%** · +3,11% (0,020) · +2,35% (0,18) · +1,95% (0,067) |

O edge existe numa janela estreita (H entre 1,35 e 1,40, lay 4,5–5,0 a 15). H≤1,30, que deveria ser o **melhor** caso
pelo mecanismo descrito (favorito ainda mais forte), dá **+0,08%**. Isso contradiz o mecanismo: se a tese é
"λ do visitante colapsa quando o mandante é muito favorito", o efeito deveria crescer com o favoritismo, não desaparecer.

### (d) O mecanismo tem um problema lógico
A tese diz que `Over 2.5 ≥ 1,80` (jogo amarrado) faz o empate subir a 19,2% e o visitante não marcar. Mas a taxa de
empate de 19,2% nos 297 jogos é praticamente a taxa base de empates do futebol (~20-25%) — não é uma anomalia. O que o
filtro faz de fato é selecionar jogos com **menos gols esperados**, onde o resultado 1 ou X é mais provável — e laying
o visitante ganha em 1 **e** em X. Isso não é ineficiência do mercado: é escolher a aposta com maior probabilidade de
acerto e menor prêmio, e o BE já cobra isso (90,86%). A margem de +2,65 pp é o que resta, e é da mesma ordem do erro
de estimativa.

## Resposta 3 — Qual threshold? **Nenhum dos dois, pelo que a sua operação real mostra**

Cruzei o ledger das 06:00 (**376 sinais reais de Lay Draw**, 01/08 → 22/09, ROI real +2,35%) com o feed do dia para
saber quantos caíram na zona de chaveamento (liga nacional, H≤1,40, Over25≥1,80, lay do visitante 4,5–15):

| recorte dos SEUS jogos | N | Lay Draw (real) | Lay Away 1X (contrafactual) |
|---|---|---|---|
| **na zona (chavearia)** | **14** | 3 reds, **−4,42%**, −0,62u | 1 red, **+0,38%**, +0,05u |
| fora da zona (segue Draw) | 302 | 34 reds, **+3,29%**, +9,95u | 69 reds, **−4,37%**, −13,20u |
| total casado | 316 | 37 reds, +2,95%, +9,33u | 70 reds, −4,16%, −13,14u |

- **A zona pega 14 dos 316 jogos (4,4%).** O chaveamento mudaria o total de +9,33u para +10,00u: **+0,67u em 53 dias**,
  decidido por 4 jogos (3 empates que virariam green e 1 vitória da zebra que viraria red). Com N=14 isso é ruído puro.
- **Fora da zona, trocar Draw por Away seria catastrófico** (−4,37% contra +3,29%): confirma que a proposta é específica
  da zona, mas também mostra que o Lay Away não é um método melhor — é um método diferente que só ganha onde o empate
  é mais provável.
- Por mês, dentro da zona: agosto Draw +9,49% vs Away +8,27% (Draw ganha); setembro Draw −39,18% vs Away −19,32%
  (Away perde menos). Não há sinal, há 4 jogos por mês.

**Ponto crítico de coerência de base:** na base da API, o Lay Draw de produção inteiro dá **−1,06%** (N=3.586) e
**−0,95% fora da zona** (N=3.290). Ou seja, a base que sustenta o chaveamento é a mesma que diz que o Lay Draw perde
em todo lugar — enquanto o seu forward real diz +2,35% em 376 apostas. Usar a base para tirar o Draw de um recorte é
confiar, naquele recorte, exatamente na fonte que discorda do seu resultado ao vivo.

## Resposta 4 — Veredito operacional: **NÃO implementar agora. Observação em stake-zero.**

1. **Não aprovar o Lay Away Fortaleza 1X:** a 5% de comissão o piso do IC encosta em zero (−0,02%), zero células
   sobrevivem ao FDR da grade pesquisada, o filtro de copas é post-hoc e prejudicial, e o mecanismo não se confirma
   no eixo do favoritismo (H≤1,30 → +0,08%).
2. **Não ligar o chaveamento nas páginas 01/02 e na automação:** ele afeta 4,4% dos seus jogos e vale +0,67u em quase
   dois meses. O ganho de R$166–R$253 no B7 é ~3–4% do resultado final com maxDD de 31% — está dentro do ruído da
   simulação, e a própria simulação usa a banda de Draw 2,5–8,5, que não é a de produção.
3. **O que fazer, que é barato e honesto:** pré-registrar o Lay Away Fortaleza 1X como **sinal em stake-zero** na
   página 03, com a regra congelada exatamente como está (liga nacional OU todas as competições — escolha uma e
   declare), N-alvo ≥ 300 no forward, e o critério: aprova se o piso do IC95 a **5%** de comissão > 0 com ≥ 20 reds;
   reprova se o teto < 0 ou ROI < −2% com N ≥ 150.
4. **Se quiser mesmo testar o chaveamento**, o teste barato é registrar os dois lados em paralelo nos jogos da zona
   (Draw com dinheiro, Away em stake-zero) e comparar em 100 jogos da zona — o que, no seu ritmo de 4 jogos por mês
   na zona, leva ~2 anos. Isso por si só responde se vale a complexidade: não vale.

## Observações de método para o Antigravity
- Sempre reportar o IC **na comissão que o Thiago paga** (5%). Reportar o de 3,5% como se fosse o de 5% mudou o veredito.
- Quando um filtro é criado a partir dos reds de uma regra anterior, testar o filtro **dentro da regra nova** antes de
  afirmar que ele protege: aqui ele removia jogos com ROI de +8,34% e +3,76%.
- Grade pesquisada exige FDR. 61 células, 13 com p<0,05, 0 sobreviventes.
- O comparativo com o Lay Draw deve usar a banda de produção (4,5–10,0). Neste caso a conclusão não mudou, mas a
  simulação B7 foi feita com a banda errada.
