# Auditoria pedida — filtro de spread no Lay Draw

Contexto: a sugestão anterior do Claude (filtro `Over 2.5 >= 1,60`) foi testada por vocês e **reprovada**
— inverteu entre Jan-Jul e Ago-Out e deu negativo com odd de lay real. Antes de levar adiante o achado
abaixo, queremos a mesma pressão em cima dele.

**Não quero confirmação. Quero o que derruba.**

---

## 1. O que foi testado

Seis hipóteses no Lay Draw de 2026, cada uma declarada antes de olhar o resultado, com validação em
duas metades do ano (H1 = 01/01 a 15/05, H2 = 16/05 a 24/09). Quatro morreram:

| hipótese | H1 | H2 | veredito |
|---|---|---|---|
| Over 2.5 barato é melhor | faixa <1,60 dá −3,24% | −1,34% | **direção invertida** vs o que foi suposto |
| `liga_draw_rate < 0,23` (regra congelada antiga) | faixa 0,21–0,24 dá −1,44% | −2,85% | **corte não se sustenta** |
| favorito curto é melhor | 1,18–1,26 dá −3,84% | +4,12% | **inverte, é ruído** |
| divisão principal vs secundária | principal +1,35% | principal −1,90% | **inverte, é ruído** |

Duas sobreviveram, e são a mesma coisa medida de dois jeitos (correlação **−0,91**, 664 dos 734 jogos
coincidem): o **spread do próprio mercado de empate** (`Odd_D_Lay / Odd_D_Back`) e a **folga de EV**
calculada tirando o overround das três odds de back.

## 2. Os números do spread

Base: `.cache_base_betfair.csv`, só 2026, governança de ligas do `sinais_ko_core.py` aplicada
(feminino, seleção, divisão baixa, periférica), regra de copas (só mandante), janela de lay 4,5–10,0.
Comissão 5%, liability 1u. **843 jogos.**

| faixa de spread | H1 (n / ROI) | H2 (n / ROI) |
|---|---|---|
| 1,00 – 1,04 | 197 / −1,55% | 121 / +0,85% |
| 1,04 – 1,08 | 117 / 0,00% | 87 / +3,24% |
| 1,08 – 1,15 | 135 / +5,27% | 77 / +3,89% |
| **1,15 – 1,30** | **47 / −11,14%** | **39 / −5,39%** |
| **acima de 1,30** | **15 / −16,41%** | **3 / −23,11%** |

Aplicando o corte em 1,15:

| cenário | N | WR | ROI | PnL | IC95 bootstrap-dia |
|---|---:|---:|---:|---:|---|
| regra atual | 843 | 85,77% | +0,14% | +1,18 u | [−2,69%, +2,82%] |
| &nbsp;&nbsp;H1 (treino) | 514 | 85,02% | −0,62% | −3,17 u | |
| &nbsp;&nbsp;H2 (fora da amostra) | 329 | 86,93% | +1,32% | +4,35 u | |
| **+ spread ≤ 1,15** | 734 | 86,78% | **+1,49%** | +10,90 u | [−1,19%, +4,09%] |
| &nbsp;&nbsp;H1 (treino) | 449 | 86,19% | +0,90% | +4,06 u | |
| &nbsp;&nbsp;**H2 (fora da amostra)** | 285 | 87,72% | **+2,40%** | +6,84 u | |

Corta 13% do volume (843 → 734).

Cruzando com o teto de 7,00 que já está no robô desde 08/10:

| | spread ≤ 1,15 | spread > 1,15 |
|---|---|---|
| odd de lay ≤ 7,00 | **n=503, ROI +2,54%** | n=49, ROI −13,17% |
| odd de lay > 7,00 | n=231, ROI −0,82% | n=60, ROI −5,45% |

## 3. Dois confundidores já testados

**É só apelido para "liga obscura"?** As ligas com mais spread largo são UKRAINE 1 (44%), SERBIA 1
(41%), WALES 1 (39%). Mas restringindo só às ligas com ≥25 jogos na amostra, o filtro ainda separa:
spread ≤1,15 dá −0,20% (n=165) contra −11,56% (n=26) acima de 1,15.

**É o teto de odd disfarçado?** Correlação entre spread e odd de lay: **0,125**. A tabela cruzada acima
mostra o efeito dentro de cada faixa de odd.

## 4. O que precisa ser atacado

1. **O corte em 1,15 foi escolhido olhando a tabela.** É exatamente o pecado que matou o filtro de
   Over 2.5. Varram o limiar (1,05 / 1,10 / 1,12 / 1,15 / 1,20 / 1,25) e digam se existe patamar ou se
   o 1,15 é um pico isolado. Se o resultado só aparece em 1,15, é overfit de limiar.

2. **O spread da base é o mesmo que se vê no OrbitX no KO−10?** A base tem um snapshot; a execução é
   outro momento. Se o spread medido na base não prevê o spread na hora de clicar, o filtro não é
   aplicável mesmo que seja real. Isso é testável contra a base do coletor da VPS, que captura de 5 em
   5 minutos.

3. **Reproduzam na base de vocês.** O número que vocês reportaram para o Lay Draw 2026 base ampla foi
   n=2.013, WR 85,15%, BE 81,65%, ROI +4,31%. Na base com odd de lay real, dentro da governança e com a
   janela 4,5–10,0, dá n=843, WR 85,77%, BE 85,65%, ROI +0,14%. **Essa diferença precisa ser explicada
   antes de qualquer conclusão** — um BE de 81,65% implica odd média de 5,32, e a odd de lay real dessa
   cohort é 6,85. Qual arquivo e qual coluna geram os 2.013?

4. **O efeito sobrevive em 2024-2025?** Foi deliberadamente ignorado por causa da mudança de regime
   (spread interno da Betfair: 1,0735 em 2024, 1,0725 em 2025, 1,0441 em 2026). Mas se o mecanismo for
   real — livro largo é preço ruim — ele deveria aparecer lá também, possivelmente mais forte. Se não
   aparecer, isso é evidência contra.

5. **Teste de permutação.** Embaralhem o rótulo "spread largo" dentro de cada dia e vejam com que
   frequência o acaso produz uma diferença de 13 pontos percentuais de ROI entre os grupos.

## 5. Regras

- Reportem ROI por **liability** (`(1−c)/(odd−1)` no green, −1 no red), não por stake. PnL nominal com
  stake 1u não é comparável entre grupos de tamanhos e odds diferentes.
- Usem **odd de lay real**. Múltiplo sobre odd de casa já criou três falsos edges neste projeto.
- Se a conclusão for "não há efeito, é ruído", digam isso. O filtro anterior durou uma rodada e isso foi
  o processo funcionando, não um problema.
