# AUDITORIA ADVERSARIAL — EIXO 3: processo do 1º tempo vs gols no 2º tempo (0-0 no HT) — Claude, 2026-09-28

Auditado: `testar_eixo3_processo_ht_vs_2t.py` + `PREREGISTRO_eixo3_processo_ht_vs_2t.md`.
Bases: `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` (2024–2026) e, para o controle de preço in-play,
o coletor da VPS (`backup_coletor/*.gz`, OVER_UNDER_25 e MATCH_ODDS com `min_to_ko` entre −50 e −72,
ou seja, a captura mais próxima dos 45 minutos de bola rolando).

## Veredito

**FALSO POSITIVO COMO OPORTUNIDADE DE MERCADO — mas não é erro de execução nem viés grosseiro.**
O fenômeno estatístico é real e sobreviveu a todos os testes de robustez que apliquei. O que não se sustenta
é a interpretação de ineficiência: **o modelo controla pelo preço PRÉ-JOGO, e o preço pré-jogo não pode
conter informação do 1º tempo.** Trocando pelo preço do intervalo, o efeito desaparece e inverte de sinal.

## 1. Reprodução — bate exatamente

| item | Gemini | Claude |
|---|---|---|
| jogos 2024-2026 | 123.080 | 123.080 ✓ |
| com xG HT real | 45.619 (37,1%) | 45.619 (37,1%) ✓ |
| 0-0 no HT | 13.514 | 13.514 ✓ |
| amostra da regressão | 13.455 | 13.455 ✓ |
| r (xG_1T × gols_2T) | +0,0853 (p=3,1e-23) | +0,0853 ✓ |
| β(xG) modelo 1 | +0,1688 (z=3,14, p=0,0017) | idêntico ✓ |
| β(xG) modelo 2 | +0,1267 (z=2,81, p=0,0049) | idêntico ✓ |
| 2024 / 2025 / 2026 | p=0,2185 / 0,0003 / 0,0267 | idênticos ✓ |

Nada a corrigir na execução.

## 2. Testes adversariais — o efeito SOBREVIVEU a todos

| teste | N | β(xG) | p |
|---|---|---|---|
| como reportado (SE não-robusto) | 13.455 | +0,1688 | 0,0017 |
| **erro-padrão agrupado por liga** | 13.455 | +0,1688 | **0,0047** |
| erro-padrão agrupado por dia | 13.455 | +0,1688 | 0,0017 |
| **sem jogos com expulsão no 1T** (726 jogos, 5,4%) | 12.729 | +0,1718 | **0,0054** |
| **com efeito fixo de liga** (variáveis centradas na liga) | 13.455 | +0,1488 | **0,0107** |
| só ligas com N≥60 (tira a cauda) | 13.146 | +0,1594 | 0,0079 |

- **Cartão vermelho não explica.** Excluir as 726 partidas com expulsão no 1º tempo não muda nada (β sobe
  de 0,169 para 0,172). Nota técnica: a base **não tem** `Red_Cards_*_HT`; derivei como `FT − 2T`.
- **Composição de liga não explica.** Com efeito fixo de liga o coeficiente cai ~12% e segue significante.
  O filtro "ter xG" seleciona ligas de elite, mas o efeito existe **dentro** da liga.
- **Agrupamento de erros não explica.** Mesmo com SE agrupado por liga, p=0,0047.

Conclusão desta parte: o 1º tempo carrega informação sobre o 2º tempo. Isso é um fato sobre futebol — e é
o oposto da independência de Poisson. O Gemini está certo neste ponto.

## 3. O problema fatal: o controle é o preço errado

O modelo pergunta *"o xG do 1º tempo informa além da odd de Over 2.5 de ANTES do jogo?"*. A resposta só
poderia ser sim: o preço pré-jogo foi formado quando o 1º tempo ainda não existia. Isso não mede
ineficiência — mede que o jogo aconteceu. O pré-registro do próprio Gemini define o mecanismo como *"se o
mercado in-play de intervalo precificar o 0-0 apenas pelo tempo decorrido"*, mas **nenhum preço de intervalo
entra no teste**.

Refiz o modelo com a odd de Over 2.5 **capturada ao vivo pelo coletor perto dos 45 minutos**:

| modelo (mesma amostra, 0-0 no HT, desde 16/08/2026) | N | β(xG) | p |
|---|---|---|---|
| só odd PRÉ-JOGO | 239 | −0,187 | 0,55 |
| **com a odd DO INTERVALO (in-play)** | 239 | **−0,250** | **0,44** |
| com as duas odds | 239 | −0,195 | 0,55 |

E o dado mais direto: **a correlação entre o xG do 1º tempo e a odd in-play de Over 2.5 no intervalo é
−0,368**. O mercado *já* baixa o preço do Over quando o 1º tempo foi perigoso. Não há nada por precificar.

Ressalva honesta: N=239 tem pouco poder — esse teste **não prova ausência de efeito**. Mas ele remove a
base da afirmação de ineficiência, e a correlação de −0,37 é evidência direta de que a informação está no
preço. O ônus da prova volta para quem propõe o método.

## 4. Sub-hipótese 3-B — significativa, e ainda assim não tradável

Meus números (na amostra com odd de Over 2.5 válida): alta pressão 65,5% (188/287), baixa 57,0% (523/917),
spread **+8,5 pp** (o Gemini reportou 65,3% / 56,0% / +9,3 pp — diferença de amostra, mesma conclusão).

- **Fisher exato p=0,01099**, qui-quadrado p=0,01320, odds ratio 1,43.
- Controlando pela odd do favorito: β=+0,3617, **p=0,0103** (as odds médias são praticamente iguais: 1,29 na
  alta pressão vs 1,31 na baixa — não é confounding de favoritismo).
- **Mas os cortes 1,00 e 0,60 descartam 28% da amostra** (458 de 1.712 jogos ficam no meio, sem uso). Os
  pontos de corte não foram pré-registrados com FDR sobre as alternativas.

**O teste que decide — dar back no favorito ao preço do intervalo** (coletor, comissão 5%):

| recorte | N | vitórias | odd média no HT | implícita | ROI |
|---|---|---|---|---|---|
| alta pressão (xG_H ≥ 1,00) | **4** | — | — | — | N insuficiente |
| baixa pressão (xG_H < 0,60) | 18 | 27,8% | 2,71 | 43,1% | **−43,8%** |
| todos | 31 | 41,9% | 2,35 | 49,2% | **−27,5%** |

E de novo: **correlação de −0,399** entre o xG do mandante no 1º tempo e a odd in-play dele no intervalo.
O mercado corta o preço do favorito exatamente quando ele pressionou. A amostra de alta pressão com preço
in-play é de **4 jogos** — não há como afirmar nada sobre tradabilidade hoje.

## 5. Veredito e diretriz

**REPROVADO como método. APROVADO como achado científico, com a interpretação corrigida.**

1. O efeito de processo é **real e robusto** (sobrevive a expulsão, liga e agrupamento de erros). Isso merece
   registro no GEMINI.md: em 0-0 no intervalo, o 1º tempo informa o 2º — a independência de Poisson é falsa.
2. A conclusão de **ineficiência não se sustenta**: o benchmark usado é o preço pré-jogo. Com o preço do
   intervalo, o coeficiente vira negativo e a correlação preço×xG é −0,37 (Over) e −0,40 (favorito).
3. **Watchlist stake-zero no coletor**: concordo com o encaminhamento, mas com a regra reescrita. O que deve
   ser medido não é "xG alto → mais gols" (já sabemos que sim), e sim **"a odd in-play no intervalo desconta
   DEMAIS ou DE MENOS o xG do 1º tempo"**. A forma correta: pré-registrar limiares, gravar a odd do
   intervalo e o xG do intervalo em stake zero, e medir P&L contra aquele preço. Com 4 jogos de alta pressão
   em seis semanas, o N-alvo de 200 leva ~2 anos no ritmo atual — declare isso no pré-registro.
4. **Parametrização do coletor**: hoje a varredura é de 5 em 5 minutos e só 239 dos 469 jogos 0-0 no HT
   tiveram captura perto dos 45'. Para este estudo, vale capturar a janela 42'–50' com passo fino nos jogos
   que chegam 0-0 — senão metade da amostra se perde.
5. Ponto de rigor para o próximo estudo: `Odd_Over25_FT` da base b365 **não é closing line** da Betfair; é a
   odd do bookmaker na base. Chamar de closing line superestima o rigor do controle.
