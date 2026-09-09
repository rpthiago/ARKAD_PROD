# RELATÓRIO — 08/09/2026 · API FotMob/Opta, dois métodos e treze hipóteses

> Cole tudo abaixo da linha no Gemini.

---

Segue o que foi feito e medido em 08/09/2026 no ARKAD. São resultados de teste, o método de cada
um, e os erros de processo que cometi. **Não estou pedindo concordância — quero a sua leitura.**

Tudo é reproduzível: scripts e dados estão no repo (`hist_xg_ht.py`, `hist_time_xg.py`,
`rodar_lote_h1_h6.py`, `rodar_h10_painel.py`, `hist_ht_stats.csv`, `hist_time_stats.csv`,
`cobertura_ligas.csv`, `base_stats_flag.csv`).

---

## 1. A infraestrutura nova

Assinado o plano Pro (US$ 9,99/mês, 20.000 requisições) da `free-api-live-football-data`
(RapidAPI), que é um wrapper do FotMob/Opta.

**Superfície mapeada por sondagem, endpoint a endpoint:**

| Existe | Não existe |
|---|---|
| `get-matches-by-date?date=YYYYMMDD` (167 partidas do dia em **1 requisição**, cauda longa inclusa) | shotmap |
| `current-live` · `matches/teams/players-search` | momentum |
| `get-match-detail` · `get-match-all-stats` · `get-match-firstHalf-stats` | lineups / escalações |
| `get-league-detail` · `get-standing-all` · `get-player-detail` | timeline / events |
| | `secondHalf-stats` · árbitro |

**Consequência dura:** sem timeline, **não há estatística em minuto arbitrário**. Só o corte do
1º tempo e o acumulado. Isso torna o under-limite (min 75-85) e o Late Goal (min 82-86)
**impossíveis** de instrumentar por esse caminho — não improváveis, impossíveis.

**Armadilha achada:** `get-all-matches-by-league?leagueid=` devolve a **temporada passada**
(ago/2025→mai/2026) e não aceita parâmetro de season.

**Gradiente de cobertura, medido em 32 ligas** (`cobertura_ligas.csv`), ponderado por fluxo de
sinais do under-limite: **39% com xG · 17% só chutes/escanteios · 22% sem dado nenhum · 22% nome
não resolvido**. Ligas com xG do 1º tempo incluem Colômbia Primera A, English League One,
Brasileirão A **e B** — bem mais fundo do que a documentação dos fornecedores sugere.

---

## 2. Radar HT — o blueprint, testado com dado limpo

O backtest **reproduz exatamente**: cego 50,92% (N=14.865), com pressão 63,17% (N=2.916).

**Achado que muda a leitura:** na base Bet365, estatística não coletada é gravada como **`0`, não
como vazio**. `isna()` não pega nada. Medido na base inteira: **só 63% dos 242.536 jogos têm stats
confiáveis** (xG: 22%), e a cobertura é **0% ou 100% POR LIGA** — ITALY 4, FRANCE 4, SPAIN 4,
VENEZUELA 2, UKRAINE 2 = 0%; ITALY 1, FRANCE 1, SCOTLAND 1 = 100%.

Trajetória do gap do filtro de pressão conforme a medição fica mais honesta:

| medição | gap |
|---|---|
| base suja, contra a base inteira | **+12,24 pp** |
| base suja, só jogos com stats | +3,27 pp |
| **dado limpo do FotMob, 180 dias, N=470** | **−2,33 pp · IC95 [−11,5; +6,8] · p=0,618** |

Com pressão 54,15% (N=277) vs sem pressão 56,48% (N=193). O sinal está invertido e não é
significativo.

**Outros dois números do blueprint, medidos:**
- Odd de HT suposta "1,80-2,05". Medida no coletor, jogos 0-0: mediana **1,23** (fav≤1,45, N=7),
  **2,19** (fav 1,45-1,60, N=16), **2,32** (fav 1,60-2,00, N=86).
- Fluxo do gatilho exato em 21 dias de coletor: 3.534 jogos vistos no intervalo → 636 em 0-0 →
  **7** com fav≤1,45 → **1** na banda de odd → **0** com liquidez ≥R$300.

O serviço `radar-ht` foi desligado e desabilitado.

---

## 3. Hipótese 2 (Over in-play com xG acumulado) — testada

Como o jogo está 0-0 no intervalo, o total FT é o número de gols do 2º tempo. N=273 com xG.

```
gols do 2º tempo por faixa de xG criado no 1º tempo
  xG 0,0-0,4  N=33  1,52     xG 0,8-1,2  N=81  1,53
  xG 0,4-0,8  N=80  1,70     xG 1,2-1,8  N=61  1,64

correlação xG_1T × gols_2T:  r = −0,026  (p=0,669)
regressão com o preço:       xG  p=0,951
```

---

## 4. Lote pré-registrado H1-H9 — features de xG

Pull: **6.974 partidas**, 25 ligas que concentram 60% dos sinais OOS do Lay 0x0, 300 dias.
93% casaram, 90% com stats, **80% com xG**. 6.555 requisições. Rolling de 8 jogos com `shift(1)`,
split temporal (1ª metade treina, 2ª valida), 1 p primário por hipótese, Benjamini-Hochberg M=9.

```
H6 goleiro segurando          p=0,0461   limiar BH 0,0056   reprovada
H9 Lay Away                   p=0,0798   limiar 0,0111      reprovada
H5 território estéril         p=0,3378                      reprovada
H2 desacordo × mercado fino   p=0,4635                      reprovada
H3 xG aberto ≠ bola parada    p=0,5913                      reprovada
H7 Lay Home                   p=0,7116                      reprovada
H1 xG melhora P(0-0)          p=0,7376                      reprovada
H4 superação de xG regride    p=1,0000 (direção contrária)  reprovada
H8 Lay Draw                   p=1,0000 (ganho negativo)     reprovada
```

Diagnóstico declarado antes de rodar — as features de xG sobrevivem **controlando pelo preço**?
Lay Home: menor p entre 4 features = 0,0131. Lay Draw: 0,0521. Lay Away: 0,2805.

---

## 5. H10 — o painel INTEIRO contra o preço

Motivo do desenho: 37 campos × 4 alvos = ~150 comparações, o que a 5% produz ~8 falsos positivos.
O teste conjunto responde "existe QUALQUER informação aproveitável aqui?" com 1 p-valor por alvo.

**152 features rolling** (26 campos por time, a favor e contra, dos dois lados: xG decomposto,
xGOT, chutes dentro/fora da área, bloqueios, trave, grandes chances perdidas, cruzamentos,
dribles, duelos, aéreos, desarmes, interceptações, cortes, faltas, cartões, passes por terço,
posse). Modelo A = só o preço; Modelo B = preço + painel, ridge com λ por CV só no treino.

```
H10a 0-0        log-loss só preço 0,26613  preço+painel 0,27197  ganho −0,00584  IC [−0,011; −0,001]
H10b Lay Home   0,63002 → 0,64996                                ganho −0,01993  IC [−0,029; −0,011]
H10c Lay Draw   0,58341 → 0,59274                                ganho −0,00933  IC [−0,016; −0,003]
H10d Lay Away   0,56934 → 0,57946                                ganho −0,01012  IC [−0,018; −0,002]
```

O painel **piora** a previsão em todos os quatro alvos, com IC excluindo zero. HistGradientBoosting,
pré-declarado como diagnóstico para checar não-linearidade, é ainda pior: −0,088 / −0,049 / −0,054 /
−0,055.

---

## 6. Outros dois resultados do dia

**Teste do espelho** (N=371, liquidação oficial Betfair): Back Under **−4,64%** e Back Over
**−4,63%** no mesmo cohort. Overround medido 2,36%.

**Outra fonte não preenche buraco de liga pequena:** dos **292 jogos sem stats na base, o FotMob
recuperou 1 (0%)**. A fronteira de cobertura é a mesma nas duas fontes.

---

## 7. Erros de processo que cometi — relevantes para julgar o resto

1. **Rodei o lote antes do pull terminar e reportei "H1 PASSA".** A H1 colapsou com N:
   `+0,01295 p=0,0016` (N=1.926) → `+0,00157 p=0,4294` (N=4.644) → `+0,00055 p=0,7376` (N=4.941).
   Decaimento monotônico rumo a zero. Havia ressalva escrita, mas o número já tinha ancorado.
2. **Duas vezes um p pequeno apareceu com o sinal contrário ao mecanismo** (H4; e o H10 chegou a
   imprimir "PASSA" para Lay Home e Lay Draw com ganho **negativo**). Corrigido com guarda de
   direção: significância na direção errada entra como p=1,00.
3. Gastei 55 requisições numa arquitetura de mapeamento que não funcionava porque o endpoint de
   liga devolve a temporada passada.

---

## 8. Custos

~7.700 requisições de 20.000 no mês. Serviços na VPS hoje: `betfair-collector`, `alerta-under`,
`late-goal`, `xg-ht`. O `radar-ht` foi desligado.

O `xg_ht_logger` continua acumulando: ele é o único teste que mede contra o **preço in-play do
intervalo**, e não contra a odd pré-jogo. Ainda sem N.

---

## 9. O que eu quero de você

1. **Os testes estão certos?** Aponte erro de desenho, de estimador ou de interpretação — o código
   está no repo e a base é a mesma.
2. **O item 5 encerra a busca por feature nesse painel?** Se o conjunto completo piora contra o
   preço, ainda faz sentido procurar coluna individual? Se você acha que sim, qual mecanismo
   justifica olhar uma coluna específica que o conjunto já não capturou?
3. **A H2 (mercado fino) foi testada com proxy fraco.** O campo `matched` do coletor vem sempre
   zero, então usei `back_size + lay_size` (profundidade no topo do livro) como espessura. Ordena
   de forma plausível (Premier 3.074 > La Liga 1.308 > Serie A 1.145 > Bundesliga 951), mas não é
   volume negociado. Vale re-testar com medida melhor, e qual?
4. **Renovação da API (decisão em ~30 dias):** o que, na sua leitura, justificaria renovar? O que
   justificaria encerrar?
5. **Diante de tudo:** o Lay 0x0 congelado segue como único candidato a edge do portfólio, e o dado
   novo não o melhorou. Você vê alguma direção que ainda não foi testada — com mecanismo declarado,
   não com mais garimpo no mesmo painel?

Sem hedge motivacional; nenhuma afirmação de resultado sem intervalo de confiança. Se faltar dado
para responder algo, diga qual coluna ou campo precisa passar a ser coletado.
