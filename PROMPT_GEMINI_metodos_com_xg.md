# PROMPT PARA O GEMINI — construir métodos usando a nova camada de estatística (xG e afins)

Você tem agora, pela primeira vez no ARKAD, uma camada de **estatística de jogo** (xG, xGOT, chutes, posse,
escanteios, grandes chances, defesas) ligada à mesma chave usada no resto do sistema. A tarefa é sua:
**formular e testar hipóteses de método com esses dados**. Nenhuma conclusão está sendo passada de propósito —
as leituras são suas.

---

## 1. O que existe agora (dados)

### 1.1 `ARKAD_PROD/estatisticas_jogos.csv` — a tabela nova
- **32.070 jogos de 2026** (todos os jogos da base b365 com placar), chave `Data` + `Home` + `Away`.
- Colunas: `Data, League, Home, Away, gh, ga, fonte` + pares `_h`/`_a` de:
  `poss, shots, sot, corners, xg, xgot` e, quando vindos do backfill, também
  `bigch, touch_box, saves, xg_open`.
- `fonte`: `b365` (30.127 jogos, estatística que já existia na base) ou `b365+api` (1.943 jogos preenchidos
  pelo backfill via RapidAPI/FotMob).
- **Cobertura de xG: 44,9%** dos jogos de 2026 (era 41,3% antes do backfill).
- Precedência na montagem: o que a base b365 já tinha **nunca** foi sobrescrito; o backfill só entrou onde
  faltava (posse=0 e chutes=0, ou xG ausente).

### 1.2 Estatística por tempo, na base b365 (`DASHBOARD_ARKAD-1/Bases_de_Dados_API_FutPythonTrader_Bet365.csv`)
248.219 jogos (1998→24/09/2026), 219 colunas. Além do FT, tem **HT e 2º tempo separados**:
`xG_H_HT, xG_A_HT, xGOT_H_HT, Possession_H_HT, Total_Shots_H_HT, Shots_On_Target_H_HT, Corners_H_HT,
Goalkeeper_Saves_H_HT` e os equivalentes `_2T` e `_FT`. Em 2026: 26,9% dos jogos vêm sem nenhuma estatística
(posse=0 e chutes=0) e 31,8% têm chutes mas não têm xG.

### 1.3 Ligas em que a estatística NÃO existe (verificado jogo a jogo)
O evento existe na API, com placar, mas o endpoint de estatística devolve vazio. Catálogo aprendido em
`xg_ft_ligas.csv` (139 ligas com contagem de tentativas/sucessos). Entre elas:
ARGENTINA 2 e 3, BOSNIA 1 e 2, BRAZIL 3 e 4, BULGARIA 2, CHILE 2, CHINA 2, COLOMBIA 2, CROATIA 2, CYPRUS 2,
CZECH 2, ECUADOR 2, ENGLAND 5, ENGLAND CUP, ESTONIA 1, FRANCE 3, IRELAND 2, ITALY 4, JAPAN 2,
NORTHERN IRELAND 1, POLAND 2, PORTUGAL 2, ROMANIA 2, SCOTLAND 3 e 4, SERBIA 2, SPAIN 3 e 4.
Qualquer método que dependa de xG não terá dado nessas divisões — inclusive em algumas onde o portfólio
atual dá sinal.

### 1.4 xG ao vivo no intervalo — `xg_ht_log.csv` (VPS, ~1.057 linhas)
Capturado pelo serviço `xg-ht`: por jogo, no intervalo, traz `xg_h, xg_a, sot_h, sot_a, shots_h, shots_a,
corners_h, corners_a, bigch_h, bigch_a, touches_box_h, touches_box_a, poss_h` **junto com as odds daquele
momento** (`odd, odd_draw, odd_away, odd_over25, odd_under25, liq_home`) e o `market_id`/`selection_id`.
É a única fonte que liga estatística de HT a preço de HT.

### 1.5 Odds
- **Pré-jogo (base da API)**: `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` (até 20/08/2026) e
  `metodos_aprovados/.cache_base_betfair.csv` (até 24/09/2026), mais os feeds diários arquivados em
  `ARKAD_PROD/scratch/feed_arquivo/*.parquet` (19/08 → hoje). Todas as odds de lay e back: Match Odds, Dupla
  Chance, Over/Under 0.5–4.5 FT e HT, BTTS, Correct Score 0x0 a 3x3 e goleadas.
- **Intradiário (coletor da VPS)**: `betfair_live_odds.csv`, 3,1 GB, varredura de 5 em 5 minutos desde
  16/08/2026, ~200 jogos/dia, mercados MATCH_ODDS, CORRECT_SCORE, OVER_UNDER 0.5–4.5, FIRST_HALF_GOALS_05,
  BOTH_TEAMS_TO_SCORE, com `back/lay`, tamanhos e `min_to_ko`. Backups locais em `ARKAD_PROD/backup_coletor/`.
- **Placar oficial**: `placares_ft.csv` na VPS, liquidação pelo status WINNER/LOSER da própria Betfair.

### 1.6 Histórico de operação
`metodos_aprovados/forward_5metodos_ledger.csv`: todos os sinais das 06:00 desde 01/08/2026, com método,
liga, odd de lay, odd do favorito, placar, resultado e P&L por unidade de liability. É o registro do que
foi realmente sinalizado.

### 1.7 Cota de API (para novo backfill, se você precisar)
`free-api-live-football-data` no RapidAPI, chave em `alerta.env` na VPS. **8.832 chamadas restantes de
20.000**, reseta em ~9 dias. Endpoints úteis: `football-get-matches-by-date?date=YYYYMMDD` (todos os jogos
de um dia, com ids e placar) e `football-get-match-all-stats?eventid=` (estatística completa do jogo).
Ferramentas prontas: `xg_backfill_alvos.py` (monta a lista de alvos), `xg_backfill.py` (roda na VPS, com
controle de cota e catálogo de ligas), `xg_backfill_juntar.py` (junta na base). O serviço `xg-ht` ao vivo
usa a mesma chave — mantenha a reserva.

---

## 2. Contexto operacional que muda o objetivo

Desde ~21/09/2026 **não é possível apostar no Brasil** (medida provisória). A situação pode mudar a
qualquer momento. Portanto:
- O produto desta tarefa **não é** um método para ligar amanhã. É **hipótese pré-registrada + medição**.
- Não há pressa de execução; há oportunidade de fazer estudo que antes não cabia no tempo.
- Todos os serviços de captura seguem no ar; o dado continua entrando.

---

## 3. Regras da casa (não negociáveis)

1. **Convenção de lay**: liability 1u. GREEN = `(1−c)/(odd−1)`, RED = `−1`, break-even = `(odd−1)/(odd−c)`.
   **Comissão c = 5%** (a que o Thiago paga). Se apresentar outro valor, diga explicitamente qual.
2. **ROI sempre sobre liability**, nunca sobre stake nominal.
3. **Uma base por método**: método pré-jogo se valida na base da API (b365 + Betfair + feeds diários).
   O coletor da VPS é a base dos métodos **in-play**. Não misturar as duas num mesmo número.
4. **Pré-registro antes de olhar o resultado**: regra congelada, N-alvo, critério de aprovação e de reprovação
   escritos antes de rodar. Ajustar filtro depois de ver o resultado invalida o teste.
5. **Bootstrap bloco-dia** (mínimo 6 dias) para o IC95 do ROI; **BH-FDR sobre toda a grade pesquisada**, não
   só sobre a célula escolhida.
6. **Break-even WR** como referência, nunca "acerto vs 50%".
7. **Estabilidade** exigida no tempo (por semestre/ano) e nos thresholds vizinhos.
8. **Mecanismo plausível**: por que o mercado erraria ali. E o mecanismo tem que ser testável com os dados
   acima, não só narrado.
9. Método que passa em quase tudo vai para **watchlist stake-zero**, não para produção.

---

## 4. A tarefa

**Formule hipóteses de método que só se tornaram possíveis agora que existe estatística de jogo, e teste-as.**

Você decide quais. Alguns eixos que os dados permitem explorar (a escolha e o recorte são seus):
- estatística como **variável de resultado** (medir o processo do jogo, não só o placar);
- estatística **do passado dos times** como filtro de entrada pré-jogo;
- estatística **do intervalo** ligada à odd do intervalo (`xg_ht_log.csv`), para métodos in-play;
- comparar o que o **mercado precifica** com o que o jogo **produziu**, para localizar onde o preço erra;
- usar estatística para **explicar as perdas** dos métodos já em forward e derivar filtro dali.

Restrições práticas: xG existe em 44,9% dos jogos de 2026 e **não existe** nas divisões listadas em 1.3 —
declare a cobertura de cada teste. Onde precisar de mais jogos preenchidos, use a cota com as ferramentas
de 1.7 e diga quanto gastou.

---

## 5. O que entregar

1. **Documento de pré-registro** (`PREREGISTRO_*.md`) por hipótese: regra congelada em uma frase, janela de
   dados, N-alvo, critério de aprovação, critério de reprovação, mecanismo e por que ele seria verdadeiro.
2. **Script reprodutível** no repositório, que qualquer um possa rodar e obter os mesmos números.
3. **Tabela de resultados** com, no mínimo: N, acertos/erros, WR real, break-even médio, margem em pp,
   P&L em liability 1u, ROI, IC95 por bootstrap bloco-dia, p-valor, e a mesma linha quebrada por ano/semestre
   e para os thresholds vizinhos.
4. **Cobertura declarada**: quantos jogos do universo tinham a estatística exigida e quantos ficaram de fora.
5. **Um veredito por hipótese**: aprovado / watchlist stake-zero / reprovado, com o critério que você
   pré-registrou.

Depois da entrega, este material passa por auditoria adversarial independente (Claude), que vai tentar
reproduzir cada número na base e derrubar cada conclusão. Escreva pensando nisso: números que não se
reproduzem ou grade pesquisada sem FDR vão aparecer.
