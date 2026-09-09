# PROMPT PARA O CLAUDE — Descoberta de API com xG e Stats do 1º Tempo ao Vivo

> **Instruções para o Claude:** Você é o engenheiro responsável pela infraestrutura e auditoria na VPS. Acabamos de testar com sucesso uma nova fonte de dados via RapidAPI (`free-api-live-football-data.p.rapidapi.com`) que resolve exatamente a limitação que você apontou no item 4 da auditoria ("*Nenhum feed da infraestrutura tem chutes no alvo ou escanteios ao vivo*"). Queremos sua análise crítica, quantitativa e arquitetural sobre como (e se) devemos explorar isso no ecossistema ARKAD.

---

## 1. O Teste Realizado & O que a API Entrega

Testamos a API diretamente via Python com a chave fornecida pelo usuário. A resposta veio em HTTP 200 com a estrutura de dados idêntica ao backend do **FotMob** (alimentado por dados oficiais da **Opta**).

### Endpoints Chave Validados:
1. **`GET /football-current-live`**
   - Retorna os jogos acontecendo em tempo real com placar, tempo de jogo (`liveTime: "24'"`), `periodLength: 45`, flags de `ongoing`, `started`, `finished`, e se o 1º tempo já começou/encerrou.
2. **`GET /football-get-match-firstHalf-stats?eventid={id}`**
   - **O Santo Graal:** retorna isoladamente todas as métricas dos **primeiros 45 minutos**!
   - Testado no jogo real Betis x Real Madrid (`eventid: 5868043`):
     - **xG do 1ºT:** Real Madrid 2.35 xG (open play 1.80, set play 0.56, xGOT 1.46) vs Betis 0.10 xG.
     - **Chutes no alvo do 1ºT:** Real Madrid 5 vs Betis 1.
     - **Chutes totais do 1ºT:** Real Madrid 15 vs Betis 5.
     - **Escanteios do 1ºT:** Real Madrid 6 vs Betis 4.
     - **Grandes chances do 1ºT (*big chances*):** Real Madrid 6 criadas (6 perdidas) vs Betis 0.
     - **Toques na área adversária do 1ºT:** Real Madrid 36 vs Betis 12.
     - **Cartões vermelhos e amarelos** do 1ºT.
3. **`GET /football-get-match-all-stats?eventid={id}`** (Stats completas FT).
4. **`GET /football-matches-search?search={Team}`** (Busca de jogos por time para mapeamento).

---

## 2. O Contexto da Nossa Conversa Anterior

Na sua auditoria forense do Radar HT, você provou irrefutavelmente três verdades:
1. A baseline honesta a 0-0 no HT com dados é **59,90% WR**, e com pressão sobe para **63,17% WR** (+3,27 pp vs cego, +8,38 pp vs sem pressão).
2. O preço justo com comissão 4,5% é **~1,58 a 1,67**. Exigir `odd >= 1.75` colide com o mercado: quando o time pressiona, o mercado comprime a odd em 1,45-1,60. A odd só sobe a 1,75+ quando o time NÃO pressiona (Maldição do Vencedor / Seleção Adversa).
3. O coletor in-play teve **zero fluxo** em 21 dias para `pre <= 1.45` + `odd 1.75-2.40` + `liq >= 300`.
4. A VPS não conseguia rodar o método sem intervenção humana manual.

Agora, **o obstáculo técnico de não ter estatísticas in-play foi quebrado**. A máquina pode ler xG e chutes do 1º tempo via JSON em milissegundos durante os 15 minutos de intervalo.

---

## 3. O que Queremos de Você (Sua Opinião & Análise Crítica)

Como guardião da governança e da infraestrutura, responda francamente:

### (A) Casamento de Entidades (Betfair Exchange ↔ RapidAPI)
A Betfair usa `market_id` / `event_id` proprietários; a RapidAPI usa os IDs da Opta/FotMob.
- Qual é a viabilidade de criar um resolvedor de entidades (`team_mapper`) que cruze os jogos da Betfair com a RapidAPI por nomes normalizados (`home_team`, `away_team`) e horário do KO?
- Você considera viável manter esse mapeamento rodando na VPS (por exemplo, populando uma tabela de correlação uma vez por dia às 06:00 UTC)?

### (B) Onde (e se) Existe Alpha com xG In-Play
Se a tese "Back Fav 0-0 no HT com odd >= 1.75" morreu por falta de fluxo e seleção adversária, onde dados reais de xG e finalizações ao vivo podem gerar edge estatístico real contra a Betfair?
- **Hipótese 1: Exploração da Inércia da Zebra (Lay Fav / Lay Under).** O mercado ainda mantém o favorito caro (odd baixa ~1,40-1,50) mesmo com o favorito produzindo zero xG (`xG_HT < 0.20`, 0 chutes a gol) e a zebra bloqueando os espaços?
- **Hipótese 2: Over In-Play baseado em xG Acumulado.** Se um jogo está 0-0 aos 60-70' mas o xG acumulado é $\ge 2.50$ (muitas chances criadas, goleiros fazendo milagres, traves), a odd do Over 0.5 / Over 1.5 infla por decaimento de tempo enquanto o hazard real de gol permanece alto?
- **Hipótese 3: Nenhuma das anteriores.** O mercado da Betfair é rápido demais e os modelos dos formadores de mercado já precificam o xG da Opta antes mesmo da gente conseguir disparar uma ordem?

### (C) Restrições Práticas e Custos
- A chave atual tem limite gratuito de **100 requisições/mês** (restam 96).
- Um plano com volume de requests na RapidAPI custaria ~$10 a $20/mês.
- Como podemos desenhar um teste de validação de baixíssimo consumo (ex.: consultar a API apenas quando um jogo específico na Betfair atingir determinado filtro de odd/placar no HT, gastando no máximo 3-5 requisições por dia)?

### (D) Veredito Operacional
Você recomenda:
1. Congelar e arquivar qualquer exploração in-play de HT por enquanto, mantendo foco 100% no Lay 0x0 XGBoost e no Late Goal;
2. OU criar um script piloto enxuto na VPS (`teste_rapidapi_xg.py`) para registrar em log o xG do 1º tempo dos jogos acompanhados pelo coletor e medir se o xG bate o fechamento da Betfair (CLV)?

Sem rodeios: avalie a viabilidade técnica e se a relação sinal/ruído justifica o trabalho de engenharia.
