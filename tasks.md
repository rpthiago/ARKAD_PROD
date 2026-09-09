# TASKS — Roadmap vivo do ARKAD

> Roadmap compartilhado (Claude + Antigravity). Marque `[x]` ao concluir; **não apague** — mova
> pra "Concluído" ou "Arquivado". Autoridade das regras = [GEMINI.md](GEMINI.md). Diário = [worklog.md](worklog.md).
> Regra-mãe do projeto: **stake-zero até um método passar o portão (CLV+ / bootstrap+FDR excluindo zero / N).**

---

## 🔴 ATIVO — em validação forward (o que importa agora)

- [ ] **Lay 0x0 XGBoost — confirmar o edge (PRIORIDADE 1).**
  - [ ] Acumular forward até **N ≥ 300** (hoje 46) via `ARKAD_Forward_0x0` (semanal).
  - [ ] Medir **CLV** dos picks reais (`clv_0x0_log.csv` / `ARKAD_CLV_0x0`). **Aprova se CLV+ mediano E %CLV+ > 50%.**
  - [ ] Calibrar fuso do `Time` do feed no CLV (conferir 1º fechamento real).
  - [ ] Regra congelada (NÃO re-ajustar): liga_0x0<0,08 & mkt_prob<0,10 & 10≤lay≤20 & ev>0,02.
- [ ] **Tríade base (Draw/Home/Over 4.5) — observar, não confirmado.**
  - [ ] 5 fins de semana limpos + bootstrap. Sinal atual: marginal; **CLV só 34%+ (não bate fechamento).**
  - [ ] Lay Home é o menos ruim (~+7% N=74); Draw esfriando; Over 4.5 N minúsculo.
- [ ] **Under-limite est2/3 + Idea1 Back Under — observação/quase-morto.**
  - [ ] Deixar bater **N≥400** (hoje 371, ROI −3,40%, IC95 [−12,8; +6,1]) e aplicar o veredito
        CONGELADO. **NÃO re-recortar antes disso** (cortes vindos do próprio cohort = garimpo).
  - [x] Instrumentação (08/09): log agora grava `placar_entrada, gols_diff, placar_ok, lay, spread,
        lay_size, competicao`. Gatilho intocado.
  - [ ] **Coletor In-Play Minuto 78-83 (Under Limite):** `coletor_inplay_min80.py` implantado para capturar xG acumulado, chutes no alvo e toques na área aos 80' em `inplay_min80_log.csv` e medir empiricamente se a pressão aos 80' prediz gol tardio.
  - [ ] Considerar cortar est3 e Under 3.5 (os piores) dos loggers — **só depois do veredito**.
- [ ] **Late Goal Desperation Hunter — stake-zero, pré-registro aberto 08/09.**
  - [ ] Coorte PRIMÁRIA até **N=250** (morte antecipada: N≥80 com ROI<−10%, automática no liquidador).
  - [ ] **Checar em 24h se a primária tem fluxo.** Banda odd 3,00-5,50 apareceu em só 3 de 360 sinais
- [ ] **Back Favorito Dominante In-Play (Stake Zero / Observação) — pré-registro aberto 08/09.**
  - [x] `PRE_REGISTRO_FAVORITO_DOMINANTE.md` congelado e calibrado com BE honesto e odd in-play Betfair real.
  - [x] Rastreador `tracker_favorito_dominante_inplay.py` corrigido por Claude e implantado na VPS como serviço systemd (log: `favorito_dominante_log.csv`).
  - [ ] Acumular forward até $N \ge 200$ sinais. Monitorar integridade dos campos `odd_inplay_real` e liquidação oficial.
  - [ ] Gatilho: min 30-70', perdendo por 1 gol ou empate tardio (min 40-65'), xG_fav >= 1.0 e >= 2.5x zebra, sot >= 3, tbox >= 15. Stake: 0.0.
- [ ] **Radar Híbrido HT (Back Favorito 0-0 com Pressão) — stake-zero, no ar 08/09.**
  - [x] `radar_ht_favorito.py` (systemd `radar-ht`), `radar_ht_liquidar.py` (cron :35),
        `radar_ht_marcar.py`, `radar_ht_log.csv`, `PRE_REGISTRO_RADAR_HT.md`.
  - [ ] **Parada por inviabilidade:** se em 30 dias a PRIMÁRIA tiver N<10 → encerra por falta de
        fluxo. Medido: 21 dias de coletor deram **0 sinais** na regra exata (636 jogos 0-0 no HT →
        7 com fav≤1,45 → 1 na banda de odd → 0 com liquidez).
  - [ ] **Marcar `pressao` S/N em TODO sinal** (`radar_ht_marcar.py --pendentes`). Sem isso o cohort
        não testa o método — só "back no fav 0-0 no HT".
  - [ ] ⚠️ O gap do filtro é **+3,27pp** (cego 59,90% vs pressão 63,17% entre jogos COM stats), não
        +12,24pp: **67,8% da base tem stats gravadas como 0** e caem no balde "sem pressão".
        E `odd≥1,75` está ACIMA do justo (~1,67) — odd e pressão se excluem mutuamente. Ver pré-registro §0.

- [ ] **API de stats ao vivo (FotMob/RapidAPI) — DECIDIDO 08/09: assinar Pro US$ 9,99/mês.**
  - [x] Varredura de cobertura (32 ligas, cota grátis): **39% do fluxo com xG**, 17% com chutes/
        escanteios, 22% sem dado, 22% nome não resolvido. Ver `cobertura_ligas.csv`.
  - [x] `fotmob_map.py` + `fotmob_ligas.csv` na VPS; `--dry-run` validado (0 requisição).
  - [x] **Pro assinado 08/09** (20.000 req/mês confirmado no header). Chave em `alerta.env`.
  - [x] **`xg_ht_logger.py` NO AR** (systemd `xg-ht`): grava X_HT (xG/SoT/chutes/escanteios/big
        chances/toques na área) ao lado de P_HT (1X2 + O/U 2.5 da Betfair no intervalo).
        Colunas de liquidação iguais às do under → `settle_betfair.py xg_ht_log.csv` roda direto.
  - [x] Cache que **aprende a cobertura**: liga com 3 falhas de stats vira degrau 3 e é pulada
        (zero requisição). Teto duro de 500 req/dia no próprio script.
  - [ ] Acumular ~300-500 linhas (2-4 semanas) → rodar `resultado ~ log(P_HT) + X_HT`.
        **Se o coeficiente de X_HT não for significativo, morre — e morreu por US$ 10.**
  - [ ] ⚠️ `fotmob_map.py` (mapper diário) **abandonado**: o endpoint
        `get-all-matches-by-league` devolve a TEMPORADA PASSADA (ago/2025→mai/2026), sem parâmetro
        de season. Resolução passou a ser por `matches-search` dentro do logger, com cache.
        Arquivo renomeado para `.obsoleto_endpoint_temporada_errada`.
  - [ ] Chave não foi trocada (usuário reenviou a mesma) — trocar é só editar a linha do `alerta.env`.
  - [x] Data Harvesting massivo concluído (08/09): 11.500 partidas consolidadas em `hist_time_stats_expandido.csv` (10.305 com stats, 7.671 com xG Opta, 48 ligas, 300 dias). Cota preservada com 7.650 reqs restantes para a VPS.
  - [ ] Orçamento medido: 73 ligas/dia, 199 jogos/dia, **174 chegam ao intervalo**.
        Mapper ~1.200/mês + logger ~3.000/mês = **~25% dos 20.000**. Folga grande.
- [ ] Contexto da decisão: [DECISAO_API_STATS_LIVE.md](DECISAO_API_STATS_LIVE.md) · [ANALISE_RAPIDAPI_XG_HT.md](ANALISE_RAPIDAPI_XG_HT.md)
  - [ ] **Rodar o teste de cobertura no free tier ANTES de assinar** (1 fim de semana, custo zero):
        ≥70% das ligas da lista com chutes/escanteios preenchidos AO VIVO e defasagem ≤60s.
  - [ ] Candidato nº1: **API-Football** (US$19 + €15 add-on stats ≈ US$36/mês, 1.200+ ligas, 15s).
  - [ ] ⚠️ **xG ao vivo na cauda longa NÃO EXISTE a nenhum preço** (todo provedor limita a 1ª divisão
        europeia). O Advanced da Sportmonks (€199-399) cobriria ~5 das 22 ligas onde os sinais caem.
  - [ ] ⚠️ Cauda longa: as 22 maiores ligas = só **33%** dos sinais; a maior isolada = 2,2%.
        API que cobre "as 50 principais" resolve ~10% do problema.

- [x] **Lote H1-H9 (xG como feature) — FECHADO 08/09: nenhuma sobrevive ao FDR (M=9).**
  - Menor p 0,0461 (H6) contra limiar 0,0056. H1 colapsou de p=0,0016 (N=1.926) para
    **p=0,7376 (N=4.941)** — artefato de amostra parcial.
  - **Tríade não se beneficia:** Lay Home p=0,712 · Lay Draw ganho NEGATIVO · Lay Away p=0,080.
  - xG melhora modelo, **não sobra sinal depois do preço** = sem edge. Ver `HIPOTESES_CANDIDATAS_API.md`.
  - ⚠️ **Regra de processo nova: não rodar lote antes do pull terminar.** Resultado parcial vira
    âncora mesmo com ressalva escrita.

## 🧹 LIMPEZA (pendente)

- [ ] **Expurgar zumbis:** achar e desligar o gerador do `paper_consolidado.csv` + métodos CS
      (2x2/0x1/1x0/0x3/2x0/0x0-RF) de qualquer gerador de sinais. Lei GEMINI.md "não re-injetar zumbis".
- [ ] Confirmar que o `paper_consolidado` é papel (usuário confirmou que não olha mais).

## ✅ CONCLUÍDO (registro, não apagar)

- [x] Feed de resultados OFICIAL da Betfair (WINNER/LOSER por market_id) — zero false-green.
- [x] Consertos VPS: cron settle (venv/pandas), NameError do relatório, limite 4096 do Telegram.
- [x] est0/est1 do under DESCARTADOS (backtest + live oficial confirmam negativo).
- [x] Forward gap 21/08→02/09 reconstruído com placar real (odd lay real).
- [x] CLV framework construído (métrica-mãe) + picks diários do 0x0 no Telegram.
- [x] worklog.md + tasks.md criados (coordenação Claude↔Antigravity).

## ☠️ ARQUIVADO — testado e MORTO (não re-testar; ver Hall of Shame)

- [x] **Constantes de odds** (busca exaustiva 3.722 combos, 0 sobrevivem OOS — beco estrutural).
- [x] **K_edge / Cross-Market Index** (= C4 reciclado; bootstrap p=0,24; garimpo).
- [x] **Dutching de Correct Score** (−10,2% mediana, 97% dos jogos — paga k spreads).
- [x] **In-play 1X2 overshoot pós-gol** (mercado eficiente; conceder −12%, scorer ~0%).
- [x] **In-play "a favor do tempo" / cashout +5%** (−10%; decaimento é precificado).
- [x] **In-play 60' lay draw** (78% gol é P(evento), não edge; −4,9% real).
- [x] **Pressão Cruzada (Lay Draw K≤2,20 / Lay Home falso-fav)** (−2 a −5%, eficiência de mercado).
- [x] **Lay Away, Lay Under 0.5, Lay 0x1 (pré-jogo)** (mortos no scan + histórico).
- [x] **Filtros refinados do scan** (18 holders Lay Home = overfit, flipam no forward).
- [x] **CS suite: Lay 2x2/0x3/2x0/0x2/1x0** (cauda gorda, liability, margem navalha).

## 🧭 NORTE (a filosofia que sobrou de tudo)

> **Só existe edge de 2 formas:** (a) **modelo com informação que a odd não tem** (stats/xG → o 0x0),
> ou (b) **viés comportamental in-play lento** (ainda não confirmado). **Constante de odd, dutching,
> pattern sem info, e cobertura% NÃO são edge.** Juiz = **CLV** (bate a linha de fechamento?) + forward
> + bootstrap/FDR. **Alavancagem amplifica edge, não cria** — ciclo só sobre edge confirmado, conservador.
