# TASKS — Roadmap vivo do ARKAD

> Roadmap compartilhado (Claude + Antigravity). Marque `[x]` ao concluir; **não apague** — mova
> pra "Concluído" ou "Arquivado". Autoridade das regras = [GEMINI.md](GEMINI.md). Diário = [worklog.md](worklog.md).
> Regra-mãe do projeto: **stake-zero até um método passar o portão (CLV+ / bootstrap+FDR excluindo zero / N).**

---

## 🔴 ATIVO — em validação forward (o que importa agora)

- [x] **Laboratório de Observação Setembro/Outubro (Página 03 do Streamlit) — stake-zero (21/09).**
  - [x] Criada página dedicada (`pages/03_🔬_Observacao_Set_Out_2026.py`) para monitoramento prospectivo dos 6 métodos em quarentena/resgate durante Setembro e Outubro/2026 (incluindo Lay 0x1 Sniper no feed da API — Lei 7).
  - [x] Arquivada a página anterior `pages/03_⚡_Radar_Trader_InPlay.py` para `pages_arquivadas/`.
  - [x] Radar do dia integrado à API Betfair Cloud (`get_daily_dataframe("betfair")`) e diário de bordo persistente em `scratch/observacao_set_out_ledger.csv`.
  - [ ] Acumular forward até $N \ge 400$ apostas em Set/Out para deliberar sobre saída da quarentena ou reprovação definitiva.
- [ ] **Suíte de Métodos Trader In-Play (4 Métodos) — stake-zero, pré-registrada (15/09).**
  - [x] Pré-registro congelado em `PREREGISTRO_SUITE_TRADER_INPLAY.md` (LTD Trader, Swing Trade Fav Desvantagem, Scalping Janela Morta, Late Goal Trader).
  - [x] Motor analítico implementado em `trader_inplay_engine.py` com cálculo exato de Cashout Betfair (5% comissão), Stop Loss por tempo e protocolo anti-fabricação (`AGUARDANDO_ODD`).
  - [x] Painel arquivado em `pages_arquivadas/03_⚡_Radar_Trader_InPlay.py` mantendo motor e logs ativos.
  - [ ] Acumular forward até $N \ge 200$ sinais executáveis ao vivo com odds reais Betfair Exchange.
  - [ ] Critério de aprovação: Piso do IC95% > 0.0% pós-FDR com $\ge 10$ reds reais.
- [ ] **Lay 0x0 XGBoost — confirmar o edge (PRIORIDADE 1).**
  - [x] Integrado oficialmente nas páginas centrais do Streamlit: `pages/01` (Radar e Auditoria 2026) e `pages/02` (Resultados e Curva de Equity unificada com 97 jogos forward).
  - [ ] Acumular forward até **N ≥ 300** (hoje 46) via `ARKAD_Forward_0x0` (semanal).
  - [ ] Medir **CLV** dos picks reais (`clv_0x0_log.csv` / `ARKAD_CLV_0x0`). **Aprova se CLV+ mediano E %CLV+ > 50%.**
  - [ ] Calibrar fuso do `Time` do feed no CLV (conferir 1º fechamento real).
  - [ ] Regra congelada (NÃO re-ajustar): liga_0x0<0,08 & mkt_prob<0,10 & 10≤lay≤20 & ev>0,02.
- [ ] **Tríade base (Draw/Home/Over 4.5) — observar, não confirmado.**
  - [ ] 5 fins de semana limpos + bootstrap. Sinal atual: marginal; **CLV só 34%+ (não bate fechamento).**
  - [ ] Lay Home é o menos ruim (~+7% N=74); Draw esfriando; Over 4.5 N minúsculo.
- [x] **Idea1 Back Under (min 10) — FECHADO 15/09:** 1.254 liquidados (57 pendentes resolvidos por base externa com placar exato;
        63 sem placar em liga sem base): **ROI −3,00%** (Under 2.5 −4,2% N=614 · Under 3.5 −1,85% N=640). Log liquidado em
        `varredura_over/idea1_log_liquidado_2026-09-15.csv`; original na VPS com backup `.bak_20260915`.
- [ ] **Under-limite est2/3 — observação/quase-morto.**
  - [ ] Deixar bater **N≥400** (hoje 371, ROI −3,40%, IC95 [−12,8; +6,1]) e aplicar o veredito
        CONGELADO. **NÃO re-recortar antes disso** (cortes vindos do próprio cohort = garimpo).
  - [x] Instrumentação (08/09): log agora grava `placar_entrada, gols_diff, placar_ok, lay, spread,
        lay_size, competicao`. Gatilho intocado.
  - [ ] **Coletor In-Play Minuto 78-83 (Under Limite):** `coletor_inplay_min80.py` implantado para capturar xG acumulado, chutes no alvo e toques na área aos 80' em `inplay_min80_log.csv` e medir empiricamente se a pressão aos 80' prediz gol tardio.
  - [ ] Considerar cortar est3 e Under 3.5 (os piores) dos loggers — **só depois do veredito**.
- [ ] **Late Goal v2 — stake-zero, `PREREGISTRO_late_goal_v2.md` (15/09).** v1 (08/09) encerrada por inviabilidade:
        547 capturas em 8 dias, 0 na faixa 3,00-5,50 (2,2% das capturas). Odd real medida: mediana 2,10.
  - [x] Faixa MEDIDA aplicada no capturador da VPS (1,50-2,60, liq>=200, spread<=0,10) em 15/09.
  - [ ] Julgar só KO >= 16/09; snapshot 12/10. Primeiro olhar (declarado): 265 · −9,7% · IC [−23,4; +0,1]; Over 3.5 já com teto<0.
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
- [x] PRD_SISTEMA_ARKAD.md consolidado (v2.1.0): especificação técnica do SaaS, 5 métodos nucleares oficiais, zebras em quarentena stake-zero, protocolo "Verification Before Completion".
- [x] Conciliação do Ledger Oficial (`forward_5metodos_ledger.csv`): 828 liquidados (636 nos 5 métodos oficiais, +9,79u de lucro; +10,53u total). Fonte única da verdade.
- [x] Integridade do Forward Preservada: Reversão da exclusão de ligas no coletor (`relatorio_forward_5metodos.py`) e scanners para proteger a regra base congelada de 270+ jogos.

## ☠️ ARQUIVADO — testado e MORTO (não re-testar; ver Hall of Shame)

- [x] **Blacklist Ad-Hoc de Ligas (Holanda 1 / Suécia 1)** (Auditada por Claude: teste de permutação 5.000 iterações entre 65 ligas dá mediana +4,16u e P(>=obs) = 0,49. Em qualquer amostra, as 2 piores concentram reds por variância pura. No histórico de 3.326 jogos, 22 de 39 operaram abaixo do BE e ambas foram positivas em 2024. Arquivado como Garden of Forking Paths / Data Dredging).
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
- [x] **xG 12 jogos como filtro de veto no Lay 0x0 (`sum_xg12 >= 2.80`)** (auditado por Claude: pico isolado de 2 cortes numa curva negativa de 2.20 a 3.50; inverte no split temporal da odd real [+0,92% treino vs −0,17% OOS]; p=0,669 controlado pelo preço; IC95 engole zero. ARQUIVADO como filtro ativo; mantido apenas K=12 como escolha física de engenharia de features futuras).
- [x] **Back 0x2 Super Fav Visitante (`Odd_A <= 1.40`, Back 6–20)** (Auditado por Claude e Antigravity: os +194% de Jan–Abr/26 eram odds fantasmas de livros de Lay vazios [ex: Back 12 / Lay 130]; no feed real da API deu −38,6% de ROI em N=49; espelho Lay 0x2 também perde −6,3% comprovando overround. REPROVADO / ARQUIVADO).
- [x] **Lay 3x3 Geral (`Odd_Lay 15–50`)** (Auditado por Claude e Antigravity: positivo unicamente em Jan–Abr/26 devido ao registro anômalo de CS na API [29% dos jogos com lay <= 50 vs 2-5% normal]; fora de Jan–Abr é sistematicamente negativo em todas as janelas [2024 −0,45%, 2025 −0,51%, mai-jul −0,13%, ago-set −1,12%, feed −1,79%]; liability 33-45u inaceitável. DESCARTE / ARQUIVADO).
- [x] **Varredura Ampla dos 43 Mercados em 2026** (Auditada por Claude: todos os 5 mercados amplos positivos em 2026 [Lay 0x3, Lay 2x0, Lay 3x3, Lay 1x3, Back 1x1] decorrem unicamente do regime de gravação anômalo da API em Jan–Abr/26; fora dessa janela todos são estritamente negativos e confirmam ausência de alpha pré-jogo ingênuo. ARQUIVADO).

## 🧭 NORTE (a filosofia que sobrou de tudo)

> **Só existe edge de 2 formas:** (a) **modelo com informação que a odd não tem** (stats/xG → o 0x0),
> ou (b) **viés comportamental in-play lento** (ainda não confirmado). **Constante de odd, dutching,
> pattern sem info, e cobertura% NÃO são edge.** Juiz = **CLV** (bate a linha de fechamento?) + forward
> + bootstrap/FDR. **Alavancagem amplifica edge, não cria** — ciclo só sobre edge confirmado, conservador.
