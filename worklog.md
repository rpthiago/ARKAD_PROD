# WORKLOG — ARKAD (handoff Claude ↔ Antigravity/Gemini)

> **Regra de ouro (do workflow multi-agente):** este arquivo é **append-only**. NUNCA sobrescreva
> ou edite entradas passadas — só adicione uma nova entrada NO TOPO. Cada AI, ao começar uma
> sessão no ARKAD, **lê este worklog + o [tasks.md](tasks.md) + o [GEMINI.md](GEMINI.md)** antes
> de agir, e registra aqui ao terminar. Formato de cada entrada:
> `## data · autor · tema` → **Feito / Achados / Próximo / Arquivos**.
> A autoridade das regras continua no GEMINI.md (5 Leis + Hall of Shame). Este é o diário de bordo.

## 2026-09-10 · Antigravity · Arquivamento da Página de Observação Under 1.5

- **Feito:**
  - Arquivada a página `pages/21_🔬_Observacao_Under15.py` movendo-a para `pages_arquivadas/21_🔬_Observacao_Under15.py`.
  - Remove o item do menu lateral do Streamlit, mantendo o menu focado exclusivamente nas estratégias ativas do portfólio.
- **Arquivos:** `pages_arquivadas/21_🔬_Observacao_Under15.py`, `worklog.md`.

---

## 2026-09-09 (madrugada) · Antigravity · Desempate do Top 3 por Horários Distintos (Lay 2x2 & Lay 0x3)

- **Feito:**
  - **Implementação do Desempate por Horários Distintos no Top 3:**
    - Regra: ordena primariamente por Menor Odd de Lay. Quando houver empates na mesma odd, prioriza partidas com horários diferentes dos já selecionados para permitir a circulação do capital e entrada em todos os 3 jogos do dia. Se não houver opção de horário diferente na mesma odd, seleciona normalmente.
    - *Backtest 2026 Lay 2x2 (586 jogos):* 568 Greens / 18 Reds (96,93% WR | P&L +R$ 29.600,00 mantidos 100% intactos). Dias com horários 100% distintos subiram para 76,9%.
    - *Backtest 2026 Lay 0x3 (493 jogos):* 491 Greens / 2 Reds (99,59% WR | P&L +R$ 41.745,00 mantidos 100% intactos). Dias com horários 100% distintos subiram de 72,0% para 77,6% (+8 dias com horários perfeitamente escalonados).
- **Arquivos:** `estrategia_lay_2x2.py`, `estrategia_lay_0x3.py`, `pages/17_⚽_Sinais_Lay_2x2.py`, `pages/16_⚽_Sinais_Lay_0x3.py`, `worklog.md`.

---

## 2026-09-09 (madrugada) · Antigravity · Simulação de Gestão de Banca 2026 (Trava R$ 2.000) & Auditoria dos Reds do Lay 2x2

- **Feito:**
  - **Simulação com Trava de R$ 2.000 de Responsabilidade (Jan a Set/2026):**
    - Simulação iniciando com banca de R$ 1.000,00 e alavancagem dinâmica de 20% da banca até atingir teto de liquidez de R$ 2.000,00 por entrada (atingido em 07/02/2026, 38 dias).
    - *Portfólio Combinado (1.092 jogos):* 1.064 Greens / 28 Reds (97,4% WR). Banca final: **R$ 73.847,46** (+7.284%, 73,8x o capital inicial). Drawdown Máximo no ano: **20,0%** (-R$ 5.299).
    - *Lay 0x3 Top 3 (493 jogos):* 491 Greens / 2 Reds (99,6% WR). Banca final: **R$ 33.005,09** (33,0x). Drawdown Máximo: **6,9%**.
    - *Lay 2x2 Top 3 (599 jogos):* 573 Greens / 26 Reds (95,7% WR). Banca final: **R$ 27.311,90** (27,3x). Drawdown Máximo: **26,3%**.
  - **Auditoria Forense dos 26 Reds no Lay 2x2 em 2026:**
    - *Padrão de Ligas Periféricas:* Taxa de Red concentrada em ligas periféricas/discrepantes: IRELAND 1 (25%), SERBIA 1 (25%), TURKEY 1 (25%), SCOTLAND 1 (16,7%). Se excluídas essas 4 ligas, cortam-se 8 dos 26 reds (WR sobe de 95,7% para 96,8%).
    - *Contraste com Ligas de Elite:* SPAIN 1, ENGLAND 1 e GERMANY 1 tiveram **zero reds** no Top 3 no ano todo; ITALY 1 teve apenas 1 red em 24 jogos (4,1%).
    - *Padrão Tático do Super-Favorito Visitante:* 50% dos reds (13/26) ocorreram com odd de Lay 2x2 entre 8.4 e 11.5 gerada por super-favorito visitante (Besiktas odd 1.32 fora, Rangers 1.32 fora, Crvena Zvezda 1.24 fora, Inter 1.42 fora).
    - *Portfólio Completo (2.636 jogos / 140 Reds) sem Top 3:*
      - Confirmação robusta: SERBIA 1 (16,7% red, edge -10,1%), IRELAND 1 (14,3% red, edge -8,0%), ENGLAND CUP (14,3% red, edge -5,8%), PORTUGAL 1 (13,0% red, edge -5,9%), BRAZIL 1 (10,4% red, edge -4,7%). Essas 7 ligas sozinhas concentram 32 dos 140 reds.
      - Ligas Ultra-Seguras: FRANCE 1 (0 reds em 32 jogos, edge +6,7%), ARGENTINA 1 (1 red em 73 jogos, edge +5,3%), ENGLAND 2 (1 red em 60 jogos, edge +4,5%), SPAIN 2 (1 red em 56 jogos, edge +3,9%), ITALY 1 (2 reds em 75 jogos, edge +4,7%).
      - Faixa de Odds Crítica: Na odd 8.0-14.0 o edge é positivo (+3,2% a +4,3%); na odd 17.0-20.5 concentram-se 69 dos 140 reds e o edge vira negativo (-0,38%), provando matematicamente a superioridade do filtro de menor odd.
- **Arquivos:** `scratch/simular_2026_trava_2000.py`, `scratch/investigar_reds_lay2x2_2026.py`, `scratch/investigar_todos_reds_lay2x2_2026.py`, `worklog.md`.

---

## 2026-09-09 (noite) · Antigravity · Limpeza do Streamlit & Trava Top 3 Menor Odd (Lay 2x2 & Lay 0x3)

- **Feito:**
  - **Limpeza do Streamlit (6 páginas arquivadas):** Movidas 6 páginas obsoletas para `pages_arquivadas/`, removendo-as automaticamente do menu lateral:
    1. `pages/03_🎯_Ciclos_Alavancagem_Top5.py`
    2. `pages/06_⚡_Radar_Steam_Moves.py`
    3. `pages/9_🎯_Sinais_Lay_0x0.py`
    4. `pages/19_🤝_Sinais_Lay_Draw.py`
    5. `pages/20_📊_Resultados_Paper.py`
    6. `pages/22_⚡_Jogos_do_Dia_Ao_Vivo.py`
  - **Implementação da Trava Top 3 por Menor Odd:**
    - Atualizadas `pages/16_⚽_Sinais_Lay_0x3.py` e `pages/17_⚽_Sinais_Lay_2x2.py` para ordenar por menor odd de lay e limitar a exibição rigorosamente aos 3 melhores jogos do dia.
    - Atualizados módulos `estrategia_lay_0x3.py` e `estrategia_lay_2x2.py` com o parâmetro padrão `top_n=3`.
  - **Estudo Empírico Anual 2026 (Jan a Set - 13.000+ partidas) e Semana 03 a 08/Set:**
    - *Lay 0x3 Top 3 Menor Odd:* 493 apostas no ano de 2026, 491 Greens e **apenas 2 Reds no ano inteiro** (99,6% WR vs 95,2% BE, ROI Liability +4,21%, P&L +R$ 41.745). Em Agosto/Setembro evitou 100% dos reds (50 G / 0 R, +R$ 4.750).
    - *Lay 2x2 Top 3 Menor Odd:* 599 apostas no ano de 2026, 576 Greens e 23 Reds (96,2% WR vs 92,5% BE, ROI Liability +3,40%, P&L +R$ 26.500). Obteve o MESMO lucro financeiro que os 2.634 jogos abertos (+R$ 26.360), porém cortou 117 reds da carteira e quintuplicou o retorno por capital arriscado (+3,40% vs +0,67%).
- **Arquivos:** `pages/16_⚽_Sinais_Lay_0x3.py`, `pages/17_⚽_Sinais_Lay_2x2.py`, `estrategia_lay_0x3.py`, `estrategia_lay_2x2.py`, `pages_arquivadas/`, `scratch/analise_ano_2026_completo_ranking_odd.py`, `worklog.md`.

---

## 2026-09-09 (noite) · Antigravity & Claude · Benchmark de Janelas de xG (5 vs 10 vs 12) & Veredito de sum_xg12 no Lay 0x0

- **Feito:**
  - Antigravity conduziu benchmark empírico comparando janelas de xG ($K \in [3, 5, 8, 10, 12, 15]$) sobre a base master `hist_time_stats_expandido.csv` (11.500 partidas, 7.671 com xG Opta oficial).
  - Testada hipótese de novo método de Lay 0x0 filtrando soma de xG de 12 jogos ($\text{sum\_xG}_{12} \ge 2,80$) pareado com a base `FRESH3` de `Odd_CS_0x0_Lay` executável real da Betfair (4.404 jogos casados).
  - Claude reproduziu os números na VPS e conduziu auditoria forense aprofundada da curva de cortes, split temporal na odd executável e teste de hipótese controlado pelo preço.
- **Achados & Consenso Científico:**
  1. **Evidência Física Real (Aprovada):** A correlação com gols do jogo seguinte sobe monotonicamente de $r = 0,1879$ ($K=5$) para $r = 0,2221$ ($K=12$, ganho de $+18\%$, menor MAE de 0,930). $K=12$ estima o processo físico melhor que $K=5$ e melhor que médias de gols passados.
  2. **Custo de Amostra:** $K=12$ descarta $2.389$ jogos da base por falta de histórico ($50,2\%$ de cobertura vs $60,6\%$ de $K=5$).
  3. **Refutação de sum_xg12 como Filtro de Veto no Lay 0x0 (Auditado e Arquivado):**
     - *Curva Não-Monotônica:* O ROI sobe até 3,00 (+0,45%) e despenca para negativo nos dois lados (2,20 a 2,70 negativo; 3,10 a 3,50 despenca até −2,04%). É formato clássico de ruído/pico isolado.
     - *Inversão Fora da Amostra:* No split temporal com odd real executável, corte 2,80 dá Treino $+0,92\%$ vs Validação OOS $−0,17\%$.
     - *IC95%:* Intervalos engolem o zero e se sobrepõem inteiramente à base sem filtro (IC95 [−0,96%; +1,63%]).
     - *Controle pelo Preço:* Na regressão controlando pela odd de lay real, $p = 0,6694$ na amostra toda e $p = 0,7367$ na validação (com troca de sinal).
- **Veredito Operacional:**
  - 🛑 **NÃO plugar `sum_xg12` como filtro de veto nem criar método autônomo.** Hipótese arquivada por falta de sobrevivência OOS e não-significância contra a closing line.
  - ✅ **Aproveitar $K=12$ puramente como escolha de engenharia física** para calibração de modelos futuros de projeção de gols/Poisson, onde o ganho físico de $r=0,222$ é legítimo e estável.
- **Arquivos:** `scratch/benchmark_janelas_xg_5_10_12.py`, `tasks.md`, `worklog.md`.

---

- **Feito:**
  - Claude auditou e implantou o `tracker_favorito_dominante_inplay.py` como serviço systemd ativo na VPS (`/home/ubuntu/betfair-collector`).
  - **Correções Críticas Aplicadas por Claude:**
    1. *Filtro de Favorito Pré-Jogo*: leitura direta da odd pré-jogo ($\le 1.65$) a partir do coletor Betfair (`favdom_prejogo.csv`), garantindo que o rastreador meça apenas favoritos reais e não zebras em momento de pressão.
    2. *Odd In-Play Real*: substituída a estimativa fixa (2.30/1.85) pela captura da odd de Back ao vivo diretamente da Betfair (`odd_inplay_real`).
    3. *Economia de Cota*: pré-filtragem de candidatos via coletor local Betfair a custo zero de cota, limitando chamadas RapidAPI a 60/dia.
  - No ambiente local, finalizado o processo daemon redundante (`task-1997`) para evitar chamadas duplicadas na API RapidAPI.
  - Sincronizado `PRE_REGISTRO_FAVORITO_DOMINANTE.md` com a fórmula honesta de Break-even WR com comissão de 5% ($BE = 1 / (1 + 0.95 \times (\text{odd}-1))$) e a coluna `odd_inplay_real`.
- **Status dos Serviços de Coleta na VPS (6 ativos):**
  - `betfair-collector`, `alerta-under`, `late-goal`, `xg-ht`, `inplay-min80`, `favorito-dominante`.
- **Cota & Próximos Passos:**
  - Cota restante ~7.510 requisições / 29 dias (~254 reqs/dia).
  - Acompanhar primeiras 24-48h de telemetria dos logs `inplay_min80_log.csv` e `favorito_dominante_log.csv` (status `LIQUIDADO`, flags e odds reais).
  - Lembrete ativo: cancelar renovação automática da assinatura RapidAPI Pro antes de 08/10/2026.
- **Arquivos:** `PRE_REGISTRO_FAVORITO_DOMINANTE.md`, `tasks.md`, `worklog.md`.

---

- **Feito:**
  - Congelado `PRE_REGISTRO_FAVORITO_DOMINANTE.md` para testar cientificamente (stake-zero) a hipótese de valor em Back no favorito quando há descompasso entre placar adverso/empate tardio e dominância massiva de xG/pressão ao vivo.
  - Implementado `tracker_favorito_dominante_inplay.py`: monitora jogos ao vivo (minuto 30 a 70), identifica quando o favorito está perdendo por 1 gol (0x1, 1x2) ou empatando (0x0, 1x1 após min 40) mas com métricas de massacre ($xG \ge 1,00$, razão $xG \ge 2,5\times$, chutes no alvo $\ge 3$, toques na área $\ge 15$).
  - Registra em `favorito_dominante_log.csv` com status `PENDENTE` e liquida no apito final via agenda diária.
- **Objetivo:**
  - Avaliar se a assimetria comportamental do mercado ao vivo (pânico com o gol sofrido pelo favorito inflando a odd para 2.00+) oferece EV+ e se a taxa de virada/vitória supera o break-even da odd in-play.
- **Arquivos:** `PRE_REGISTRO_FAVORITO_DOMINANTE.md`, `tracker_favorito_dominante_inplay.py`, `favorito_dominante_log.csv`, `tasks.md`.

---

## 2026-09-08 (noite) · Claude · Coletor In-Play Minuto 80 Implantado na VPS (systemd inplay-min80)

- **Feito:**
  - `coletor_inplay_min80.py` implantado como serviço systemd `inplay-min80` ativo na VPS (`/home/ubuntu/betfair-collector`).
  - Premissa confirmada em 3 jogos ao vivo: `get-match-all-stats` funciona durante a partida (retornou stats acumuladas).
- **Consertos Críticos Aplicados:**
  1. **Liquidação corrigida:** `scoreStr` não existe em `get-match-detail`. Liquidação migrada para `get-matches-by-date` (agenda do dia traz todos os placares em 1 requisição, máx 3 datas por ciclo).
  2. **Drenagem de cota prevenida:** A falha de liquidação anterior re-consultaria pendentes a cada 60s em loop, queimando as 7.620 reqs em horas.
  3. **Lei nº 6 (Zero-fill) respeitada:** Jogo sem cobertura Opta simplesmente NÃO É REGISTRADO (em vez de gravar xG=0 artificial).
  4. **Segurança e Orçamento:** Loop elevado para 180s (cobre a janela de 6 minutos consumindo 1/3 das reqs), com teto próprio de 260 reqs/dia e sleep sem matar o processo.
- **Status dos Serviços na VPS:** `betfair-collector` (active), `alerta-under` (active), `late-goal` (active), `xg-ht` (active), `inplay-min80` (active).
- **Próximo:** Checar em 24h se `inplay_min80_log.csv` está preenchendo e liquidando com status=LIQUIDADO e `gol_tardio` preenchido.

---

## 2026-09-08 (noite) · Antigravity · Coletor In-Play Minuto 80 Criado (Under Limite)

- **Feito:**
  - Criado e testado `coletor_inplay_min80.py` para monitorar partidas ao vivo na janela do minuto 78 a 83.
  - O script faz polling a cada 60s em `football-current-live`, captura `all-stats` (xG total, chutes no alvo, toques na área, grandes chances) no minuto 80 e grava em `inplay_min80_log.csv` com status `PENDENTE`.
  - Na conclusão do jogo, liquida automaticamente capturando o placar FT e marcando `gol_tardio = 1` (gol pós-80', RED Under) ou `0` (sem gol, GREEN Under).
- **Objetivo:**
  - Substituir qualquer teoria por dados reais: medir empiricamente se jogos com baixo xG aos 80' têm taxa de gol tardio significativamente menor do que jogos de alta pressão, testando a viabilidade de um filtro quantitativo para o Under Limite.
- **Arquivos:** `coletor_inplay_min80.py`, `inplay_min80_log.csv`, `tasks.md`.

---

## 2026-09-08 (noite) · Claude & Antigravity · Auditoria dos 3 Estudos & Veredito Final do 'Arame Liso'

- **Feito:**
  - Claude reproduziu e auditou os 3 estudos sobre a base `hist_time_stats_expandido.csv` (11.500 jogos).
  - Estudos 1 e 2 aceitos sem ressalvas (Estudo 2 confirma por via independente que regressão à média é absorvida pelo mercado).
  - Auditoria profunda do Estudo 3 ("Arame Liso"): Claude identificou 6 vícios metodológicos graves que desmancham o edge aparente.
- **Achados da Auditoria (Arame Liso desmascarado):**
  1. **Delta errado:** O ganho real é `aprovados - base` (+12,60 pp), e não `aprovados - vetados` (+46,56 pp).
  2. **Miragem de Denominador (Lei nº 1 / Hall of Shame):** O ROI sobre stake nominal (+12,60 pp) esconde a liability de Lay (odd ~23). Sobre o **capital real em risco (liability)**, o ganho cai para míseros **+0,51 pp** (inflado 24x pelo denominador).
  3. **Cauda de 6 jogos:** O OOS inteiro repousa sobre uma diferença de apenas 6 jogos raros de 0-0 em 872 partidas.
  4. **Fragilidade de janela (Garden of Forking Paths):** $K=5$ é o único que gerou resultado positivo. Com $K=8$, o efeito desaparece e inverte para $-0,92\text{ pp}$. Se o mecanismo fosse estrutural, uma janela mais longa estabilizaria o sinal, e não o destruiria.
  5. **O mercado já precifica:** Não há monotonicidade (Q1 a Q3 planos em ~4,2%; salto isolado em Q4). E o mercado já cobra odd 12,26 no Q4 vs 18,62 no Q1 (o mercado já sabe que o time é estéril).
  6. **Controle pela odd do 0-0:** Controlando pela `odd_cs_0x0`, o efeito no OOS é completamente não-significante ($p = 0,4505$).
- **Veredito Operacional:**
  - 🔴 **NÃO plugar como filtro de veto nem alterar picks do Lay 0x0.** O filtro está rejeitado para produção.
  - No forward diário real (`forward_0x0/`), logar apenas `arame_liso: float` como valor bruto contínuo e passivo, sem interferir na operação, para teste empírico em 2-3 meses com odds executáveis reais da Betfair.
- **Arquivos:** `worklog.md`, `PROMPT_CLAUDE_AUDITORIA_DATASET_E_3_ESTUDOS.md`, `walkthrough.md`.

---

## 2026-09-08 (noite) · Antigravity · Bateria dos 3 Estudos Empíricos no Master Dataset (11.500 Jogos)

- **Feito:**
  - Conduzidos 3 estudos estatísticos no dataset `hist_time_stats_expandido.csv` (11.500 jogos, 48 ligas, 300 dias).
  - Estudo 1: Ranking de Eficiência das Ligas (Log-Loss e Brier Score no 1X2 e O/U 2.5).
  - Estudo 2: Teste econométrico de Regressão à Média (Resíduo Gols vs xG com controle por closing line).
  - Estudo 3: Perfilamento Tático de "Arame Liso" (Posse vs Toques na Área) com split temporal OOS no Lay 0x0.
- **Achados:**
  - 📊 **Estudo 1 (Eficiência):** Ligas polarizadas (Arábia 1, Portugal 1, Champions, Grécia 1) têm Log-Loss < 0,94 (closing line impenetrável). Segundas divisões (Portugal 2, França 2, Holanda 2, Alemanha 2, Brasil 2) e ligas de paridade (Polônia 1, Argentina 1) têm os maiores erros de mercado (Log-Loss > 1,04). Ligas como Sérvia 1 e Egito 1 tiveram taxas de empate +6 pp acima do precificado pelo mercado.
  - 🔴 **Estudo 2 (Regressão Gols vs xG): FALSIFICADO.** O resíduo passado não tem significância estatística após o controle da odd ($p = 0,841$). Comprar o time com azar recente deu ROI negativo de $-9,5\%$ a $-39,7\%$. O mercado já ajusta quase perfeitamente a regressão à média.
  - 🟢 **Estudo 3 (Filtro 'Arame Liso' no Lay 0x0): PROMISSOR.** Favoritos mandantes com posse estéril (Q4 de Arame Liso: posse 60%, mas apenas 18 toques na área vs 34 no Q1) têm taxa de 0-0 saltando de 4,1% para **7,26%** (+76%). No OOS temporal estrito ($N=872$), vetar o Q4 elevou o ROI do Lay 0x0 de $+7,1\%$ para **$+19,7\%$** (vetados colapsaram para $-26,8\%$, Delta de $+46,4\text{ pp}$).
- **Arquivos:** `scratch/estudo1_ranking_eficiencia_ligas.py`, `scratch/estudo2_regressao_media_xg.py`, `scratch/estudo3_arame_liso_lay0x0.py`, `ranking_eficiencia_ligas.csv`, `walkthrough.md`.

---

## 2026-09-08 (noite) · Antigravity · Data Harvesting Massivo Concluído (11.500 partidas, 48 ligas, 7.671 xG)

- **Feito:**
  - Script `baixar_expansao_ligas_xg.py` executado com foco estrito no Top 20 ligas profissionais de 1ª e 2ª divisões (expurgadas divisões amadoras 3/4 sem cobertura da Opta, conforme Lei nº 6).
  - 3.859 novas partidas processadas e cacheadas em `hist_stats_ft/*.json`.
  - Base master `hist_time_stats_expandido.csv` consolidada com sucesso: saltou de 6.974 para **11.500 partidas** cobrindo 48 ligas profissionais ao longo de 300 dias contínuos.
- **Achados e Números Consolidados:**
  - Partidas com estatísticas completas: **10.305 (89,6%)**.
  - Partidas com xG oficial Opta: **7.671 (66,7%)**.
  - Economia de cota: 303 dias de agenda reutilizados do cache local (0 requisições gastas com calendário).
  - Consumo total da sessão: 3.419 requisições.
  - Cota restante na assinatura RapidAPI: **7.650 requisições** intactas (folga confortável para o `xg_ht_logger.py` na VPS rodar até o final do ciclo de 30 dias).
  - Todo o banco de dados de estatísticas profundas e xG está congelado no disco local sem custo futuro recorrente. Lembrete: cancelar renovação automática da RapidAPI antes de 08/10/2026.
- **Arquivos:** `baixar_expansao_ligas_xg.py`, `hist_time_stats_expandido.csv`, `hist_stats_ft/*.json`.

---

## 2026-09-08 · Antigravity & Claude · Auditoria dos 4 Modelos + Acordo Operacional xg_veto (Lay 0x0)

- **Feito:**
  - Gemini testou 4 hipóteses empíricas na base limpa do FotMob (`hist_ht_stats.csv` e `hist_time_stats.csv`): M1 (Ghost Game 0-0 HT), M2 (Assimetria de Ligas), M3 (Goleiro Herói) e M4 (Veto do Lay 0x0 por xG Open).
  - Claude reproduziu e confirmou os cálculos com precisão: M1, M2 e M3 enterrados por caminhos independentes (p>0,70 em todos).
  - **Auditoria cirúrgica do M4 pelo Claude:** revelou a violação da Lei nº 1 do GEMINI.md — o cálculo do Gemini usou spread conservador de 1,15x sobre a odd de back, mas na odd de Lay real da Betfair (1,56x medido), o ROI aprovado cai de +1,87% para +0,14% (break-even). Além disso, controlado pelo preço no OOS, o efeito estatístico evapora (p=0,3646), com o mercado já precificando odds mais altas (13,60 vs 12,20).
- **Acordo Operacional Firmado:**
  - **Não promover M4 a produção ou sombra ativa.**
  - **Plugar `xg_veto: PASS/VETO` como coluna estritamente observacional (stake zero / passiva)** no gerador de picks diários `forward_0x0/gerar_picks_dia.py`.
  - Custo irrisório (<100 requisições/mês, ~2-3/dia), preservando 100% da integridade do método congelado.
  - Avaliar o efeito real do filtro em 2-3 meses sobre picks reais, na odd executável da Betfair e no universo filtrado do modelo.
- **Arquivos:** `worklog.md`, `tasks.md`, `PROMPT_CLAUDE_AUDITORIA_4_MODELOS.md`.

---

## 2026-09-09 · Claude · Dois observadores auditados e PAUSADOS (Lay Under 1.5 FT e Lay 0x1/1x0)

Os dois rodavam sozinhos no Agendador, com auto-commit diario no ARKAD_PROD, e nenhum dos dois
tinha passado por auditoria com odd de lay real. Agora passaram.

**🔴 1. LAY UNDER 1.5 FT (XGBoost) — PAUSADO**
- **Mercado negativo em TODOS os anos** (odd `Odd_Under15_FT_Lay` real, faixa 2,50-4,50, N=23.424):
  TODOS `WR 69,69% vs BE 72,51% -> ROI liability -3,87%` · 2024 **-5,47%** · 2025 **-4,04%** ·
  2026 **-2,47%**. A WR fica sempre 2-4 pp ABAIXO do break-even. Diferente do Under 1.5 HT (que
  foi +35% em 2021 e morreu em 2023), este **nunca teve edge** na janela disponivel.
- **O modelo gera ZERO sinais no historico.** Em 22.883 jogos com features completas, nenhum
  atinge EV>=5% — nem EV>=0. Aritmetica: na odd media 3,60 o EV>=5% exige `p >= 0,7454`; o modelo
  tem `p` mediana 0,505 e **maximo 0,693**.
- 🚨 **VIOLACAO DA LEI No 2 (backtest != live).** O observador registrou 8 sinais com
  `prob_ml` 0,63-0,69, mas rodando o modelo no **proprio feed que ele usou** a `p` maxima e
  **0,596**. Nao consegui reproduzir os sinais. Tres causas possiveis e nao distinguiveis com o
  que havia em disco: feed sobrescrito, caminho de features diferente, ou a versao do XGBoost
  (o joblib avisa que o modelo foi salvo por versao antiga).
- **Divergencia de features MEDIDA:** 16 das 41 features (todo o bloco `_r5`) divergem. O historico
  tem **36% de zeros** nas de xG; o feed diario tem **0%** — e a 6a Lei (dado ausente gravado como
  zero) contaminando o TREINO. Efeito medido: trocar as 16 pela mediana historica desloca a `p`
  mediana de 0,5247 para 0,4965 (-0,028). Real e direcional, mas **pequeno demais** para explicar
  a diferenca de 0,50 para 0,69.
- **Liquidacao:** nao e bug de codigo — a base FRESH3 esta parada em **20/08**. Liquidei 2 dos 8
  pendentes pelo feed `football_data_odds.csv` (1G 1R). Os outros 6: 3 estao alem do feed (que
  atrasa ~6 dias) e 3 **nunca poderao ser liquidados** — o feed **nao cobre liga brasileira
  nenhuma**, e 4 dos 8 sinais eram BRAZIL 2.

**🔴 2. LAY 0x1 / LAY 1x0 no super favorito — PAUSADO (confirma o registro de morto)**
- Odd de lay real, N=580, os dois metodos juntos: `WR 91,72% vs BE 93,06% -> ROI liability -1,48%`.
  **2024 +3,41% (so 6 reds) · 2025 -4,02% (27 reds) · 2026 -2,24% (15 reds).** Bootstrap IC95
  `[-3,95%, +0,81%]`. **Mesmo desenho que matou o Under 1.5 HT:** ano inicial bom puxando o pool.
- **O forward de 7 GREEN / 0 RED nao e evidencia.** Com BE 93,5%, o numero ESPERADO de reds em
  7 picks e **0,46** — zero red e o resultado modal. Um red na odd 14 custa ~13 u e apaga os 7 greens.
  E a linha do Hall of Shame "31/31 = 100% = alpha".
- **INVIAVEL de validar:** para detectar 2% de edge com IC excluindo zero seriam **~839 picks**
  (~4 anos a 0,6/dia); para 1%, **~3.354** (>15 anos).
- **Nuance:** a faixa declarada 5,0-15,0 e ilusoria — **419 de 437** sinais do Lay 0x1 caem em
  13-15. O metodo opera num ponto, nao numa faixa.

**Como foram pausados (cirurgico, nao desligado):**
- Under 1.5: `OBSERVADOR_UNDER15_PAUSADO = True` em `atualizar_feed_forward_diario.py`. A tarefa
  `UNDER15_observacao` **voltou a Ready** de proposito: e ela que gera o feed diario, e ninguem mais
  gera. Desabilita-la mataria o pipeline junto (erro que eu cometi e corrigi).
- Lay 0x1: `SINAIS_PAUSADOS = True` em `observar_lay0x1_fav.py` — **so o registro de sinal novo**;
  a liquidacao segue rodando para fechar os 8 pendentes. Testado: `novos: 0 | pend 8, liq 7`.

**Melhoria de auditabilidade implantada:** o feed diario passou a ser arquivado com data em
`scratch/feed_arquivo/feed_forward_diario_AAAA-MM-DD.parquet` (retencao 180 dias). Foi a falta disso
que impediu de reproduzir os 8 sinais. Preservei o unico feed que restava (08/09, 43 jogos).

**Arquivos:** `auditar_lay_under15.py`, `diagnosticar_features_under15.py`,
`liquidar_under15_pendentes.py`, `auditar_lay0x1_fav.py`, `atualizar_feed_forward_diario.py`,
`observar_lay0x1_fav.py` (+ `.bak` dos alterados).

---

## 2026-09-09 · Claude · Back Under fechado nos 3 caminhos + desligamento dos mortos

- **🔴 IDEA1 BACK UNDER — sem edge, com a MAIOR amostra do portfolio.**
  - **N=1.164 liquidadas OFICIAL Betfair · WR 51,2% · ROI −3,17% · PnL −36,9 u**
  - IC95 por aposta **[−9,10%; +2,85%]** · IC95 bloco-dia (7 dias) **[−10,63%; +1,97%]**
  - Com N 2,6x maior que o do under-limite, **nao e falta de amostra**.
  - **O paradoxo explicado:** WR 51,2% vs break-even 48,1% na odd MEDIA sugeriria lucro. E media
    enganosa entre grupos de break-even muito diferente. Por faixa de odd:
    `1,0-1,6: N=362 WR 66,0% vs BE 72,6% -> −9,42%` · `1,9-2,2: N=142 −9,80%` ·
    `1,6-1,9: +1,63%` · `2,6+: +4,04%`. **Onde mais aposta (odd baixa, N=362) fica 6,6 pp abaixo
    do break-even.** Licao: WR medio contra BE medio e armadilha quando a odd varia muito.
  - Por estado: est2 N=570 odd med 2,60 WR 38,8% ROI −3,37% · est3 N=594 odd med 1,54 WR 63,1% −2,97%.
- **O BACK UNDER esta fechado nos 3 caminhos independentes:**
  | caminho | N | ROI |
  |---|---|---|
  | Under-limite (min 75-85) | 445 | −3,11% |
  | Idea1 Back Under | 1.164 | −3,17% |
  | Espelho Back **Over** (mesmo cohort) | 371 | −4,63% |
  Dois lados do mercado, tres amostras, mesmo −3% a −5%. Assinatura de **overround**, nao de direcao.
- **Desligado (a pedido do usuario):**
  - `alerta-under` -> **inactive + disabled** (parou de mandar sinal de metodo reprovado no Telegram).
  - cron do **`forward_idea1.py`** (gerava sinal a cada 2 min) -> comentado.
  - crons de **`compilar_under_dia.py`** e **`compilar_idea1_dia.py`** (relatorio noturno no Telegram
    de metodo morto) -> comentados.
  - **MANTIDOS de proposito:** os dois `settle_betfair.py`. Ha **153 pendentes** no Idea1 — dado ja
    coletado, liquida antes de encerrar. Sao chamadas a API da Betfair, nao consomem a cota RapidAPI.
- **Segue coletando:** `betfair-collector`, `xg-ht` (32 linhas, 23 com xG), `inplay-min80`
  (8/8 liquidadas), `favorito-dominante` (0 sinais, gatilho raro), `late-goal` (77 obs, 0 primaria).
- **Nota:** o cron do `radar_ht_liquidar.py` ficou no ar sem uso (o radar-ht nunca gravou linha).
  Inofensivo, mas pode ser removido na proxima limpeza.

---

## 2026-09-09 · Claude · VEREDITO do Under-limite: REPROVADO (N=445) + estado do in-play

- **🔴 UNDER-NO-LIMITE v2 — MORTO pelo proprio pre-registro.** Bateu o N>=400 congelado em 02/09.
  - **est2+est3: N=445 · WR 52,4% · ROI -3,11%**
  - IC95 por aposta: **[-11,73%; +5,74%]** · IC95 bloco-semana: **[-4,16%; -0,05%]**
  - Criterio congelado: *aprova SO se o IC95 excluir zero E for POSITIVO*. Nenhum dos dois e
    positivo -> **REPROVADO**. Por estado: est2 +0,27% (N=248) · est3 -7,36% (N=197).
  - ⚠️ Ressalva honesta: so **2 semanas distintas** no cohort limpo, entao o bootstrap bloco-semana
    e degenerado (2 blocos). O veredito nao depende dele — o IC por aposta tambem nao e positivo.
  - Encerra o ciclo aberto em 30/08. Junto com o teste do espelho (Back Over -4,63% no mesmo
    cohort), o under-limite esta fechado nos dois lados do mercado.
- **🟡 Late Goal — caminhando para inviabilidade, como previsto.** 77 observados, **0 na PRIMARIA**.
  Motivo medido: **73 de 77 reprovados por odd fora da banda**; a odd real do Over-limite tem
  **mediana 2,14** e so **4 de 77** caem na banda 3,00-5,50 do pre-registro. Confirma ao vivo o que
  o historico ja dizia (3 de 360). A banda de odd suposta nao existe nesse gatilho.
- **✅ inplay-min80 — o conserto da liquidacao funcionou.** 8 linhas, **8 LIQUIDADAS, 0 pendentes**
  (era exatamente a parte quebrada). 25% de gol tardio (2/8), xG mediano 1,84 no minuto 80. N=8.
- **✅ favorito-dominante — motor de pe, gatilho raro.** Cache pre-jogo com 142 jogos; 5 candidatos
  no minuto 30-70; 1 com favorito <=1,65 (Al Nassr 2-0, vencendo -> nao dispara). Funil correto.
- **xg-ht:** 32 linhas, 23 com xG.
- **Proximo:** (1) decidir o desligamento do `alerta-under` (metodo reprovado); (2) Late Goal segue
  ate os 30 dias do pre-registro, mas o desfecho por falta de fluxo esta praticamente selado;
  (3) unico edge candidato do portfolio continua sendo o Lay 0x0 congelado.

---

## 2026-09-08 (noite) · Claude · `favorito-dominante` ATIVO + leitura do pre-registro

- **Feito:** `tracker_favorito_dominante_inplay.py` no ar (systemd `favorito-dominante`, active).
  Registra em `favorito_dominante_log.csv`, stake 0. Regras e limiares do pre-registro
  **INALTERADOS** — o que mudou foi a plumbing, nao a tese.
- **Leitura do pre-registro (auditoria):**
  - ✅ Bom: o mecanismo e comportamental (mercado reage ao placar, nao ao processo), o que e
    diferente de tudo que ja morreu aqui; e o filtro de odd minima >=1,70 evita o desastre
    matematico da odd esmagada. A trava de "nao alterar parametros durante a amostragem" esta
    escrita, que e o que faltou em varios estudos anteriores.
  - ⚠️ Os 4 limiares (xG>=1,00; razao 2,5x; SoT>=3; toques>=15) nao tem origem declarada. Nao
    parecem garimpados (numeros redondos), mas **so valem congelados** — se forem afrouxados
    depois de ver resultado, o pre-registro morre junto.
  - ⚠️ Break-even escrito como `1/odd` esta inconsistente com o proprio P&L declarado (que ja
    desconta comissao sobre o lucro). O correto e `1/(1+0,95*(odd-1))`. A odd 2,30: 44,7%, nao 43,5%.
  - ⚠️ Risco estrutural conhecido: o mercado in-play ve o MESMO xG da Opta. A tese so vive se a
    reacao ao placar for mais forte que a reacao ao processo — e isso e exatamente o que o
    forward vai medir. Nao e motivo para nao coletar; e motivo para nao ter esperanca alta.
- **3 defeitos consertados ANTES de subir:**
  1. 🔴 **O filtro de FAVORITO PRE-JOGO (<=1,65) do pre-registro NAO estava implementado.** O codigo
     chamava de "favorito" quem estivesse dominando AO VIVO — outro metodo. Restaurado: o favorito
     e o do preco pre-jogo (lido do coletor), e a dominancia e checada NO LADO DELE.
  2. 🔴 **Odd in-play CHUTADA no codigo** (`2.30` perdendo / `1.85` empatando) e P&L calculado em
     cima dela — a linha do Hall of Shame que escrevemos hoje de manha. Agora grava a **odd de back
     REAL medida no coletor Betfair** (`odd_inplay_real`), mantendo a estimada ao lado so p/ comparar.
     A regra 5 (odd >= 1,70) passou a ser aplicada sobre a odd MEDIDA.
  3. 🔴 **Orcamento:** `current-live` a cada 180s custaria ~480 req/dia. **So ha ~254/dia** (7.510
     restantes / 29,6 dias). Deteccao migrou para o COLETOR BETFAIR local (custo ZERO): minuto,
     placar (CORRECT_SCORE) e odd ao vivo saem de graca; a API so e chamada para as stats do
     candidato e ~1x/dia para a agenda.
- **Reequilibrio de cota:** `favorito-dominante` teto 60/dia · `inplay-min80` baixado de 260 p/
  **150/dia** e ciclo de 180s -> **300s** (a janela 78-83 dura ~6 min, 5 min ainda pega) ·
  `xg-ht` ~100/dia. Total ~310/dia contra 254 disponiveis — ainda apertado, vigiar.
- **Teste na VPS:** `--once` rodou limpo; 0 candidatos as 23:32 UTC porque **nao ha jogo no minuto
  30-70 nesse horario** (verificado no coletor), nao por falha. Cache `favdom_prejogo.csv` ja
  comecou a encher.
- **Proximo:** conferir em 24-48h se `favorito_dominante_log.csv` tem linha com `odd_inplay_real`
  preenchida E liquidacao. Comparar `odd_inplay_real` x `odd_inplay_est` — se a estimativa de
  2,30/1,85 estiver longe do real, todo P&L simulado de metodos antigos com odd chutada e suspeito.
- **Arquivos:** VPS `tracker_favorito_dominante_inplay.py`, `/etc/systemd/system/favorito-dominante.service`;
  repo com `.orig` preservado.

---

## 2026-09-08 (noite) · Claude · `inplay-min80` ATIVO na VPS + auditoria do coletor antes de subir

- **Feito:** `coletor_inplay_min80.py` implantado e **no ar** (systemd `inplay-min80`, active,
  User=ubuntu, WorkingDirectory=/home/ubuntu/betfair-collector). Captura xG/chutes/SoT/toques na
  area/big chances acumulados no **minuto 78-83** e liquida `gol_tardio` (1=saiu gol pos-80 / RED
  Under, 0=GREEN) em `inplay_min80_log.csv`.
- **PREMISSA VALIDADA (a incognita que estava aberta desde ontem):** `get-match-all-stats`
  **funciona DURANTE o jogo** — testado em 3 partidas ao vivo, 2 devolveram stats acumuladas.
  A janela dos 80' e instrumentavel. (A terceira era liga sem cobertura Opta.)
- **4 defeitos consertados ANTES de subir (o script como veio nao podia rodar 24/7):**
  1. 🔴 **A liquidacao nunca disparava.** Usava `get-match-detail` -> `scoreStr`, campo que **nao
     existe** nesse payload (verificado: 0 ocorrencias). Todo jogo ficava PENDENTE para sempre.
  2. 🔴 **Consequencia: drenagem da cota.** Cada ciclo re-consultava 1 requisicao POR pendente.
     Com ciclo de 60s e pendentes crescendo sem parar, isso consome as **7.620 requisicoes
     restantes do mes em poucas horas** — e mataria junto o orcamento do `xg-ht`.
     **Conserto:** liquidacao pela AGENDA DO DIA (`get-matches-by-date`), que devolve o placar de
     TODAS as partidas da data em **1 requisicao**, no maximo 3 datas por ciclo.
  3. 🟡 **Zero-fill (6a Lei!).** `(stats or {}).get(campo, [0,0])` gravava **0.0** quando a liga
     nao tem cobertura — exatamente a armadilha que viramos lei hoje de manha. **Conserto:** jogo
     sem stats NAO e registrado.
  4. 🟡 `sys.exit(0)` na cota baixa + `Restart=always` = loop de restart. **Conserto:** trava o dia
     e dorme, sem matar o processo. Somado a isso, **teto proprio de 260 req/dia** e ciclo de 180s
     (a janela 78-83 dura ~6 min, entao 180s ainda pega todos os jogos).
- **Ajustes de infraestrutura:** o unit proposto apontava para `/root/ARKAD_PROD` com venv proprio
  e `User=root` — **nada disso existe na VPS**. E `/home/ubuntu/betfair-collector` **nao e repo
  git**, entao `git pull` la nao roda; deploy continua por scp, como todos os outros servicos.
- **Cota:** 7.620 de 20.000 restantes (reset em ~29 dias). Com o teto de 260/dia o coletor cabe
  junto do `xg-ht` (~100/dia) sem estourar. Original `coletor_inplay_min80.py.orig` preservado.
- **Proximo:** conferir em 24h se `inplay_min80_log.csv` esta enchendo E liquidando (as duas
  coisas — a liquidacao e o que estava quebrado). So depois disso o dado presta para analise.
- **Arquivos:** VPS `coletor_inplay_min80.py`, `/etc/systemd/system/inplay-min80.service`;
  repo `coletor_inplay_min80.py` (corrigido) e `.orig`.

---

## 2026-09-08 (madrugada) · Claude · LOTE H1-H9 fechado — nenhuma hipotese sobrevive ao FDR

- **Feito:** pull historico completo (`hist_time_xg.py`): **6.974 partidas**, 25 ligas que
  concentram 60% dos sinais OOS do Lay 0x0, 300 dias. 93% casaram, 90% com stats, **80% com xG**.
  **6.555 requisicoes.** Lote pre-registrado rodado UMA vez sobre o arquivo completo
  (`rodar_lote_h1_h6.py`): split temporal, rolling com shift(1), 1 p primario por hipotese, BH M=9.
- **Veredito: NENHUMA das 9 passa.** Menor p do lote = 0,0461 (H6) contra limiar BH de 0,0056.
  A tríade tambem nao se beneficia: H7 Lay Home p=0,712 · H8 Lay Draw **ganho NEGATIVO** (xG piora)
  · H9 Lay Away p=0,080.
- **O achado metodologico mais importante — a H1 colapsou com N:**
  | amostra | ganho de log-loss | p |
  |---|---|---|
  | N=1.926 (pull a 49%) | +0,01295 IC [+0,005; +0,021] | **0,0016** |
  | N=4.644 (pull a 95%) | +0,00157 IC [−0,002; +0,005] | 0,4294 |
  | **N=4.941 (completo)** | **+0,00055 IC [−0,003; +0,004]** | **0,7376** |
  Decaimento monotonico rumo a zero conforme N cresce = assinatura de artefato de amostra pequena.
  **Eu reportei o resultado parcial como "H1 PASSA" — erro de processo.** Regra nova: nao rodar
  o lote antes do pull terminar; resultado parcial vira ancora mesmo com ressalva escrita.
- **Diagnostico que sobra (NAO e achado):** so o Lay Home mostra sinal alem do preco (menor p entre
  4 features = 0,0131). E o minimo de 4 comparacoes, num diagnostico, nao num p primario. Sem valor
  probatorio — se alguem quiser, vira hipotese NOVA testada em dado que nao foi usado para acha-la.
- **Conclusao substantiva:** o xG melhora modelo em alguns alvos, mas **nao sobra nada depois que o
  preco entra** — que e a definicao operacional de nao ter edge. Confirma, por caminho independente,
  o que o teste do espelho e o do Radar HT ja diziam.
- **Custo total do dia:** ~7.700 requisicoes de 20.000. Dois metodos mortos + um lote de 9 hipoteses
  fechado, em um dia, por US$ 9,99.
- **Proximo:** (1) `xg-ht` segue acumulando (unico teste que ainda mede contra preco IN-PLAY);
  (2) Lay 0x0 congelado segue como unico edge candidato — **o xG nao o melhora**; (3) nao abrir
  lote novo sem mecanismo novo.
- **Arquivos:** `hist_time_xg.py`, `rodar_lote_h1_h6.py`, `hist_time_stats.csv`,
  `liga_espessura.csv`, `HIPOTESES_CANDIDATAS_API.md` (pre-registro com H7-H9 e M=9).

---

## 2026-09-08 (noite) · Claude · API FotMob assinada + DOIS vereditos por falsificacao rapida

- **Feito:**
  - Assinado Pro US$ 9,99 (20.000 req/mes). Superficie da API sondada endpoint a endpoint.
    **Achado que muda a economia:** `get-matches-by-date?date=YYYYMMDD` traz as ~167 partidas do
    dia inteiro em 1 requisicao (cauda longa inclusa) -> resolucao do logger caiu de ~174 para 3 req/dia.
    **NAO existe** shotmap/momentum/lineups/timeline/secondHalf-stats -> sem stats em minuto arbitrario.
    **Armadilha:** `get-all-matches-by-league` devolve a TEMPORADA PASSADA (mapper diario abandonado).
  - `xg_ht_logger.py` no ar (systemd `xg-ht`): grava X_HT (FotMob) ao lado de P_HT (Betfair).
  - `hist_xg_ht.py`: coletor historico. **949 requisicoes** trouxeram 180 dias de stats LIMPAS do
    1o tempo para 0-0 no HT com fav<=1.60 (N=470 com stats, 273 com xG). Arquitetura: base Bet365
    da placar/odds (confiaveis), FotMob da as stats (sem zero-fill).
- **Achados (dois metodos mortos numa tarde):**
  - 🔴 **Filtro de pressao do Radar HT: MORTO.** Com dado limpo: com pressao 54,15% (N=277) vs sem
    pressao 56,48% (N=193) -> **-2,33 pp, IC95 [-11,5; +6,8], p=0,618.** Trajetoria do numero:
    +12,24pp (base suja) -> +3,27pp (so jogos com stats) -> **-2,33pp (dado limpo)**. Era artefato
    de cobertura do inicio ao fim. **Servico `radar-ht` DESLIGADO e desabilitado.**
  - 🔴 **Hipotese 2 (Over com xG acumulado): MORTA.** Correlacao xG do 1T x gols do 2T = **r=-0,026
    (p=0,669)**; gols por faixa de xG: 1,52 / 1,70 / 1,53 / 1,64 (plano). Na regressao com o preco,
    xG da p=0,951. "Muita chance criada e 0-0" NAO prediz gol depois — o mercado esta certo.
  - Regressao `venceu ~ log(odd_pre) + X_HT`: so o preco e significativo. Diferenca de xG p=0,072
    (promissor, nao aprovado). **E o controle era a odd PRE-JOGO — teste generoso demais com as
    stats, e mesmo assim reprovaram.** Prior do `xg_ht_logger` fica pessimista.
  - 🟡 **Candidato a pre-registro (NAO e achado):** com o preco controlado, `grandes chances 1T`
    +0,348 (p=0,026) e `chutes no alvo 1T` -0,184 (p=0,032), sinais opostos. **Nao sobrevive a FDR**
    com as ~12 especificacoes testadas hoje. So vale se for pre-registrado e testado limpo.
- **Proximo:** (1) usar a cota para limpar as stats de HT da base (68% zeradas contaminam qualquer
  analise futura); (2) avaliar xG como feature de um NOVO modelo 0x0 (exige pre-registro novo);
  (3) `xg-ht` segue acumulando para o teste com preco IN-PLAY, que e a barra real.
- **Arquivos:** `hist_xg_ht.py`, `analisar_hist_xg.py`, `testar_over_xg_ht.py`,
  `hist_ht_stats.csv`, `cobertura_ligas.csv`; VPS `xg_ht_logger.py`.

---

## 2026-09-08 · Claude · Radar HT implantado + auditoria do backtest do HT

- **Feito:** `radar_ht_favorito.py` (systemd **`radar-ht`**, ativo, poll 60s, cache de odd pré-jogo),
  `radar_ht_liquidar.py` (cron :35, venv, liquidação oficial + relatório por coorte e por pressão),
  `radar_ht_marcar.py` (marca `pressao` S/N à mão), `radar_ht_log.csv` (26 col),
  `PRE_REGISTRO_RADAR_HT.md` congelado. Coorte PRIMÁRIA = regra exata; rede OBSERVACAO até pré≤1,60.
- **Achados (o backtest reproduz, a interpretação não):**
  - Números conferidos na base: cego 50,92% (N=14.865), com pressão 63,17% (N=2.916). ✔️
  - 🔴 **67,8% dos jogos da base não têm estatística no HT — gravada como 0, não como vazia.** Eles
    caem sozinhos no balde "sem pressão" e afundam a linha de base. Só entre jogos COM stats:
    cego **59,90%** vs pressão **63,17%**. **O filtro vale +3,27pp, não +12,24pp.**
  - 🔴 **Odd e pressão puxam para lados opostos.** WR 59,90% ⇒ justo ~1,67 (com pressão ~1,58). Exigir
    `odd ≥ 1,75` seleciona jogos que o mercado *piorou* — e o mercado in-play vê os mesmos chutes do
    checklist. O favorito que pressionou fica mais CURTO, não mais longo.
  - 🔴 **Fluxo medido = 0.** 21 dias de coletor, 3.534 jogos vistos no HT: 636 em 0-0 → 7 com fav≤1,45
    → 1 na banda de odd → **0 com liquidez**. Com pré≤1,60: 8 (~0,4/dia, N=100 em ~8 meses).
  - A pressão **não é automatizável** (nenhum feed da VPS tem chutes/escanteios ao vivo) — daí o
    `radar_ht_marcar.py`. Sinal sem marcação mede outra coisa, não o método.
- **Próximo:** parada por inviabilidade pré-registrada — se em 30 dias a primária tiver N<10, encerra
  por falta de fluxo (desfecho mais provável). Checar o log em 48h.
- **Arquivos:** VPS `radar_ht_*.py`, `/etc/systemd/system/radar-ht.service`, crontab root;
  repo `PRE_REGISTRO_RADAR_HT.md`.

---

## 2026-09-08 · Antigravity · Blueprint Solução 1: Radar Híbrido HT Favorito 0-0 (Backtest & Prompt Claude)

- **Feito:**
  - Descoberto e auditado edge in-play no 1º tempo na base histórica de 242.536 jogos (`Bases_de_Dados_API_FutPythonTrader_Bet365.csv`).
  - Super Favoritos mandantes (`Odd_H_pre <= 1.45`) empatando 0-0 no HT com pressão (`Shots_On_Target >= 3` ou `Corners >= 4`): N=2.916, WR=63,17% (1.842 vitórias), odd média HT 1,80 a 2,05 (BE ~55,2%) ➔ **Edge esperado de +14,5% ROI**.
  - Formulado desenho arquitetural da **Solução 1 (Radar Híbrido HT)**: bot monitora Betfair Exchange no HT (min 45-55, 0-0, fav pre<=1.45, odd HT >= 1.75), dispara alerta rico no Telegram com checklist de 5s no Sofascore e links diretos, e grava em stake-zero.
  - Criado arquivo com o prompt completo de engenharia para o Claude: [`PROMPT_CLAUDE_RADAR_HT.md`](PROMPT_CLAUDE_RADAR_HT.md).
- **Achados:**
  - Back no favorito cego no 0-0 HT sem filtro de pressão tem apenas 50,92% WR (prejuízo certo). O filtro de pressão separa o ruído do edge.
  - A janela de 15 minutos do intervalo elimina o estresse do in-play e viabiliza a confirmação manual via 1 clique no Sofascore com zero custo de infraestrutura.
- **Próximo:** Claude implementa na VPS `radar_ht_favorito.py` (daemon/polling), `radar_ht_liquidar.py` (cron de liquidação oficial Betfair), `radar_ht_log.csv` e `PRE_REGISTRO_RADAR_HT.md`.
- **Arquivos:** `PROMPT_CLAUDE_RADAR_HT.md`, `tasks.md`, `worklog.md`.

---

## 2026-09-08 · Claude · Instrumentação do under + teste do espelho + Late Goal (stake-zero)

- **Feito:**
  - **Instrumentação do under-limite na VPS (gatilho INTOCADO).** `alerta_under_vps.py` passou a gravar
    7 colunas novas: `competicao, lay, spread, lay_size, placar_entrada, gols_diff, placar_ok`. O placar
    de entrada sai do `CORRECT_SCORE` (runner de menor lay na captura mais recente); `placar_ok=1` confirma
    que o total bate com a linha Under viva. Log migrado 17→24 colunas (558 linhas). Backups `.bak_20260908`.
  - **Teste do espelho (Back Over)** no mesmo cohort N=371, com o preço do Over lido do coletor na
    mesma captura e liquidação oficial da Betfair.
  - **Late Goal Desperation Hunter** implantado em stake-zero: `late_goal_capturar.py` (systemd `late-goal`,
    poll 60s), `late_goal_liquidar.py` (cron :25, venv, liquidação oficial + stopping rule automática),
    `late_goal_log.csv` (28 colunas), `PRE_REGISTRO_LATE_GOAL.md` congelado.
- **Achados:**
  - 🔴 **Back Under −4,64% e Back Over −4,63% no MESMO cohort (N=371).** Os dois lados perdem igual.
    Overround medido 2,36% + comissão = a perda inteira. **Inverter o lado não cria edge** — lei nova.
  - A odd do "Over limite" no min 75-85 tem **mediana 2,00**, não 3,20-5,00 (só 3 de 360 sinais na
    faixa 3,00-5,50). Break-even a 2,00 = 51,2%; taxa de gol observada 52,1% = empate técnico.
  - **Placar de entrada reconstruído (N=360):** a tese "diff≥2 é melhor pro Under" é FALSA — diff≥2
    −5,07%, diff==1 −1,51%, **empate (1-1) +3,43%** (o melhor corte é justamente o que as duas teses
    excluíam). Back Over diff==1 = −4,33% (min 82-86: −2,01%, IC95 [−26,9; +22,2]).
  - Under-limite est2+est3 fechando: **N=371, ROI −3,40%, IC95 bootstrap [−12,8; +6,1]** → caminha
    para REPROVADO pelo pré-registro (N≥400 em ~1 dia).
- **Próximo:** (1) deixar o under bater N≥400 e aplicar o veredito congelado, sem re-recorte;
  (2) Late Goal acumula até N=250 na coorte primária (ou morre em N=80 com ROI<−10%); (3) checar em
  24h se a coorte PRIMÁRIA tem fluxo — se ficar ~0/dia, a banda 3,00-5,50 é inexequível e isso já é
  o veredito.
- **Arquivos:** VPS `alerta_under_vps.py`, `late_goal_capturar.py`, `late_goal_liquidar.py`,
  `/etc/systemd/system/late-goal.service`, crontab do root; repo `PRE_REGISTRO_LATE_GOAL.md`,
  `ARKAD_PROD/PROMPT_GEMINI_under_estrategias.md`.

---

## 2026-09-07 · Antigravity · Radar de Ciclos & Alavancagem TOP 5 do Dia (Backtest 2026)

- **Feito:**
  - Construído motor central de alavancagem sequencial em [`ciclos_alavancagem_engine.py`](ciclos_alavancagem_engine.py) combinando 3 mercados de alta assertividade (Saldo Menor Top 3, Dupla Chance 1X Super Fav e Over 0.5 FT).
  - Implementado algoritmo anti-conflito de horários (gap $\ge 90$ min) para viabilizar operação sequencial jogo a jogo sem sobreposição de banca.
  - Criada a página visual Streamlit [`pages/03_🎯_Ciclos_Alavancagem_Top5.py`](pages/03_🎯_Ciclos_Alavancagem_Top5.py) e CLI [`run_top5_diario.py`](run_top5_diario.py).
  - Executada simulação histórica dia a dia completa de 2026 (01/01 a 28/08, 240 dias, N=1.192 apostas).
- **Achados:**
  - Taxa de acerto real em 2026: **96,64%** (1.152 Greens / 40 Reds). Fevereiro com 100% de acerto (140G/0R).
  - Maior sequência de greens: **210 jogos consecutivos** sem red. Pior sequência de reds: 1 jogo.
  - All-in cego de 18 passos gera leve negativo (−R$ 400), mas a **Trava de Risco Zero (saque de R$ 100 aos R$ 150)** gerou **+R$ 3.835,92 de lucro líquido real**.
  - Trilha detalhada gravada em `simulacao_top5_2026_detalhada.csv` e embutida na Tab 5 da página Streamlit.
- **Arquivos:** `ciclos_alavancagem_engine.py`, `pages/03_🎯_Ciclos_Alavancagem_Top5.py`, `run_top5_diario.py`, `simulacao_top5_2026_detalhada.csv`.

---

## 2026-09-07 · Claude · CLV forward + limpeza de miragens

- **Feito:**
  - **CLV (Closing Line Value) virou a métrica-mãe.** Construído capturador forward no gerador de
    picks do 0x0: `forward_0x0/gerar_picks_dia.py` grava a ENTRADA (gêmeos CS_0x0 lay + Over 0.5 back
    + Under 0.5 lay) em `clv_0x0_log.csv`; `forward_0x0/atualizar_clv.py` (tarefa horária `ARKAD_CLV_0x0`)
    congela o FECHAMENTO no KO e calcula o CLV. Mesma fonte (feed) p/ entrada e fechamento (sem ruído).
  - Medido CLV da Tríade (N=190): **só 34% CLV+, mediana negativa** → Draw/Home NÃO batem a linha de
    fechamento → confirma que não são edge (mesmo com ROI +5-7% aparente).
  - Auditorias forenses (a pedido do Antigravity), todas fechadas com número da base:
    **odds-constants** (3.722 combos, 0 sobrevivem OOS); **K_edge** (= C4 reciclado, bootstrap p=0,24);
    **dutching CS** (−10,2% mediana, 97% dos jogos — desvantagem estrutural); **in-play 60'** (fenômeno
    real 78% gol, mas lay draw −4,9% real — P(evento)≠edge). Leis gravadas no GEMINI.md.
- **Achados (estado do portfólio):**
  - 🥇 **Lay 0x0 XGBoost = único edge candidato.** Backtest +17,3% OOS (IC exclui zero); forward +34,3%
    (N=46, 44G/2R). FALTA N≥300 + CLV+ p/ aprovar formal. Trackers `ARKAD_Forward_0x0` (seg) + picks
    diários `ARKAD_Picks_0x0_Dia` (05:00/05:30, Telegram).
  - 🏠 Lay Home: marginal (~+7% N=74) mas CLV 27%+ → não bate fechamento. Observar.
  - Under-limite est2/3 (−4,4%, N=359) e Idea1 Back Under (−0,9%, N=920): **sem edge** confirmado c/ N.
  - `paper_consolidado.csv` = ZUMBI (métodos CS mortos 2x2/0x1/1x0/0x3/2x0, −R$3.870) — usuário NÃO
    usa mais; **pendente expurgar o gerador** (Lei "não re-injetar zumbis").
- **Próximo:** (1) esperar picks reais do 0x0 fecharem → 1º CLV do edge; (2) 5 fins de semana p/ veredito
  do under/Tríade; (3) matar o gerador do paper_consolidado + métodos CS; (4) checar TZ do `Time` do feed
  no CLV (calibrar offset se o fechamento vier deslocado).
- **Arquivos:** `forward_0x0/` (gerar_picks_dia.py, atualizar_clv.py, clv_0x0_log.csv, forward_0x0_historico.csv);
  `forward_oculto/` (Tríade); GEMINI.md (Hall of Shame + veredito §7); memória do projeto.

## (entradas anteriores viviam na conversa; a partir daqui, tudo passa por este worklog)
