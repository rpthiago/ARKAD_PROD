# PROMPT PARA O CLAUDE — Radar Híbrido HT: Back Favorito 0-0 com Pressão

> **Instruções para o Claude:** Você é o engenheiro responsável pela infraestrutura e execução na VPS Oracle Cloud (São Paulo). Leia o [GEMINI.md](GEMINI.md), o [worklog.md](worklog.md) e o [tasks.md](tasks.md). Siga rigorosamente as 5 Leis Inegociáveis, o protocolo stake-zero e o desacoplamento estrito entre gatilho e liquidação oficial da Betfair.

---

## 1. Contexto & Descoberta Quantitativa

O Antigravity (Gemini) auditou a base histórica de 242.536 jogos (`Bases_de_Dados_API_FutPythonTrader_Bet365.csv`) para responder à pergunta: **existe edge real em explorar favoritos que tropeçam no 1º tempo?**

Os resultados empíricos revelaram um fenômeno nítido:

### Cenário: Super Favorito Mandante (`Odd_H_pre <= 1.45`), empatando 0-0 no Intervalo (HT)
1. **Apostar às cegas no Favorito no HT:**
   - Taxa de vitória FT: apenas **50,92%** (N=4.810).
   - Como a odd média do Back no HT gira entre 1,80 e 2,05 (break-even ~55,2%), apostar sem critério dá **ROI negativo (−7% a −10%)**. O mercado pune o favorito apático.

2. **O Filtro de Pressão do 1º Tempo (`Shots_On_Target_H_HT >= 3` OU `Corners_H_HT >= 4`):**
   - Amostra: **N = 2.916 jogos**.
   - O Mandante virou e venceu o jogo em **1.842 partidas (63,17% Win Rate)**!
   - Com odd executável no HT entre 1,80 e 2,05 (BE 55,2%), o edge é de **+7,98 pp acima do break-even** ($\approx \mathbf{+14,5\%}$ **ROI esperado**).

### O Desafio da Automação 100% vs. Solução 1 (Radar Híbrido)
A API Betfair fornece odds e placar ao vivo com precisão de segundos, mas feeds de estatísticas minuto a minuto (chutes no alvo / escanteios em tempo real) exigem APIs pagas especializadas. 
No entanto, **o Intervalo (HT) dura 15 minutos inteiros**. Não há correria de in-play aos 85'. O trader tem uma janela estrutural enorme para checar em 5 segundos no Sofascore/Flashscore se o favorito pressionou.

**Solução 1 — O Radar Híbrido HT:**
1. A VPS monitora o mercado Betfair Exchange 24/7.
2. Detecta jogos no intervalo (HT / min 45-55) com placar 0-0 onde o mandante era Super Fav pré-jogo (`<= 1.45`) e a odd Back live inflou para $\ge 1.75$.
3. Dispara alerta rico no Telegram com links diretos (Betfair + Sofascore) e um checklist binário de 5 segundos.
4. Registra o sinal em `radar_ht_log.csv` com `stake: 0.0` (Observação Stake-Zero).
5. Liquidador oficial via `list_market_book` (status autoritativo `WINNER/LOSER`).

---

## 2. O que Implementar na VPS

Implemente a solução mantendo o padrão já consolidado no `alerta_under_vps.py` e `late_goal_capturar.py`:

### (A) Capturador & Alerta: `radar_ht_favorito.py`
- **Loop / Polling:** a cada 60s inspeciona mercados in-play de futebol na Betfair.
- **Gatilho de Entrada:**
  - Jogo em andamento no Intervalo: `in_play == True`, tempo entre minutos 45 e 55 (ou status indicando HT/Halftime).
  - Placar atual: **0-0** (verificado via menor lay do CS == "0 - 0" ou mercado de Over 0.5 ainda não liquidado / placar verificado).
  - Pre-game Odd do Mandante: `Odd_H_pre <= 1.45` (ou $\le 1.50$ com flag). Pode ler do feed pré-jogo diário ou do snapshot pré-jogo do coletor.
  - Live Back Odd do Mandante no Match Odds: `odd_back_fav >= 1.75` e `<= 2.40`.
  - Liquidez: volume disponível no Back do Mandante $\ge R\$ 300$ (ou equivalente em GBP).
- **Anti-Duplicação:** cada `market_id` só pode disparar **1 vez**.
- **Log Central:** gravar em `radar_ht_log.csv` (modo `PENDENTE`, `stake: 0.0`, `tipo: OBSERVACAO_STAKE_ZERO`).

### (B) Mensagem Rica do Telegram
Disparar mensagem no Telegram utilizando o bot oficial configurado (`telegram_notifier.py` ou rotina interna da VPS):

```text
⚡ RADAR HT: FAVORITO 0-0 COM ODD INFLADA ⚡
━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 Competição: {competicao}
⚽ Jogo: {home} x {away}
⏱ Momento: Intervalo (HT - Min {minuto}')
📊 Placar Atual: 0 - 0

💰 Odd Pré Mandante: {odd_h_pre:.2f}
📈 Odd Back HT Betfair: {odd_back_ht:.2f} (Break-even: {be_wr:.1f}%)
💵 Liquidez Disponível: R$ {liq:,.0f}

🔗 Links de 1 Clique:
👉 [Mercado Betfair Exchange](https://www.betfair.com/exchange/plus/football/market/{market_id})
👉 [Ver Stats no Sofascore](https://www.sofascore.com/search?q={home_encoded})

━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 CHECKLIST DE PRESSÃO (5 Segundos no Sofascore):
[ ] Chutes no Alvo do Favorito no 1ºT >= 3?
[ ] Escanteios do Favorito no 1ºT >= 4?

🎯 CRITÉRIO QUANTITATIVO:
• Se SIM (pressão confirmada) ➔ ENTRADA APROVADA (+14.5% EV esperado, WR 63.2%)
• Se NÃO (jogo morno / sem chutes) ➔ SKIP (armadilha de 50.9% WR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### (C) Liquidador Oficial: `radar_ht_liquidar.py`
- Executado via `cron` (ex.: a cada 30 minutos ou no minuto :35 de cada hora).
- Lê linhas com status `PENDENTE` em `radar_ht_log.csv`.
- Chama a API da Betfair `list_market_book(market_ids=[...])` mesmo com o mercado `CLOSED`.
- Obtém o status do runner do mandante (`selection_id` do Home):
  - `WINNER` ➔ `resultado = GREEN`, `pnl = +0.955 * (odd_back_ht - 1)` (comissão 4.5%).
  - `LOSER` ➔ `resultado = RED`, `pnl = -1.0`.
- Grava resultado oficial, timestamp de liquidação e fecha a pendência.
- Implementa a **Stopping Rule Automática**: se o cohort de observação atingir $N \ge 50$ com $ROI < -15\%$, dispara alerta de anomalia no Telegram.

### (D) Documento de Pré-Registro: `PRE_REGISTRO_RADAR_HT.md`
Crie o arquivo no repositório congelando o desenho do experimento:
- **Hipótese:** Super Favorito empatando 0-0 no HT com odd $\ge 1.75$ tem valor esperado positivo quando acompanhado de pressão no 1º tempo.
- **Coorte 1 (Automática / Mercado Cru):** Todos os sinais capturados pelo bot (stake-zero). Serve de benchmark baseline para confirmar a média cega de 50-52% WR.
- **Coorte 2 (Com Pressão / Checklist Manual):** Sinais onde o usuário confirma no Telegram/log que o critério de pressão foi atendido.
- **N mínimo para auditoria:** $N \ge 100$ sinais.
- **Métrica de corte:** Bootstrap IC95 de ROI excluindo zero e CLV positivo frente ao fechamento do 2º tempo.

### (E) Governança e Serviços
1. Configure o serviço systemd na VPS (ex.: `radar-ht.service`) ou integre ao loop in-play existente.
2. Adicione a tarefa de liquidação no crontab.
3. Atualize o topo de `worklog.md` com a entrada do dia registrando a implantação.
4. Adicione o item no `tasks.md` na seção `🔴 ATIVO — em validação forward`.

---

## 3. Formato de Saída Esperado
Quando concluir a implantação, envie o status informando:
1. Status do serviço (`systemctl status radar-ht`).
2. Teste a seco de envio do alerta rico no Telegram.
3. Linhas iniciais de `radar_ht_log.csv` e confirmação do crontab de liquidação.
4. Resumo da entrada registrada no `worklog.md`.
