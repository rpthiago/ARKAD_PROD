# Conferência da VPS em 08/10 04:21 UTC — o que bate, o que não bate, e o que eu religuei

Conferi na máquina o relato de que a infraestrutura está "100% em pé e automatizada". A VPS está viva e
o login voltou, mas quatro pontos do relato não correspondem ao que está rodando. Segue o que foi medido,
com arquivo e linha, para você verificar por conta própria.

## 1. O que confirmei

| item | estado medido |
|---|---|
| login mTLS pelo proxy UK | funcionando — o `ACTIONS_REQUIRED` foi liberado |
| `betfair-collector` | `active`/`enabled` desde 04:05; ciclos de 530 cotações / 15 jogos |
| gravação ao vivo | última linha às 04:15:38 em `betfair_live_odds.csv` (3,58 GB) |
| `sinais-ko` | `active`/`enabled` desde 04:05, loop 60s |
| janela do KO | `sinais_ko_core.py:15` → `JANELA_KO = (4.0, 16.0)` minutos, compatível com "15 min antes" |
| radar nos 3 blocos | cron do usuário às 09:00 / 13:30 / 19:30 UTC = 06:00 / 10:30 / 16:30 BRT |
| cache de catálogo | **funcionando**: `[catalogo em cache] 130 mercados | renova em 25 min`; ciclo caiu de 12s para 7s |

## 2. O que não corresponde ao relato

**2.1 O robô não aplica os tetos de 7,00 e 8,00.** Em `sinais_ko_core.py`:

```
linha 112:  # 3. Lay Draw: min(H,A) <= 1.40 e 4.5 <= lay empate <= 10
linha 126:  if a and a <= 1.65 and hl and 2.0 <= hl <= 10.0:   # Lay Home
```

As janelas em produção são **4,5–10,0** (Draw) e **2,0–10,0** (Home), não `≤ 7,00` e `≤ 8,00`.
O Telegram vai apitar jogos acima do teto que o backtest mediu. Decisão em aberto: o teto entra no
código (e aí o alerta = cohort testado) ou continua só no clique manual (e aí o volume de alertas é
maior que o cohort, e isso precisa estar claro para quem recebe o alerta).

**2.2 O radar continua emitindo os métodos de Correct Score.** `sinais_ko_core.py:18-19` lista seis:

```
"Lay 0x3 Top 3", "Lay 0x3 (Regra Ampla)", "Lay 2x2 Top 3",
"Lay Draw (Fav<=1.40)", "Lay Home/DC X2 (FavVis<=1.65)", "Lay Over 4.5 (Under Pesado)"
```

E `radar_periodos_vps.py` filtra `MATCH_ODDS, OVER_UNDER_25, OVER_UNDER_45, CORRECT_SCORE` com
`p[7] == "2 - 2"`. São os métodos que a nova estratégia declarou sem viabilidade operacional.

**2.3 O radar ainda não rodou nenhuma vez.** `radar_periodos.log` não existe no disco, e
`/var/spool/cron/crontabs/ubuntu` foi modificado às 01:47 UTC de hoje. O primeiro disparo é às
09:00 UTC (06:00 BRT).

**2.4 A liquidação estava desligada.** Esse era o ponto sério:

- `liquidador-betfair`: `inactive` + `disabled`
- as 4 entradas de cron de liquidação (`settle_betfair.py` ×2, `late_goal_liquidar.py`,
  `radar_ht_liquidar.py`): comentadas com a marca `PAUSADO 08/10 ACTIONS_REQUIRED`
- `placares_ft.csv` congelado às **01:37 UTC**
- `forward_ko_ledger.csv`: 762 linhas, **100% com status PENDENTE**

Com isso, qualquer entrada em micro-stake a partir de hoje não teria resultado registrado.

## 3. O que eu religuei (04:21 UTC)

Eu mesmo tinha desligado isso ontem às 01:37, quando o login estava recusando com `ACTIONS_REQUIRED` e
saíam 48 tentativas recusadas por dia do cron. Esse motivo acabou, então desfiz:

```
sudo systemctl enable --now liquidador-betfair
sudo crontab -l | sed -E 's/^# PAUSADO 08\/10 ACTIONS_REQUIRED: //' | sudo crontab -
```

Verificado depois: `liquidador-betfair` `active`/`enabled`, zero entradas `PAUSADO` restantes, e o
liquidador voltou a gravar — `04:21:11 [passagem] mapa +80 mercados | liquidados 6`, com
`placares_ft.csv` indo de 3.836 para 3.842 linhas.

Backups na VPS: `crontab_root.bak_pre_religar` (antes de religar) e `crontab_root.bak_pre_pausa`
(antes de pausar). **Possível lacuna:** jogos encerrados entre 01:37 e 04:21 UTC podem não ter tido o
placar capturado, já que o liquidador lê o livro enquanto o mercado está aberto. Vale conferir se os
jogos daquela faixa ficaram sem placar e, se ficaram, resolver por outra fonte.

## 4. O que a auditoria do backtest de agosto-setembro mediu

Documento completo em `AUDITORIA_BACKTEST_AGO_SET_claude.md`. Os números que interessam aqui:

- **O script está limpo**: condições de green corretas nos três métodos, liability e break-even
  corretos, zero chaves duplicadas no merge, sem look-ahead.
- **O ponto sobre o regime da base se confirma.** Spread interno da Betfair (lay ÷ back, independente
  da Bet365): 1,0735 (2024) → 1,0725 (2025) → **1,0441 (2026)**. Refiz meu teste anterior por ano:
  2024 −0,72%, 2025 −2,00%, **2026 +0,20%**. Meu agregado negativo era puxado por 2024-25.
- **Significância**: Lay Draw com N=426 tem edge de +1,39 pp contra erro-padrão de 1,84 pp (z = 0,76,
  p = 0,224); Lay Home com N=271 tem +2,24 pp contra 2,58 pp (z = 0,87, p = 0,193). Para significância
  seriam necessários **2.854** e **1.382** jogos — cerca de 12 meses no ritmo atual.
- **13 células foram testadas** (5 tetos no Draw, 4 no Away, 4 no Home) e as duas melhores reportadas
  sem FDR. O teto do Draw migrou de 7,50 para 7,00 entre auditorias, no mesmo período de dados.
- **Comissão**: derivando a odd representativa, a 14% o Lay Draw dá −0,45% e o Lay Home dá exatamente
  0,00%. A 3% dão +1,71% e +3,02%. Toda a tese depende de qual é a comissão real do OrbitX.

## 5. Perguntas

1. **O teto entra no código ou fica manual?** Se ficar manual, como o alerta deixa claro que o jogo
   apitado pode estar fora da regra testada?
2. **Os quatro métodos de CS continuam no radar de propósito?** Se não, o que sai e o que fica.
3. **Qual é a comissão real do OrbitX, por extrato?** É a variável de maior impacto e a mais barata de
   checar.
4. **Qual é a definição exata de `liga_draw_rate` no código de produção** (janela, mínimo de jogos,
   agrupamento de liga)? Preciso dela para refazer o teste do Lay Draw congelado restrito a 2026 — o
   número que reportei antes (−3,49%) usou todos os anos e carrega o viés de regime que acabei de
   corrigir. Essa é a pergunta mais importante em aberto.
5. **Os jogos entre 01:37 e 04:21 UTC ficaram sem placar?** Se sim, qual fonte usar para fechar.

## 6. Regras

- Verifique na máquina antes de afirmar estado. Três dos quatro pontos acima eram afirmações sobre
  serviços que não correspondiam ao que `systemctl` e os arquivos mostravam.
- Se religar ou pausar qualquer coisa, deixe backup e diga qual, como foi feito aqui.
- Não ajuste parâmetro de método (teto, filtro) olhando o período que já está sendo usado como
  evidência. O teto do Draw já mudou uma vez dentro da mesma amostra.
