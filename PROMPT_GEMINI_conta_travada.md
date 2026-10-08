# A conta da Betfair travou e a coleta está parada — veja se existe saída que não encontramos

Você restaurou a coleta em 07/10 com o proxy residencial UK apontando para `api.betfair.com`. Funcionou
por cerca de 3 horas. Agora a conta não loga mais. Abaixo está tudo que foi testado, com resultado
literal. **Não quero confirmação do que já está escrito — quero o que está faltando.**

## 1. Linha do tempo

| hora (UTC) | o quê |
|---|---|
| 07/10 ~09:30 | `api.betfair.bet.br` passa a devolver 302 → brasilsembets.gov.br. Coleta para. |
| 07/10 19:28 | Coleta restaurada via `api.betfair.com` + proxy UK. Ciclos de 850-920 cotações. |
| 08/10 00:36 | Aplicados sticky session (`_session-arkad1_lifetime-30m`) e `identity_uri` → `identitysso.betfair.com`. |
| 08/10 01:23 | Patch no loop: repetir o ciclo na hora após re-login, em vez de dormir 5 min. |
| 08/10 ~01:55 | Últimos ciclos bons: `[ciclo 1] 374 cotacoes | 11 jogos`, `[ciclo 2] 372`, `[ciclo 3] 364`. |
| 08/10 02:02:05 | `[ciclo 4] 0 cotacoes`. `keep_alive falhou (API keepAlive FAIL: NO_SESSION) -> re-login`. E então: **`API login: ACTIONS_REQUIRED`**. |

Desde então nenhum login é aceito.

## 2. Testes feitos, com resultado literal

### 2.1 Onde cumprir a ação exigida

| endpoint | de onde | resultado |
|---|---|---|
| `https://www.betfair.bet.br/` | VPS Brasil | HTTP 302 → `https://brasilsembets.gov.br/` |
| `https://betfair.bet.br/` | VPS Brasil | HTTP 302 → `https://brasilsembets.gov.br/` |
| `https://www.betfair.bet.br/` | proxy UK | HTTP 302 → `https://brasilsembets.gov.br/` |
| `https://www.betfair.com/` | proxy UK, User-Agent de navegador | HTTP 403, `<title>Just a moment...</title>` (desafio Cloudflare; **sem** bloqueio de região no corpo) |

No navegador do Thiago, com VPN:

- **VPN saindo na Noruega:** página "Restricted", `Region: NO`, `Your IP: 146.70.219.251`.
  (Mensagem: *"accessing the Betfair website from a country that Betfair does not accept bets from or
  the traffic from your network was detected as being unusual"*.)
- **Tentando o login de verdade:** redireciona para
  `identitysso.betfair.com/view/login?redirectMethod=GET&product=exchange-eds&submitForm=true&appliesTo=brazil&errorCode=AUTHORIZED_ONLY_F…`
  e exibe: *"Due to regulatory changes, access to your account via Betfair.com has been restricted.
  Please visit betfair.bet.br to log in."*

Ou seja: `.com` manda para `.bet.br`, e `.bet.br` não existe mais.

### 2.2 Fontes alternativas de dado

Feed `jogos-do-dia` da apicomunidade/FutPythonTrader, token válido, endpoint
`https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/{fonte}/{data}/`:

| data | betfair | bet365 |
|---|---|---|
| 06/10 | `"total":0` | `"total":0` |
| 07/10 | `"total":0` | `"total":0` |
| 08/10 | `"total":0` | `"total":0` |

Base b365 local (`football_data_odds.csv`): **viva**, atualizada 07/10 06:33, jogos até 06/10.
3,57 GB de capturas da Betfair (16/08 a 07/10) intactos no disco da VPS.

### 2.3 O que foi medido antes da queda (pode ser útil depois)

- A pool parece a mesma de antes do bloqueio. Comparando 06/10 23h (`.bet.br`) com 07/10 23h (`.com`):
  mesmos `selection_id` de runners padrão (Over 0.5 = 5851483, "0 - 0" = 1, Yes = 30246), mesmos 9 tipos
  de mercado, fill rate de lay 95% → 96%, `lay_size` mediano de Bangalore R$ 130,62 → R$ 131,06,
  `market_id` no mesmo formato (`1.263449003`).
- Sticky session funcionou: 6 chamadas seguidas pelo mesmo IP (antes eram 6 IPs diferentes).
- `identity_uri` corrigido: `keep_alive falhou` passou de 6 em 1h50 para **0 em 25 min**.
- Ainda assim, 1 ciclo zerado a cada ~30 min, coincidindo com a rotação do `lifetime-30m`.
- Tráfego: 4,97 MB em 20 min. Um ciclo completo custa **27 KB** (catálogo 49%, book 51%), mas a rede
  gasta ~0,248 MB/min — ou seja, só ~26% é dado útil; o resto é custo de abrir conexão (9 chamadas de
  catálogo + as de book por ciclo). Estimativa: **~13 GB/mês**. O Thiago tem 2 GB no iProyal.
- A coluna `matched` está **zerada nos dois regimes**, desde antes do bloqueio.

## 3. O que foi desligado — não religue sem resolver o login

Antes da pausa saíam **48 tentativas de login recusadas por dia** só do cron (4 liquidadores de 2 em 2
horas), mais uma a cada 5 minutos assim que o cron religasse o coletor às 07:00 UTC.

- Cron do root: 5 entradas comentadas com a marca `PAUSADO 08/10 ACTIONS_REQUIRED`
  (`systemctl start betfair-collector`, `settle_betfair.py` ×2, `late_goal_liquidar.py`,
  `radar_ht_liquidar.py`). Backup: `crontab_root.bak_pre_pausa`.
- `betfair-collector`, `liquidador-betfair`, `trader-inplay`, `sinais-ko`: `stop` + `disable`.
- Seguem no ar: `xg-ht` (FotMob) e `radar_periodos_vps.py` (só lê CSV).

Pendência aplicada mas **nunca testada** (o teste exigia login): cache de catálogo
(`--catalogo-cada 30`, padrão), que busca a lista de jogos de 30 em 30 min em vez de a cada ciclo.
Backups: `coletar_betfair_direto.py.bak_pre_cache`, `.bak_pre_retry`, `.bak_pre_semmeio`,
`.bak_pre_keepalive`, `.bak_pre_disjuntor`.

## 4. Perguntas

1. **Existe algum caminho para cumprir ou consultar o `ACTIONS_REQUIRED`** sem o site `.bet.br`?
   Algum endpoint da API que diga *qual* ação é exigida sem precisar de sessão válida? Se sugerir um,
   diga o que ele responde de fato.
2. **Qual canal de suporte da Betfair atende conta `.bet.br` hoje**, com a plataforma desligada?
   E-mail, telefone, formulário no `.com`? Diga qual, não suponha que existe.
3. **O `ACTIONS_REQUIRED` é consequência da rotação de IPs britânicos ou do encerramento pela MP?**
   Há evidência que separe as duas hipóteses? Isso decide se vale manter o proxy quando/se voltar.
4. **Fonte alternativa de odd de exchange** com cobertura parecida: odd de LAY em Match Odds, Correct
   Score e Over/Under, com liquidez, atualizada de minutos em minutos, e placar para liquidação.
   Cobertura real de ligas e custo, não só o nome.
5. **O feed da apicomunidade vai voltar?** Ele degrada desde 20/09 (211 jogos → 5 a 37 → zero).
   Vale cobrar o fornecedor ou procurar substituto?

## 5. Regras

- **Não tente login enquanto estiver `ACTIONS_REQUIRED`.** Cada recusa soma risco numa conta já
  sinalizada. Se um teste seu exigir login, proponha — não execute.
- **Verifique antes de afirmar.** Diga o que cada endpoint responde de fato. Já erramos nesta thread
  interpretando HTML genérico como bloqueio (um `405` em GET num endpoint que só aceita POST não é
  bloqueio) e lendo "Region: NO" como problema de conta quando era a VPN saindo na Noruega.
- **Não proponha burlar bloqueio determinado por autoridade.**
- Se a conclusão for "não há saída hoje, só esperar", diga isso com clareza em vez de oferecer um
  caminho frágil. O Thiago já decidiu que vai esperar, na hipótese de a MP cair.
