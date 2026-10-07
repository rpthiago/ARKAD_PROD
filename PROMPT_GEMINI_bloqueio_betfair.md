# PROMPT PARA O GEMINI — a coleta de odds parou; procure uma saída que eu não tenha achado

Contexto para você revisar com ceticismo o diagnóstico abaixo e, se houver, apontar um caminho que não
foi considerado. **Não quero confirmação do que já está escrito — quero o que está faltando.**

## 1. O que aconteceu

07/10/2026, por volta das 06:20 BRT, a VPS coletora (Oracle Cloud, Vinhedo/SP, IP brasileiro
163.176.59.215) parou de receber cotações. O serviço continuava ativo, mas cada ciclo de 5 minutos
retornava `0 cotacoes | 0 jogos | 0 mercados`. O log mostrava a API da Betfair devolvendo HTML em vez de
JSON.

## 2. Tudo o que foi testado, com resultado literal

### 2.1 Da VPS brasileira (IP de datacenter, São Paulo)

| endpoint | método | resultado |
|---|---|---|
| `api.betfair.bet.br/exchange/betting/rest/v1.0/` | GET | HTTP 302, `location: https://brasilsembets.gov.br/` |
| `api.betfair.com/exchange/betting/rest/v1.0/` | GET | HTTP 302, mesma página |
| `identitysso-cert.betfair.com/api/certlogin` | GET | HTTP 405 + HTML |

Resolução DNS normal (Cloudflare: 104.18.39.87, 172.64.148.169). Não é DNS envenenado nem firewall local.

### 2.2 Criei um host fora do Brasil para testar

Google Cloud, **e2-micro, us-east1-b (Carolina do Sul), Ubuntu 22.04**, IP **136.108.221.253**, acessível
por SSH com a mesma chave da VPS antiga. Custo zero (free tier permanente).

| endpoint | método | resultado **dos EUA** |
|---|---|---|
| `api.betfair.bet.br/exchange/betting/json-rpc/v1` (o que o `betfairlightweight` usa) | POST + headers | **HTTP 302 → brasilsembets.gov.br** |
| `api.betfair.bet.br/.../rest/v1.0/listEventTypes/` | POST + headers | HTTP 302 → brasilsembets.gov.br |
| `identitysso-cert.betfair.bet.br/api/certlogin` | POST | **HTTP 400, "Application Server - Error report"** — servidor real respondendo |
| `api.betfair.com/exchange/betting/json-rpc/v1` | POST + headers | **HTTP 403, "Attention Required! Cloudflare"** (cf-ray ...-ATL) |
| `identitysso-cert.betfair.com/api/certlogin` | POST | HTTP 400, servidor real respondendo |

### 2.3 O feed de dados alternativo (apicomunidade / FutPythonTrader)

Endpoint `jogos-do-dia`, chamado com o token válido, **dos dois lugares**, mesmas datas:

| data | Brasil (betfair / bet365) | EUA (betfair / bet365) |
|---|---|---|
| 05/10 | total 0 / 0 | total 0 / 0 |
| 06/10 | total 0 / 0 | total 0 / 0 |
| 07/10 | total 0 / 0 | total 0 / 0 |

HTTP 200 nos dois casos, `total: 0`. Histórico de degradação: 211 jogos em 20/09 → 5 a 37 jogos entre
21/09 e 03/10 → zero agora. As bases históricas consolidadas (`.cache_base_betfair.csv` e a base b365)
pararam em **24/09**, embora o job de atualização rode diariamente às 06:00 sem erro.

### 2.4 Conta Betfair

Saldo disponível R$ 0,71. Login por certificado funcionava normalmente até ontem (o liquidador oficial
fechou mercados em 03/10 às 14:25). A conta é **betfair.bet.br**.

## 3. O que eu concluí (e quero que você ataque)

1. `api.betfair.bet.br` redireciona para `brasilsembets.gov.br` **de qualquer país** → a plataforma
   brasileira foi desligada, não é geo-bloqueio de rede. Trocar hospedagem ou usar túnel/VPN não resolve.
2. `api.betfair.com` é alcançável dos EUA mas devolve 403 do Cloudflare para IP de datacenter do Google
   Cloud, antes de qualquer autenticação. E, mesmo que passasse, exigiria conta `.com`, que é plataforma
   separada da `.bet.br` (credenciais não são intercambiáveis).
3. O feed da apicomunidade está vazio para todo mundo porque a fonte dele é a mesma Betfair BR.
4. Logo: **não há caminho técnico para retomar a coleta com a conta atual.**

## 4. O que já foi feito de mitigação

- **Disjuntor no coletor** (`coletar_betfair_direto.py`): o loop interpretava o HTML como sessão expirada
  e refazia login a cada ciclo — **3.284 tentativas em poucas horas**, risco de a Betfair travar a conta.
  Agora reconhece a página de bloqueio, não refaz login, e espera 5 → 15 → 30 → 60 → 120 min, voltando
  sozinho se a API responder de novo. Backup em `coletar_betfair_direto.py.bak_pre_disjuntor`.
- **Kit de migração** pronto em `migracao_coletor/` (empacotar, instalar em host novo, testar acesso),
  hoje sem uso dado o diagnóstico.
- **Patrimônio intacto:** 3,1 GB de capturas da Betfair (16/08 → 07/10, 5 em 5 min, 8 mercados por jogo),
  base b365 com 248 mil jogos, `estatisticas_jogos.csv` com xG, ledgers e histórico de sinais.

## 5. As perguntas

1. **Algum endpoint ou host da Betfair que eu não testei** e que ainda sirva para ler mercados com conta
   `.bet.br`? (ex.: Stream API `stream-api.betfair.com`, `historicdata.betfair.com`, endpoints de conta,
   Navigation Data `api.betfair.com/exchange/betting/rest/v1/en/navigation/menu.json`.) Se achar, diga
   qual e o que ele devolve — não suponha.
2. **O 403 do Cloudflare na `.com` é da faixa de IP do Google Cloud?** Se sim, um provedor com IP menos
   marcado (residencial, ou outro datacenter) mudaria o resultado? Vale a pena testar, sabendo que a
   barreira de conta continuaria de pé?
3. **Fontes alternativas de odd de exchange** com cobertura parecida (Betdaq, Matchbook, Smarkets, ou
   agregadores com API). O que interessa ao ARKAD: **odd de LAY** em Match Odds, Correct Score e
   Over/Under, com liquidez, atualizada de minutos em minutos, e placar para liquidação. Diga cobertura
   real de ligas e custo, não só o nome.
4. **Para o objetivo declarado hoje — captar dados para estudo, não apostar** — existe fonte de odd
   histórica/ao vivo que não dependa de conta em casa de aposta?
5. O que você faria com a VM nova (e2-micro, us-east1, grátis): deletar ou aproveitar para quê?

## 6. Regras

- Verifique antes de afirmar. Qualquer endpoint que você sugerir, diga **o que ele responde de fato** —
  nós dois já erramos hoje por interpretar HTML genérico como bloqueio (um `405` em GET num endpoint que
  só aceita POST não é bloqueio, e foi exatamente esse erro que escondeu, por um tempo, que o problema
  real era o 302 para o site do governo).
- Não proponha burlar bloqueio determinado por autoridade. Hospedar em outro país é uma coisa; montar
  túnel para furar bloqueio nacional é outra, e essa não está em discussão.
- Se a conclusão for "não há saída hoje", diga isso com clareza em vez de oferecer um caminho frágil.
