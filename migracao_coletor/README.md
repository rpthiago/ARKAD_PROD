# Coletor bloqueado no Brasil — diagnóstico e migração

**07/10/2026, ~06:20 BRT:** a Betfair deixou de responder à VPS de São Paulo. Confirmado na própria
máquina: `api.betfair.bet.br`, `api.betfair.com` e `identitysso-cert` devolvem **HTTP 302 → página HTML
do bloqueio nacional** (MP 1.394/26), em vez de JSON. Não é sessão expirada, não é a conta, não é o código.

## O que já foi feito (aplicado em 07/10)

**Disjuntor de bloqueio no coletor.** O loop interpretava o HTML como sessão morta e refazia login a cada
ciclo: **3.284 tentativas** em poucas horas. Login falhando em repetição é tratado como ataque e pode
travar a conta — a mesma conta do liquidador e de todo o histórico. Agora o coletor reconhece a página de
bloqueio, **não refaz login**, e espera 5 → 15 → 30 → 60 → 120 min entre tentativas, voltando sozinho
quando a API responder de novo. Backup do arquivo original em `coletar_betfair_direto.py.bak_pre_disjuntor`.

## A recomendação: mover o coletor para fora do Brasil

Entre as duas opções, **migrar o host** é melhor que **tunelar de dentro do Brasil**:

| | túnel/VPN saindo da VPS de SP | host fora do Brasil |
|---|---|---|
| natureza | contorna um bloqueio nacional a partir do Brasil | hospedagem em outro país, onde o serviço opera normalmente |
| estabilidade | depende do túnel; cai junto com ele | conexão direta |
| latência/consumo | todo o tráfego encapsulado, mais CPU no 1 GB | nenhum custo extra |
| manutenção | mais uma peça para quebrar às 3h da manhã | nenhuma |

Não é só questão técnica: montar um túnel especificamente para furar um bloqueio determinado por
autoridade é outra coisa, juridicamente, do que hospedar um servidor em outro país. Não sou a fonte certa
sobre o enquadramento, mas a diferença é real e vale considerar antes de escolher.

### Hosts candidatos

| opção | custo | RAM | observação |
|---|---|---|---|
| **Google Cloud e2-micro** (us-central1/us-west1/us-east1) | grátis permanente (1 instância) | 1 GB | mesma classe da atual; disco grátis de 30 GB exige rotação do histórico |
| Oracle Always Free em outra região | grátis | 1 GB | **só na home region** da conta — a sua é sa-saopaulo-1, então exigiria outra conta |
| AWS t3.micro | grátis 12 meses, depois pago | 1 GB | prazo limitado |
| VPS barata (Hetzner, Racknerd) | US$ 4–5/mês ou ~US$ 15/ano | 2 GB+ | mais folga de RAM e disco |

### A pergunta que decide, e que precisa ser testada ANTES

A sua conta é **betfair.bet.br** (plataforma brasileira). Hospedar fora resolve o bloqueio *da rede*, mas
resta saber se a Betfair BR aceita login de IP estrangeiro. Ninguém responde isso sem testar — e o teste
custa 2 minutos.

## Como executar

1. **Crie o host** (qualquer um da tabela) e rode lá:
   ```
   bash testar_acesso_betfair.sh
   ```
   Precisa aparecer **ACESSIVEL** em `api.betfair.bet.br` e em `login por certificado`.
   Se aparecer BLOQUEADO, aquele host não serve. Se só o `.com` estiver acessível, a conta brasileira não
   funciona de lá — e aí mudar de host não resolve o problema.

2. **Empacote na VPS atual** (já testado, gera 516 KB):
   ```
   bash empacotar_coletor.sh          # -> /tmp/coletor_pacote.tar.gz
   ```
   Leva código, `alerta.env`, certificados, mapas, `forward_ko_ledger.csv`, `placares_ft.csv`,
   `xg_ht_log.csv`, units do systemd, crontab e o `requirements.txt` exato do venv.
   **O pacote contém senha e certificado** — transfira por scp e apague depois.

3. **Instale no host novo**:
   ```
   bash instalar_coletor.sh coletor_pacote.tar.gz
   ```
   Ele instala dependências, recria o venv, corrige os caminhos dos certificados, instala os serviços e
   **testa o login antes de subir qualquer coisa**, dizendo se a falha foi bloqueio de rede ou recusa da
   Betfair BR por país.

4. **Traga o histórico** (3,1 GB) com limite de banda, uma transferência por vez:
   ```
   scp -l 24000 -i <chave> ubuntu@163.176.59.215:/home/ubuntu/betfair-collector/betfair_live_odds.csv .
   ```

5. **Atualize o IP** nos scripts do PC que falam com a VPS (relatório das 06:00, backup semanal,
   `xg_backfill_juntar.py`, `reliquidar_under_oficial.py`).

## Enquanto isso

A VPS de São Paulo continua útil: ela guarda os 3,1 GB de histórico e os serviços de estudo que não
dependem da Betfair. O disjuntor evita que ela gaste tentativas de login. Se o bloqueio cair, o coletor
volta sozinho na próxima janela.

---

## Criando o host novo — passo a passo (Google Cloud, e2-micro grátis)

A chave pública já está em `migracao_coletor/chave_publica_ssh.txt`, com o usuário `ubuntu` no fim
(o Google Cloud usa esse campo como nome de usuário). É a mesma chave que já abre a VPS atual, então
nada muda no seu lado.

1. **console.cloud.google.com** → criar conta (pede cartão para verificação; a e2-micro é grátis
   permanente, não é trial) → criar um projeto, por exemplo `arkad-coletor`.
2. **Compute Engine → VM instances → Create instance.**
3. **Region: `us-east1` (South Carolina)** — é a região grátis mais próxima do Brasil. Zone: qualquer.
4. **Machine configuration:** série **E2**, tipo **e2-micro** (2 vCPU compartilhadas, 1 GB).
   Confira que aparece "Your first 744 hours of e2-micro are free".
5. **Boot disk → Change:** Ubuntu **22.04 LTS**, tipo **Standard persistent disk**, **30 GB**
   (é o teto do free tier; acima disso passa a cobrar).
6. **Advanced options → Security → Manage Access → Add manually generated SSH keys → Add item**
   e cole o conteúdo de `chave_publica_ssh.txt` **inteiro, incluindo o ` ubuntu` no final**.
7. **Create.** Anote o **External IP** da instância.

Depois disso é comigo: com o IP em mãos eu rodo o teste de acesso, empacoto, migro e subo os serviços.

### Se preferir pagar para resolver um problema antigo
O 1 GB de RAM já causou dois incidentes reais (o `trader-inplay` em crash-loop por 33 h e o travamento de
3 h de 16/09). Uma VPS de ~US$ 4-5/mês (Hetzner CX22, 4 GB, 40 GB de disco) elimina essa classe de
problema. Mas o caminho de menor arrependimento é testar primeiro no grátis: se a Betfair BR recusar
login de IP estrangeiro, mudar de host não resolve e você não gastou nada descobrindo.


---

## RESULTADO DO TESTE (07/10, host us-east1 criado e testado)

Host novo: `arkad-coletor`, Google Cloud e2-micro, us-east1-b, Ubuntu 22.04, IP **136.108.221.253**.
SSH funcionando com a mesma chave da VPS antiga.

Teste de acesso rodado de lá:

| endpoint | resposta dos EUA |
|---|---|
| `api.betfair.bet.br` (conta BR) | **302 → brasilsembets.gov.br** |
| `api.betfair.com` (internacional) | 403 desafio do Cloudflare — host alcançável |
| `identitysso-cert` (POST) | 400 do próprio servidor Betfair — **alcançável** |

**Conclusão que muda a decisão: não é geo-bloqueio.** O domínio `api.betfair.bet.br` redireciona para a
página do governo **de qualquer país** — a plataforma brasileira foi desligada, não apenas filtrada na
rede brasileira. Nenhuma mudança de hospedagem resolve, e um túnel também não resolveria.

A Betfair internacional (`.com`) responde normalmente dos EUA, mas exige **conta betfair.com**, que é
plataforma separada da `.bet.br` — as credenciais não são intercambiáveis.

**O teste anterior deu leitura errada** porque marcava qualquer HTML como bloqueio: um `405` em GET num
endpoint que só aceita POST não é bloqueio. O script foi corrigido para separar bloqueio da MP, desafio
do Cloudflare e resposta real do servidor, usando o método HTTP correto em cada endpoint.

---

## VERIFICAÇÃO DA COLETA RESTAURADA (07/10, 23h UTC)

A coleta voltou pela `api.betfair.com` com login mTLS em `identitysso-cert.betfair.bet.br`, através de
proxy residencial UK. Serviços todos ativos (`betfair-collector`, `liquidador-betfair`, `trader-inplay`,
`sinais-ko`, `xg-ht`), disjuntor nunca acionou, liquidador fechando mercados (55 numa passagem).

### A base continua comparável? Sim, até onde é verificável

Comparando a mesma hora em dias consecutivos — 06/10 23h (`.bet.br`) contra 07/10 23h (`.com`):

| teste | 06/10 (`.bet.br`) | 07/10 (`.com`) |
|---|---|---|
| `selection_id` de runners padrão (Over 0.5 / "0 - 0" / Yes) | 5851483 / 1 / 30246 | **iguais** |
| tipos de mercado capturados | os mesmos 9 | os mesmos 9 |
| MATCH_ODDS com lay disponível | 95% | 96% |
| `lay_size` mediano — Bangalore Super Division | R$ 130,62 | R$ 131,06 |
| `lay_size` mediano — Bolivian Cup | R$ 162,56 | R$ 199,33 |
| `lay_size` mediano — Chilean Cup | R$ 230,48 | R$ 272,07 |

Formato de `market_id` também inalterado (`1.263449003`). Apareceu R$ 19,6 mil de `back_size` em
Bragantino x Mirassol, ou seja, dinheiro brasileiro real chega pela `.com`. **Não dá para provar com os
dois endpoints lado a lado**, porque o `.bet.br` morreu — a evidência é indireta, mas consistente.

Achado lateral: a coluna `matched` está **zerada nos dois regimes**. Falha antiga do coletor, não do
proxy. Liquidez só pode ser medida por `back_size`/`lay_size`.

### Dois defeitos residuais

**1. O proxy está em modo rotativo.** Seis chamadas seguidas saíram por seis IPs diferentes
(80.40.102.214, 81.147.1.154, 82.0.119.205, 5.69.110.195, 82.7.177.196, 89.243.66.110). A Betfair
invalida a sessão quando o IP muda, então:

- **22% dos ciclos saem zerados** (`[ciclo 7] 0 cotacoes` seguido de "refazendo login") — buraco de
  5 minutos cada;
- **42 re-logins em 1h50** no liquidador. São logins que *dão certo*, então não é o caso dos 3.284 de
  manhã — mas é login repetido vindo de IP novo cada vez, que é padrão que casa de aposta trata como
  suspeito.

Conserto: *sticky session* na credencial do iProyal, acrescentando à **senha** (não ao host):
`_session-arkad1_lifetime-30m`. Não mexe em endpoint nem em regra de método.

**2. `keep_alive` aponta para um host morto.** `coletar_betfair_direto.py:65` ainda tem
`t.identity_uri = "https://identitysso.betfair.bet.br/api/"`, que devolve 302 → brasilsembets. Logo
todo `keepAlive` falha e cai no re-login. O endpoint global responde JSON pelo proxy:

```
identitysso.betfair.com/api/keepAlive  -> HTTP 200 {"status":"FAIL","error":"INPUT_VALIDATION_ERROR"}
identitysso.betfair.bet.br/api/keepAlive -> HTTP 302 brasilsembets.gov.br
```

Não apliquei: mudar o endpoint de autenticação é alteração que precisa da sua decisão explícita.

---

### Resolução Aplicada e Validada (08/10, 00h35 UTC · Antigravity)

1. **Sticky Session no IPRoyal:**
   - Adicionado `_session-arkad1_lifetime-30m` à senha no `.env`:
     `BETFAIR_PROXY=http://97AmEBoDr4acyTIs:wUXBDw9ofD5vbyUc_country-gb_session-arkad1_lifetime-30m@geo.iproyal.com:12321`
   - Testado e validado: o mesmo IP residencial UK (`86.174.65.24`) é mantido em requisições consecutivas, eliminando as quedas por rotação.

2. **Endpoint `identitysso.betfair.com` para KeepAlive:**
   - Atualizado em `coletar_betfair_direto.py`: `t.identity_uri = "https://identitysso.betfair.com/api/"`.
   - Testado e validado: `t.keep_alive()` retornou `SUCCESS` sem erros.
   - Serviços `betfair-collector.service` e `liquidador-betfair.service` reiniciados e rodando com ciclo 100% preenchido.


---

## 08/10, 01:08 UTC — coleta reduzida a pré-jogo + fim de jogo

Decisão do Thiago: os métodos aprovados não usam in-play, então o coletor passou a rodar com
`--sem-meio-jogo` (flag nova em `coletar_betfair_direto.py`, já no `ExecStart` do unit).

A flag filtra o **book**, não o catálogo: puxa `mtk >= 0` (pré-jogo) e `mtk <= -95` (fim de jogo,
min ~80-105), e pula o minuto 0-80. O catálogo continua olhando 2,5 h para trás **de propósito** —
o `--horas-atras 0` que já existia teria desligado também a captura de fim de jogo, porque
`passar_ft()` reaproveita o `meta` daquele catálogo.

Verificado em produção: `[sem-meio-jogo] 38 mercados em andamento pulados` → `[ciclo 1] 254 cotações`,
`[ft-rapido] 120 cotações | 24 jogos no fim`, e nos 6 min seguintes 216 linhas de pré-jogo, 802 de fim
de jogo, 1 de meio (mercado que cruzou o limite entre o filtro e a leitura).

Economia medida: meio de jogo era **14,4%** das linhas (12.284 de 85.125 em 07-08/10). O volume é
dominado pelo fim de jogo (24%), que re-puxa a cada 45 s e foi mantido. Sobre ~6 GB/mês de proxy, são
uns US$ 3-6/mês — o ganho real é menos escrita no CSV de 3,57 GB e menos superfície de falha.

**Custo da decisão:** `trader-inplay` perde a fonte (usa `minuto = int(-min_to_ko - 15)`), e com ele os
candidatos in-play em teste cego. Odd ao vivo não se reconstrói retroativamente. `alerta-under` já
estava inactive; o Late Goal sobrevive (minuto 82-86 cai na janela de fim de jogo).

Backups: `coletar_betfair_direto.py.bak_pre_semmeio`, `betfair-collector.service.bak_pre_semmeio`.

### Defeito que continua

`lifetime-30m` na sticky session faz o IP rotacionar a cada 30 min; a sessão morre junto e custa um
ciclo inteiro (1 zerado em 8 após o restart — era 22%, caiu para ~12%). O `keep_alive`, esse sim,
está resolvido: zero falhas e zero re-logins em 25 min de observação. O conserto do resto não é
aumentar o lifetime, é o loop refazer o login e **repetir o ciclo na hora** em vez de dormir 5 min.

### Revertido em 08/10, 01:23 UTC — voltou a coleta inteira

A economia de 14,4% foi considerada pouca para o preço de perder o in-play. O `ExecStart` voltou a
`--horas 6 --intervalo 300`; a flag `--sem-meio-jogo` fica no código, desligada, para quando fizer
sentido. Confirmado: `[ciclo 1] 390 cotações | 12 jogos` sem a linha de mercados pulados (eram 9 jogos
com a flag). `trader-inplay` volta a receber dado — perdeu só os 15 min do experimento.

### Conserto do ciclo zerado

A cada 30 min o sticky session do iProyal troca de IP, a Betfair derruba a sessão, todos os catálogos
falham e o ciclo sai zerado. O loop fazia o re-login e **ia dormir até o próximo intervalo**, então cada
queda custava 5 minutos inteiros de gravação.

Agora, depois do re-login bem-sucedido, ele **repete o ciclo na hora**. O ciclo repetido aparece no log
como `[ciclo Nr] ... (repetido na hora apos re-login)`. Patch em `main()`, logo após
"catalogo falhou em todos os mercados". Backup: `coletar_betfair_direto.py.bak_pre_retry`.

Aumentar o `lifetime` da sticky session seria tratar o sintoma: a sessão cai de qualquer forma quando o
IP muda, e com o retry o custo da queda deixa de existir.

---

## 08/10 — PAUSA: conta travada, nada tenta login

`API login: ACTIONS_REQUIRED` desde 08/10 02:02 UTC. Não há site onde cumprir a ação:
`betfair.bet.br` devolve 302 → brasilsembets.gov.br (do Brasil **e** do Reino Unido), e
`betfair.com` responde *"Due to regulatory changes, access to your account via Betfair.com has been
restricted. Please visit betfair.bet.br to log in"* (`appliesTo=brazil`, `errorCode=AUTHORIZED_ONLY_F…`).
Circuito fechado: cada plataforma manda para a outra.

Antes da pausa, **48 tentativas de login por dia** saíam do cron (4 liquidadores de 2 em 2 horas), mais
uma a cada 5 min assim que o cron religasse o coletor às 07:00 UTC. Login falhando em repetição numa
conta já sinalizada é o pior cenário possível, então tudo foi parado.

### O que foi desligado

| o quê | como |
|---|---|
| `0 7 * * * systemctl start betfair-collector` | comentado no cron do root |
| `settle_betfair.py` (×2), `late_goal_liquidar.py`, `radar_ht_liquidar.py` | comentados (marca `PAUSADO 08/10 ACTIONS_REQUIRED`) |
| `betfair-collector`, `liquidador-betfair`, `trader-inplay`, `sinais-ko` | `stop` + `disable` |

Backup do cron: `crontab_root.bak_pre_pausa`. Continua no ar: `xg-ht` (FotMob) e `radar_periodos_vps.py`.

### Para retomar, quando o login voltar

```bash
cd /home/ubuntu/betfair-collector
set -a; . ./.env; set +a
./venv/bin/python -c "import coletar_betfair_direto as C; t=C.login(); print('LOGIN OK', t.account.get_account_funds().available_to_bet_balance)"
#   só depois que a linha acima imprimir LOGIN OK:
sudo crontab -l | sed -E 's/^# PAUSADO 08\/10 ACTIONS_REQUIRED: //' | sudo crontab -
for u in betfair-collector liquidador-betfair trader-inplay sinais-ko; do sudo systemctl enable --now $u; done
```

### Pendência que ficou sem teste

O **cache de catálogo** (`--catalogo-cada 30`, padrão) está aplicado e compila, mas nunca rodou — o teste
exigia login. Ele busca a lista de jogos de 30 em 30 min em vez de a cada ciclo, mantendo as odds de 5 em
5. Estimativa: corta 40-50% do tráfego de proxy (medido: ~13 GB/mês, e só ~26% disso é dado útil; o resto
é custo de abrir conexão, 9 chamadas de catálogo por ciclo). Ao retomar, conferir no log a linha
`[catalogo em cache] N mercados | renova em M min` e medir o tráfego de novo.
Backup: `coletar_betfair_direto.py.bak_pre_cache`.

---

## 08/10 — detecção específica de recusa do proxy

O proxy residencial é pré-pago. Quando o saldo acaba, ele recusa o CONNECT e o sintoma fica **idêntico
ao do bloqueio da MP**: zero cotações, erro na API, disjuntor abrindo — sem nada dizer que acabou o
crédito. Já custou meia hora de diagnóstico uma vez.

Agora o coletor separa **três** causas, nesta ordem:

| causa | como reconhece | o que faz |
|---|---|---|
| **proxy recusou** | `proxyerror`, `proxy authentication`, `tunnel connection failed`, `cannot connect to proxy`, `407 proxy`, … | avisa no Telegram e abre o disjuntor, **sem** refazer login |
| bloqueio da MP | `brasilsembets`, `<!doctype html`, `medida provisoria`, … | abre o disjuntor, sem refazer login |
| sessão expirada | qualquer outro erro com catálogo vazio | refaz login e **repete o ciclo na hora** |

A mensagem no Telegram diz em uma linha o que é: *"o PROXY recusou a conexao. A coleta parou. Causa
mais provavel: acabou o saldo de trafego do iProyal. Nao e bloqueio da MP e nao e sessao expirada."*
Limitada a **1 aviso a cada 6 horas**, para não virar spam durante a espera do disjuntor.

Testado antes de subir, com atenção aos falsos positivos — o risco real era um `market_id` como
`1.263407811` ser lido como erro 407:

| caso | detecta proxy? | esperado |
|---|---|---|
| `Tunnel connection failed: 407 Proxy Authentication Required` | sim | sim |
| `ProxyError: Cannot connect to proxy` | sim | sim |
| HTML do brasilsembets | não (cai em bloqueio) | correto |
| `INVALID_SESSION_INFORMATION` | não | correto |
| `erro no mercado 1.263407811` | **não** | correto |
| `Read timed out` | não | correto |

Envio de Telegram testado de verdade (uma mensagem entregue, a segunda silenciada pelo limite).
Backup: `coletar_betfair_direto.py.bak_pre_proxydet`.

### Confirmado em produção no mesmo restart

`[ciclo 7r] 720 cotacoes | 20 jogos | 9 mercados | 23s (repetido na hora apos re-login)` — o conserto
do ciclo zerado disparou pela primeira vez e recuperou um ciclo que antes teria virado buraco de 5 min.
