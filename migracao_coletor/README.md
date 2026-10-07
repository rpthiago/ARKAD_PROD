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
