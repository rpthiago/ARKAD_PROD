# Handover — o que o Claude fez entre 12/10 e 19/10

Resumo do que mudou, o que está pendente e o que não pode ser desfeito sem quebrar algo.
Não é pedido de opinião: é estado do sistema.

---

## 1. URGENTE — credencial do proxy vazou no GitHub

**O que aconteceu:** o commit `fceaffb` (12/10) levou o `migracao_coletor/README.md` com a credencial
do iProyal em texto puro na linha 200. O repositório tem remoto em `github.com/rpthiago/ARKAD_PROD` e
o commit foi enviado. Ficou exposta de 12/10 a 19/10.

**Responsabilidade:** foi erro do Claude — `git add` no README inteiro sem ler o diff. Fica a regra:
**antes de commitar arquivo que outra pessoa editou, ler o que está sendo adicionado.**

**O que já foi feito:**

| item | estado |
|---|---|
| credencial nova (sub-user `arkad_coletor`) criada no iProyal | ✓ |
| `.env` da VPS atualizado e testado antes do restart (saída UK, Virgin Media) | ✓ |
| coletor reiniciado e coletando (`[ciclo 1] 530 cotacoes`) | ✓ |
| backup do `.env` | `.env.bak_pre_subuser` |
| README mascarado localmente (`<usuario>:<senha>`) | ✓ **não commitado** |
| credencial antiga revogada | ✗ — o botão Reset do painel não funcionou |

**Mitigação em vigor:** todo o tráfego está atribuído ao sub-user; o pool principal tem **0,01 GB**.
A credencial vazada ainda autentica, mas só alcança ~10 MB. **Regra que fica: GB comprado vai sempre
para o sub-user, nunca fica no pool principal.**

**PENDENTE e importante:** existe `migracao_coletor/README.md.bak_pre_mascara` **com a credencial
antiga em texto puro**, dentro da pasta do repositório. **Não commitar.** Deve ser apagado junto com o
commit do README mascarado.

---

## 2. Estado do tráfego — a coleta vai parar

A conta tem recarga automática em blocos de 1 GB. O bloco acabou, a carteira tem US$ 0,75 e o próximo
bloco custa ~US$ 7, então a recarga falhou. Mensagem recebida: *"You Have Reached Your Balance Limit"*.

**Esclarecimento que vale registrar:** os GB que aparecem no sub-user são **teto de consumo**, não
tráfego comprado. Tráfego só existe depois de um pedido. É a mesma lição que já estava no README sobre
"Add funds" só creditar a carteira.

**Decisão do Thiago:** deixar acabar e então testar um VPS próprio como proxy. Não recarregar.

Quando acabar: o log vai mostrar `[PROXY] recusou a conexao`, o alerta cai no Telegram, e o disjuntor
espera 5 → 15 → 30 → 60 → 120 min. Nenhum login em repetição, conta Betfair protegida.

**Script pronto:** `migracao_coletor/instalar_proxy_vps.sh` instala 3proxy num VPS Ubuntu novo e já
testa se a Betfair responde de lá (distingue bloqueio do Cloudflare, bloqueio da MP e resposta real).

---

## 3. Conserto que não pode ser revertido

`sinais_dia_coletor.sincronizar_ledger_vps()` fazia **scp do ledger da VPS por cima do arquivo do PC**.
Como nada na VPS liquida o ledger, cada chamada apagava todo o histórico liquidado — e isso roda em
cada bloco do radar e a cada clique de Escanear nas páginas 04 e 05.

Agora baixa para `.cache_forward_ko_vps.csv` e **mescla** pela chave `(Data, Metodo, Home, Away)`.
Testado: 761 linhas liquidadas sobreviveram a 3 sincronizações seguidas. Commit `8e970d2`.

Junto com isso, o commit `c7d6606` consertou o BOM que travava `atualizar_ko()` por completo.

**Se qualquer um dos dois for revertido, a liquidação volta a se apagar sozinha todo dia.**

---

## 4. Os estudos lacrados para 12/10 — rodados em 13/10

Os scripts foram executados como estavam, sem tocar em parâmetro.

**CS no coletor** (`varredura_cs_coletor.py`) — snapshot 2 salvo em
`varredura_over/cs_coletor_2026-10-13.csv`:

| | 13/09 | 13/10 |
|---|---:|---:|
| células com N ≥ 50 | 54 | 82 |
| PASSA | 0 | **3** |
| REPROVA | 4 | **29** |

As três que passaram: `Any Other Home Win` fav>1,80 (N=3.386, ROI +1,99%), `1-3` fav≤1,40 (N=199,
+4,05%), `Any Other Away Win` fav>1,80 (N=3.102, +1,30%). Pelo próprio pré-registro, **nenhuma vira
método** — só célula que passa nos 3 snapshots vira candidata. Falta o snapshot 3, em **12/11**.

**Grade Over in-play** (`varredura_over_inplay.py`): **0 células PASSA**, ROI total **−10,7%** em 58.842
apostas. Degradação monotônica: Over 0.5 −0,7%, Over 1.5 −2,8%, Over 2.5 −8,8%, Over 3.5 **−25,8%**.

**H-extra**: N=963 (só KO ≥ 13/09), WR 74,0% vs BE 75,6%, ROI **−2,2%**, IC95 [−7,5%, +3,0%].
No snapshot 1 as cinco janelas pareciam positivas; com dados novos o efeito sumiu. O pré-registro fez
o trabalho dele.

**Não rodado:** `PREREGISTRO_late_goal_v2.md`, que também apontava snapshot em 12/10.

**Ressalva de cobertura:** o coletor perdeu 13 h em 07/10 e ~12% dos ciclos entre 07 e 12/10. Jogos com
KO nessas janelas ficaram fora por falta de captura.

---

## 5. Pré-registro novo — Lay 0x3

`PREREGISTRO_lay0x3_2026-10-13.md`, commit `f819acc`. Regra congelada: governança + Under 2.5 ≤ 2,10 +
visitante ≥ 1,85 + **lay entre 14 e 30** (teto 30, não 35) + liquidez ≥ R$ 200. Sem Top 3.

Medido em 2026 na base da API: **+3,82%, IC95 [+3,39%, +4,19%], N=1.650**. A odd transfere da base para
a execução (correlação base × KO−10 de 0,643, razão mediana 1,000, 90% dos jogos continuam na janela).

Registrado antes de congelar: o edge decai dentro de 2026 (1ª metade +3,38% em 1.825 jogos; 2ª metade
+1,14% em 259, IC incluindo zero) e o break-even é 95,7%.

**O método segue desativado no robô desde 03/10.** Para o teste existir, alguém precisa reativá-lo com
a regra acima. N-alvo 600, stake zero/micro, parada em −3% após 150 entradas.

---

## 6. Pendências

1. Commitar o README mascarado **e apagar** `README.md.bak_pre_mascara` (tem credencial).
2. Revogar a credencial antiga no iProyal quando o painel permitir, ou manter o pool principal em zero.
3. Testar o VPS como proxy quando o tráfego acabar.
4. Snapshot 3 do CS em 12/11.
5. Late Goal v2 nunca foi rodado.
6. Decidir se o Lay 0x3 volta ao robô com a regra pré-registrada.
