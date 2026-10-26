# Auditoria — filtros de produção do Lay Draw e Stop Red no portfólio limpo

**Data:** 23/10/2026. Auditor: Claude. Autópsia e simulação: Antigravity. Questionamentos: Thiago.

**Veredito em duas partes:**
1. **A contaminação da base é real e eu estava errado.** O `−14,65u` que eu reportei em 23/10 media
   o ledger, não o método. Reproduzi a reclassificação quase exatamente. A Lei 2 foi violada — por mim.
2. **O Stop Red não sobrevive.** Reprova em três testes independentes e, além disso, **não é
   implementável** com o pipeline de liquidação atual.

---

## 1. Validação da contaminação — CONFIRMADO

Reproduzi a reclassificação aplicando as regras exatas de `sinais_ko_core.avaliar` (confirmei no
código que o `M_DRAW` realmente exige `liq_draw_ok = dls >= 200` e `spread_draw_ok <= 1,15`, além do
teto 4,50–7,00, do `fav <= 1,40` e da governança).

| conjunto | N | G | R | WR | BE | gap | P&L | meu número | Antigravity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Fora dos filtros | 211 | 170 | 41 | 80,6% | 86,3% | −5,8pp | **−14,00u** | ✓ | −14,00u |
| **Dentro dos filtros** | **81** | 68 | 13 | 84,0% | 84,5% | −0,5pp | **−0,49u** | ✓ | −0,49u |
| Total bruto | 292 | 238 | 54 | 81,5% | 85,8% | −4,3pp | −14,48u | ✓ | −14,48u |

Motivos de exclusão (com sobreposição): governança 131, teto 108, liquidez 134. **Bate linha por
linha com o relatório do Antigravity.**

**Respondendo à pergunta 1: sim, concordo, e o erro foi meu.** Eu medi o ledger inteiro como se fosse
o método, quando 72% das linhas eram de jogos que o robô de hoje nunca geraria — Butão, Ruanda,
futebol feminino, odds de lay 8 a 12, livros sem R$ 200. O número `−14,65u` descreve o histórico do
arquivo, não a regra em produção. Isso é exatamente a Lei 2 ("backtest e live têm que ser o mesmo
objeto") e eu a violei. Retiro a frase "o Lay Draw é o vilão com −14,65u".

---

## 2. Mas `−0,49u em 81 jogos` não significa "quase no break-even". Significa **não medido**.

| | valor |
|---|---|
| N | 81 |
| WR observado | 83,95% |
| break-even | 84,50% |
| gap | −0,55pp |
| erro-padrão do WR | **4,02pp** |
| **z** | **−0,14** |
| IC95 do WR | **de 76,1% a 91,8%** |
| IC95 do ROI (bootstrap bloco-dia) | **[−7,9%, +7,4%]** |

Com 81 jogos e 13 reds, o intervalo de confiança não distingue um método de **+4%** de um método de
**−4%**. O `−0,49u` não é evidência de que o Lay Draw limpo está perto do justo — é evidência de que
**ainda não há evidência**. A amostra limpa é pequena porque os filtros são novos.

Isso muda o diagnóstico de *"método reprovado"* para *"método não medido"*. É uma correção
importante, e é diferente de absolvição.

---

## 3. O teste que o relatório não fez: os filtros são post-hoc?

As travas entraram em produção em **08/10 e 13/10** — ou seja, **depois** de os reds de setembro
estarem visíveis. Então a pergunta obrigatória é: a regra limpa foi escolhida olhando esses reds?
O corte é direto — separar a amostra limpa pela data de deploy:

| fatia | N | WR | BE | gap | ROI | IC95 |
|---|---:|---:|---:|---:|---:|---|
| limpo, KO **< 08/10** (dado em que o filtro foi escolhido) | 37 | 89,2% | 84,5% | **+4,7pp** | **+5,54%** | [−4,9%, +14,5%] ⚠ 4 reds |
| limpo, KO **>= 08/10** (out-of-sample) | 44 | 79,5% | 84,5% | **−4,9pp** | **−5,76%** | [−15,3%, +6,5%] |
| limpo, KO **< 13/10** | 61 | 85,2% | 84,4% | +0,9pp | +1,04% | [−5,6%, +9,5%] |
| limpo, KO **>= 13/10** (out-of-sample) | 20 | 80,0% | 84,9% | −4,9pp | −5,61% | [−26,0%, +18,9%] |

**Todo o lado positivo da amostra limpa está antes do deploy dos filtros.** Depois do deploy, a
amostra limpa é negativa nas duas datas de corte. É a assinatura de filtro ajustado aos reds — a
mesma coisa que reprovou a blacklist NL1/SE1 (permutação P=0,49).

**Evidência independente que fecha o quadro:** na auditoria de hoje do Lay Zebra eu rodei a regra
congelada do `M_DRAW` (fav ≤ 1,40 + teto 4,50–7,00 + spread, com governança) na base FRESH3, só 2026,
em dado que nenhum filtro nosso jamais viu: **−1,32% em 344 jogos**. Negativo, com N quatro vezes
maior que a amostra limpa do forward.

Nenhum desses cortes tem IC excluindo zero. A leitura honesta: **a regra limpa não está provada morta
nem viva, e toda a evidência fora da amostra de ajuste é negativa.**

---

## 4. O Stop Red — reprova em três testes

### A. Resposta a dose não é monótona

Se reds se agrupam dentro do dia, parar depois de 1 red ajuda, parar depois de 2 ajuda menos,
e nunca piora em relação a não parar. Observado:

| | Lay Draw limpo (N=81) | Portfólio limpo (N=185) |
|---|---:|---:|
| sem stop | −0,49u | +4,29u |
| stop 1 red | **+0,18u** (+0,67u) | **+6,02u** (+1,73u) |
| stop 2 reds | **−1,56u** (−1,07u) | **+4,11u** (−0,18u) |
| stop 3 reds | −0,86u (−0,38u) | +4,59u (+0,30u) |

**Nos dois casos o stop de 2 reds é PIOR que não fazer nada.** Não existe mecanismo que produza isso.
É a mesma assinatura de serra do teto 10.00 na proposta do Lay Zebra: um pico isolado entre vizinhos
que vão para o outro lado.

### B. Teste de permutação — P = 0,32

H0: mesmos jogos, mesmas odds, mesmo número de reds, posições sorteadas (destrói o agrupamento
intra-dia e preserva todo o resto).

| | ganho observado do stop | mediana sob H0 | IC90 sob H0 | **P(ganho ≥ observado)** |
|---|---:|---:|---|---:|
| Lay Draw limpo | +0,67u | −0,08u | [−2,60u, +2,53u] | **0,323** |
| Portfólio limpo | +1,73u | −1,55u | [−5,34u, +2,25u] | **0,076** |

O ganho observado cai no meio da distribuição do azar. **Não há agrupamento de reds além do
esperado.** E vale lembrar: `scratch/backtest_stop_red_green.py` varre **13 configurações** de stop —
isso é grade, e pela nossa própria regra ("FDR só onde houve varredura") o P de 0,32 ainda seria
penalizado por testes múltiplos. Ele já não passa sem penalidade nenhuma.

### C. Deixa-um-dia-de-fora — o efeito é um único dia

| | ganho do stop no Lay Draw |
|---|---:|
| com todos os dias | +0,67u |
| **sem 10/10** (o dia dos 3 reds agrupados) | **−0,95u** |
| sem 14/10 (2 reds) | −0,33u |
| sem qualquer dia sem red | +0,67u (inalterado) |

O relatório identificou o dia corretamente — Ajax, Vasco, Peñarol, Junior Barranquilla — mas tirou a
conclusão invertida. **Aquele dia não é a prova do mecanismo; aquele dia é o mecanismo inteiro.**
Removido, o stop passa a destruir valor, porque nos outros dias ele só joga fora os 13 greens.

---

## 5. Implementabilidade — objeção RETIRADA (correção de 23/10, após esclarecimento do Thiago)

**Eu havia escrito que o stop não era implementável.** O argumento era a latência de liquidação do
ledger: nas 799 linhas liquidadas o mínimo é 135 min, só 1,1% em ≤180 min, **zero em ≤115 min**.

**Esse argumento está errado, porque o robô não aposta — o Thiago aposta.** Quem opera é uma pessoa,
que sabe o resultado quando o jogo termina (~KO+105min) e simplesmente deixa de entrar nos alertas
seguintes. Não há nada a implementar: a latência de 135 min é do *pipeline de liquidação do ledger*,
não do operador. A janela de KO+115min da simulação é realista para operação manual.

A latência do ledger segue sendo relevante para uma coisa só: um disjuntor **automático** dentro do
`sinais_ko_vps.py` continuaria inviável sem placar ao vivo no processo. Para o fluxo manual, é
irrelevante.

### O que sobra depois de retirar essa objeção

Os outros dois testes não dependem de implementabilidade — são sobre o stop **criar valor**. Rodando
o stop isolado por método, como o operador faria, com permutação em cada um:

| método | N | gap vs BE | sem stop | com stop | delta | pulou | **P permutação** |
|---|---:|---:|---:|---:|---:|---:|---:|
| Lay 0x3 (Regra Ampla) | 23 | +3,9pp | +0,93u | +0,93u | +0,00u | 0G/0R | 1,000 |
| Lay 0x3 Top 3 | 18 | +4,0pp | +0,76u | +0,76u | +0,00u | 0G/0R | 1,000 |
| Lay Draw | 81 | −0,5pp | −0,49u | +0,18u | +0,67u | 13G/3R | **0,328** |
| Lay Home/DC X2 | 58 | **+5,7pp** | +3,88u | +4,27u | +0,39u | 4G/1R | **0,144** |
| Lay Over 4.5 | 5 | −15,1pp | −0,79u | −0,79u | +0,00u | 0G/0R | 0,796 |

**Em nenhum método o stop atinge significância.** Nos dois em que ele chega a disparar, P=0,33 e
P=0,14. Nos outros três ele nunca dispara (0 reds), então a questão não existe.

### A assimetria que decide a regra prática

O custo do stop não é o mesmo em todo método — depende do EV da entrada que se deixa de fazer:

- **Lay Home tem EV medido de +6,69% da liability por entrada.** O stop pularia 5 das 58 entradas,
  o que custa **~+0,33u em expectativa**. Neste histórico deu +0,39u porque 1 dos 5 pulados era red
  — sorte, não mecanismo (P=0,14).
- **Lay Draw tem EV não medido (gap −0,5pp, z=−0,14).** Pular entradas ali **não custa nada** —
  e também não ganha nada.

Daí sai a única recomendação defensável sobre o stop manual:

| método | parar depois de um red? | por quê |
|---|---|---|
| **Lay Home/DC X2** | **não** | EV comprovado +5,7pp. Cada entrada pulada joga fora lucro esperado |
| **Lay Draw (regra limpa)** | **se você quiser** | EV zero: o stop é de graça. É decisão sobre disciplina, não sobre o mercado |
| 0x3, Over 4.5 | irrelevante | nunca dispara (0 a 1 red na amostra) |

"De graça" é o ponto importante: parar o Lay Draw depois de um red **não** é uma estratégia que ganha
dinheiro, mas também não tira nada — e se ajuda a não operar irritado, o custo estatístico é zero.
Isso é uma regra sobre o operador, e para esse tipo de regra não é preciso p-valor nenhum.

---

## 6. O que do relatório eu aceito integralmente

**O ponto 3B está certo e é um achado de engenharia válido, independente de tudo o resto.** Um stop
**global** teria cancelado 90 jogos do Lay Home (81 greens / 9 reds) e jogado fora ~+4u — o lucro do
único método que funciona. Se algum disjuntor existir algum dia, **tem que ser isolado por método.**
Isso vale como princípio de arquitetura e não depende de o stop gerar edge.

---

## 7. Respostas diretas

**1. Avaliar o Lay Draw pelos 292 jogos brutos violava a Lei 2?**
**Sim, e o erro foi meu.** 211 das 292 linhas são de jogos que a regra atual não geraria. Retiro
o `−14,65u` como número do método. O número do método é `−0,49u em 81 jogos`.

**2. Mantenho a recomendação de desligar?**
**Não. Mudo a recomendação.** Mas não pelo motivo do relatório.

- Desligar sumariamente estava errado porque eu estava medindo a coisa errada.
- Manter com o disjuntor de Stop 1 Red também está errado: P=0,32, dose não-monótona, efeito
  concentrado em um dia, e não implementável.
- **A recomendação correta é a terceira opção: manter ativo na regra limpa, stake zero, com
  pré-registro e N-alvo, e SEM stop.** O método está *não medido*, não aprovado. 81 jogos com IC de
  16 pontos de largura não autorizam dinheiro real; autorizam continuar medindo.

Sugestão de pré-registro, a congelar antes de olhar mais nada: regra exatamente a de
`sinais_ko_core.M_DRAW` sem nenhuma alteração; N-alvo 250 entradas **com KO a partir de 24/10/2026**
(todo o histórico anterior é amostra de ajuste dos filtros); métrica ROI por liability; sucesso =
IC95 inteiramente acima de zero; parada por perda em −5% após 100 entradas; parada por inviabilidade
em menos de 15 sinais/mês por 2 meses. No ritmo atual (~20 sinais limpos/mês) isso leva ~12 meses —
e esse prazo é, por si, informação sobre se o método vale a infraestrutura.

**3. Incorporar o disjuntor em `sinais_ko_vps.py` / `stop_diario.py`?**
**Não precisa existir em código — e como mecanismo de lucro, não existe o efeito.**

Como quem aposta é o Thiago e não o robô, o "disjuntor" é só ele não entrar no alerta seguinte.
Não há componente a construir, e a VPS deve continuar **mandando todos os alertas** — quem filtra é
o operador. Codificar o stop no robô só esconderia sinais que a gente precisa continuar medindo
(o N-alvo do pré-registro depende de o alerta aparecer, mesmo quando não é apostado).

Regra prática, por método, conforme a seção 5:

| método | parar depois de um red? | custo |
|---|---|---|
| Lay Home/DC X2 | **não** | cada entrada pulada custa ~+0,067u de EV |
| Lay Draw limpo | opcional | zero — EV não medido |
| 0x3 / Over 4.5 | irrelevante | nunca dispara |

Se algum dia for automatizado para **controle de drawdown** (justificativa de banca, não de lucro),
três pré-condições:
1. **Detecção de placar ao vivo no processo** — a latência do ledger é de 135 min no mínimo, então
   um disjuntor automático hoje dispararia depois de o dia acabar. (Para o fluxo manual isso não se
   aplica: ver seção 5.)
2. **Isolado por método**, nunca global (ponto 3B do relatório).
3. **Limite em % de banca**, não em contagem de reds — contagem de reds trata um red de odd 25 igual
   a um de odd 5, quando a liability é a mesma mas o green não é.

**E o registro tem que separar as duas coisas:** se você parar de apostar mas o alerta existir, o
ledger precisa marcar a entrada como *sinal não apostado*, não como ausente. Senão a amostra do
pré-registro passa a ter um filtro seu dentro dela e vira outra Lei 2.

---

## 8. Correções que este laudo faz no meu relatório de 23/10

| eu disse | correto |
|---|---|
| "Lay Draw é o vilão: −14,65u em 291 jogos" | −14,48u é o ledger bruto; o método em produção é −0,49u em 81 jogos |
| "4,4pp abaixo do break-even" | −0,5pp na amostra limpa; os −4,3pp vinham dos 211 jogos fora da regra |
| "negativo em 5 das 6 semanas" | medido na amostra contaminada; não re-verificado na limpa |
| "matar o Lay Draw — decisão madura" | **retirado.** Método não medido, mantém-se em stake zero com pré-registro |
| "a base 2026 concorda: −1,32%" | **isso se mantém** — N=344, regra limpa, dado independente |

O que **não** muda: o Lay Home/DC X2 segue sendo o único método com gap positivo, e o portfólio limpo
(+4,29u na minha contagem, +3,36u na do Antigravity) é carregado por ele.

---

*Reprodução: `auditoria_stop_red.py` — ledger + `sinais_ko_core`, bootstrap de blocos-dia 4.000
reamostragens, permutação 3.000 sorteios, stop cronológico com liquidação em KO+115min, comissão 5%.*
