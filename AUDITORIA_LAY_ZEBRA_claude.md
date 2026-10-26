# Auditoria — proposta Lay Zebra em substituição ao Lay Draw

**Data:** 23/10/2026. Auditor: Claude. Proposta: Thiago. Estudo empírico: Antigravity.
**Veredito: NÃO APROVADO para o forward oficial.** A proposta tem um mérito estrutural real, mas o
resultado positivo vem inteiro de 2024-2025 e o teto 10.00 é um pico de serra, não um limiar.

---

## 0. O mérito que o estudo não menciona (e que é o ponto mais forte da hipótese)

A proposta está estruturalmente certa num ponto importante: **`Lay Home/DC X2 (FavVis<=1.65)` já é
Lay Zebra.** É laydar o mandante quando o favorito é o visitante — ou seja, Dupla Chance do super
favorito. E é o **único método positivo do nosso forward**: +7,90u em N=157, WR 90,4% contra
break-even 86,2%, gap **+4,3pp**.

Então a versão honesta da hipótese não é "trocar Lay Draw por Lay Zebra". É:

> *"O lado zebra-com-favorito-visitante funciona e está no robô. Estender para o lado
> zebra-com-favorito-mandante (Lay Away quando Odd_H <= 1,40)."*

Esse é um argumento muito melhor que o backtest, porque parte de evidência forward nossa e não de
base histórica. **Foi exatamente isso que eu isolei e medi.** O resultado está na seção 3.

---

## 1. Conferência da matemática do relatório

A aritmética do Antigravity está correta. O que precisa de tradução é a **convenção de unidade**.

| célula do relatório | N | gap declarado | erro-padrão | z | reds |
|---|---:|---:|---:|---:|---:|
| Lay Zebra sem teto | 1.327 | +0,53pp | 0,76pp | **+0,70** | 103 |
| Lay Zebra teto<=10 | 340 | +2,64pp | 1,65pp | **+1,60** | 26 |
| Lay Draw atual | 1.327 | −0,81pp | 1,00pp | −0,81 | 220 |

**Nenhuma das duas células de Lay Zebra atinge significância.** z=0,70 é ruído puro; z=1,60 dá
p≈0,11 bilateral — e isso **antes** de corrigir pela correlação intra-dia e **antes** de penalizar o
fato de o teto ter sido escolhido olhando o resultado. Com bootstrap de blocos-dia o intervalo só
alarga.

**Tradução de unidade (importante):** o `+85,30u` do relatório está em convenção de *stake 1u*. Na
nossa convenção de portfólio — *liability 1u*, que é a que o ledger usa — a mesma coisa vale
**+9,93u**, não +85,30u. O ROI percentual (+2,92% recalculado, contra +3,02% declarado) é
comparável; **as unidades não são.** Comparar `−74,65u` do Lay Draw com `+85,30u` do Lay Zebra é
comparar réguas diferentes: o Lay Draw opera com liability ~5x e o Lay Zebra com liability ~8,3x.

---

## 2. Reprodução independente na FRESH3 com as regras de produção

Universo: regra congelada do `sinais_ko_core.M_DRAW` — `min(H,A) <= 1,40`, `4,50 <= Odd_D_Lay <= 7,00`,
spread <= 1,15, governança `eh_jogo_ignorado`, regra de copa (só favorito mandante). Zebra = lay no
não-favorito, mesma trava de spread. **977 jogos** (2024: 286, 2025: 347, 2026: 344).

### Os dois métodos, nos mesmos jogos

| | N | WR | BE | gap | ROI/liability | IC95 bloco-dia | reds |
|---|---:|---:|---:|---:|---:|---|---:|
| Lay Draw (empate) | 977 | 82,29% | 84,28% | −1,99pp | **−2,34%** | [−5,28%, +0,55%] | 173 |
| Lay Zebra (sem teto) | 977 | 92,12% | 91,38% | +0,74pp | **+0,82%** | **[−1,01%, +2,53%]** | 77 |

Direção reproduzida: o Lay Zebra é melhor que o Lay Draw. Mas **o IC95 inclui zero.**

### Ano a ano — e aqui o estudo quebra

| ano | Lay Draw ROI | Lay Zebra ROI | IC95 do Zebra |
|---|---:|---:|---|
| 2024 | −1,73% | +2,38% | [−0,86%, +5,35%] |
| 2025 | −3,85% | +1,90% | [−1,16%, +4,69%] |
| **2026** | −1,32% | **−1,57%** | [−4,99%, +1,50%] |

**Nenhum ano tem IC excluindo zero, e o único ano que conta é negativo.** A regra em vigor neste
projeto é explícita: *"agora não adianta pegar do ano passado, porque as odds mudaram, o sistema é
outro, não vale a pena."* Por esse critério, o resultado da proposta é **−1,57%**, não +0,51%.

### O teto 10.00 é um pico de serra

| teto | N (todos) | ROI todos | IC95 | ROI **2026** | reds 2026 |
|---|---:|---:|---|---:|---:|
| <= 8 | 5 | +14,08% | — | +13,57% | **0** ← inutilizável |
| <= 9 | 104 | **−0,60%** | [−7,54%, +5,93%] | −0,94% | 4 |
| **<= 10** | 309 | **+2,46%** | [−0,76%, +5,61%] | **−1,24%** | 11 |
| <= 11 | 531 | +1,17% | [−1,39%, +3,58%] | −1,37% | 19 |
| <= 12 | 689 | +0,73% | [−1,54%, +2,89%] | −2,43% | 26 |
| <= 14 | 877 | +0,91% | [−1,09%, +2,78%] | −1,35% | 30 |
| sem teto | 977 | +0,82% | [−1,01%, +2,53%] | −1,57% | 34 |

Duas coisas matam o teto:

1. **O vizinho de baixo é negativo.** `<=9` dá −0,60% e `<=10` dá +2,46%. Um limiar real de gestão de
   risco produz comportamento monótono ou um platô — *quanto menor o liability, melhor*. Serra com
   vizinho negativo é assinatura de parâmetro ajustado a ruído.
2. **O teto não salva 2026.** Na minha reprodução, `teto<=10` em 2026 dá **−1,24% com 11 reds**.
   A afirmação *"verde em todos os anos"* **não reproduz**. Mesmo com o teto, 2026 é vermelho.

*(Minha contagem é 977 jogos contra 1.327 do relatório — diferença provável da trava de spread que
apliquei também no lado da zebra, e/ou de variante de governança. A direção e os sinais reproduzem;
as células não são idênticas.)*

---

## 3. O que é realmente novo, medido isoladamente

Separando o universo pelo lado do favorito, com `teto<=10`:

| célula | N | WR | BE | gap | ROI | IC95 | reds |
|---|---:|---:|---:|---:|---:|---|---:|
| Lay Away (fav MANDANTE) — **o que é novo** | 238 | 91,60% | 89,73% | +1,87pp | +2,08% | [−1,82%, +5,68%] | 20 |
| Lay Home (fav VISITANTE) — **já está no robô** | 71 | 92,96% | 89,57% | +3,39pp | +3,77% | [−3,66%, +9,96%] | 5 |
| **só 2026: Lay Away (novo)** | **74** | 89,19% | 89,68% | **−0,49pp** | **−0,55%** | **[−8,32%, +6,64%]** | 8 |
| só 2026: Lay Home (já existe) | 22 | 86,36% | 89,59% | −3,23pp | −3,57% | [−20,39%, +11,58%] | 3 ⚠ |

**A metade nova, no único ano que conta: ROI −0,55%, IC de 15 pontos de largura, N=74.** Não há nada
ali. E note que a metade que *já está no robô e dá +4,3pp no nosso forward* aparece **negativa** na
base em 2026 (N=22, 3 reds — abaixo da regra de guarda, não conclui nada). Ou seja: nesta fatia a
base não consegue nem reproduzir o método que está comprovadamente funcionando. Isso é um aviso sobre
o poder de resolução da base nesse universo, não um aval para a proposta.

### 2026 partido em duas metades (teto<=10)

| | N | WR | BE | ROI | IC95 | reds |
|---|---:|---:|---:|---:|---|---:|
| 1ª metade (até 23/04) | 48 | 83,33% | 89,70% | **−7,12%** | [−17,76%, +2,96%] | 8 |
| 2ª metade (de 25/04) | 48 | 93,75% | 89,61% | **+4,63%** | [−4,01%, +11,58%] | 3 ⚠ |

Inverte de sinal no meio do ano e a metade "boa" tem 3 reds — reprovada pela regra de guarda.

---

## 4. Resposta à pergunta 2 — o comportamento nos 291 jogos do forward

**Não é calculável com o ledger que temos, e isso não é detalhe.** O
`metodos_aprovados/forward_ko_ledger.csv` tem 24 colunas e **nenhuma delas é a odd de lay da zebra**,
nem o lado do favorito:

```
Data, Metodo, Liga, Home, Away, Hora, Odd_Lay (= lay do EMPATE), Odd_Fav, liq_lay,
min_to_ko, ts_captura, origem, status, gols_H, gols_A, placar, resultado, pnl_u,
pnl_rs, break_even, liquidado_em, fonte_placar, universo, no_0600
```

Sem a odd da zebra não existe green (o green é `(1−c)/(odd−1)`, depende da odd) e sem o lado do
favorito não se sabe qual resultado é red. O desfecho dos 222 jogos com placar legível é:

```
empates 46 (20,7%)   |   casa venceu 120 (54,1%)   |   fora venceu 56 (25,2%)
```

Esses 25,2% de vitórias do visitante **não se decompõem** em "zebra venceu" sem saber, jogo a jogo,
se o favorito era o mandante.

E há **dois erros de contabilidade** na formulação da hipótese que precisam ser corrigidos antes de
qualquer estimativa:

1. **`+0.95u` por empate está na convenção errada.** Na nossa convenção de liability, um green a odd
   9,30 paga `0,95/8,30 = +0,115u`, não +0,95u. Os 54 empates valeriam **+6,2u**, não +51,3u.
2. **Os reds não foram subtraídos.** A hipótese lista os greens (empate, vitória do favorito) e trata
   a vitória da zebra como "o único red" — mas cada um custa **−1u inteiro**. A odd 9,30 exige WR de
   **89,71%**: em 291 jogos o método suporta no máximo ~30 vitórias da zebra antes de zerar. O
   forward teve 56 vitórias do visitante. Se boa parte desses jogos tinha favorito mandante, o
   método já estaria no vermelho.

O ganho do Lay Zebra nunca foi "o empate virou amigo de graça". O mercado cobra por isso: o
break-even sai de 85,8% (lay empate) para ~89,7–91,7% (lay zebra). **A estrutura está precificada.**
Já medimos esse mecanismo antes neste projeto — *"espelho Back Over não cria edge: os dois lados do
mesmo mercado perdem ~4,6%; a perda é o overround."*

---

## 5. O problema operacional que ninguém mediu ainda

Mesmo que a estatística tivesse passado, há uma trava dura: **a odd 9,30 significa liability de 8,3x
o stake.**

- O `LIQ_MIN = 200,0` do robô cobre **R$ 24 de stake** a essa odd.
- No nosso forward, a liquidez mediana no lay do empate foi **R$ 230** (p10 = R$ 85, p90 = R$ 1.706).
- Não existe nenhuma medição da profundidade do livro **na zebra** a 9,30 no KO−10.

Esse é o filtro que decidiu Lay Draw e Lay Home — *"o piso de liquidez, não o spread, é o filtro que
funciona"*. Enquanto não houver essa medição, o estudo é sobre preço de base, não sobre preço
executável. E o nosso próprio precedente é que isso importa: a tríade 1X2 parecia +4,31% e virou
+0,14% com preço real.

---

## 6. Respostas diretas

**1. Concorda com a transição?**
**Não.** Não há uma única célula, em nenhum corte, com IC95 excluindo zero. O resultado positivo é
2024-2025, que por regra do projeto não conta. Em 2026 a proposta é negativa com teto e sem teto. E o
teto 10.00 tem vizinho negativo em 9 — é ajuste a ruído, não gestão de risco.

**2. Comportamento nos 291 jogos do forward?**
**Não mensurável.** O ledger não grava a odd de lay da zebra nem o lado do favorito. A estimativa de
"+0,95u por empate" está na convenção de stake (o valor correto é +0,115u) e não desconta as
vitórias da zebra a −1u. Qualquer número aqui seria inventado.

**3. Recomendação operacional para a VPS?**
**Nem forward oficial, nem watchlist ainda.** A sequência correta é:

| ordem | ação | por quê |
|---|---|---|
| 1 | **Matar o Lay Draw** — decisão independente e já madura | −14,65u, 4,4pp abaixo do BE em 291 jogos forward, negativo em 5 das 6 semanas, fluxo caiu de 95 para 4/semana. A base 2026 concorda: −1,32%. |
| 2 | **Logar a odd e a liquidez da zebra** no coletor, stake zero, sem método | É o dado que falta. Sem ele não há estudo possível, só backtest de base. |
| 3 | Só então decidir sobre watchlist | Com odd executável medida, o teste passa a existir. |

O passo 1 e o passo 2 são independentes: dá para desligar o Lay Draw hoje e começar a gravar a zebra
hoje, sem nenhum método novo entrar no ar.

**O que eu não recomendo:** aproveitar o fato de `Lay Home/DC X2` ser a mesma estrutura para mandar o
lado novo direto ao forward "por analogia". Testei exatamente esse argumento — Lay Away com favorito
mandante, 2026, teto 10: **−0,55% em 74 jogos.** A analogia é boa como hipótese e não sobrevive à
medição.

---

## 7. O que mudaria o meu voto

- Odd de lay da zebra **medida no KO−10 pelo coletor**, com correlação base × execução (o teste que
  aprovou o Lay 0x3 e reprovou o filtro de spread: 0,643 contra 0,001).
- Profundidade do livro na zebra suficiente para liability de 8–9x acima de um piso definido antes.
- ROI por liability com IC95 **inteiramente acima de zero em 2026**, em teto escolhido antes de ver
  o resultado — e com o vizinho de teto não-negativo.

Se os três acontecerem, isso vira pré-registro e eu assino. Hoje não.

---

*Reprodução: `auditoria_lay_zebra.py`, FRESH3 + `sinais_ko_core`, bootstrap de blocos-dia 4.000
reamostragens, seed 7, comissão 5%.*
