# Auditoria — Fluxo Semi-Automático (Scanner Bet365 ➔ validação visual ➔ execução manual no OrbitX)

**Veredito: não aprovo para dinheiro real.** A trava manual é a melhor ideia do desenho e eu a mantenho
— mas ela não cria edge, e a régua de corte proposta não existe fora dos 118 jogos em que foi escolhida.

---

## 1. A afirmação central foi testada e não se confirma

O estudo diz: quando o empate na Bet365 passa de 5,00, o spread na exchange cai para 1,08–1,14x.
Isso é verificável na base Betfair da própria API, que traz `Odd_D_Lay` real. Nos jogos com
Super Fav ≤ 1,40 — **5.856 jogos**, não 118:

| faixa do empate na Bet365 | estudo (118 jogos) | **medido (5.856 jogos)** | n | p75 |
|---|---:|---:|---:|---:|
| < 4,50 | 1,42x | **1,244x** | 1.299 | 1,415 |
| 4,50 – 5,50 | 1,25x | **1,200x** | 2.401 | 1,347 |
| 5,50 – 6,50 | 1,14x | **1,200x** | 1.057 | 1,367 |
| 6,50 – 8,00 | 1,08x | **1,225x** | 578 | 1,467 |
| > 8,00 | 1,00x | **1,368x** | 521 | 1,917 |

O spread **não encolhe**: fica praticamente plano em 1,20–1,24x e **piora** acima de 8,00.
A célula "> 8,00 → 1,00x (idêntico)" veio de **5 jogos**; a "6,50–8,00 → 1,08x" veio de **15**.
A Zona de Ouro inteira se apoia em 37 + 15 + 5 observações.

Vale notar que as duas tabelas não se contradizem por acaso: quando se agrupa pela odd da Bet365 e se
divide pela própria odd da Bet365, as faixas baixas herdam o ruído da variável de agrupamento e exibem
razões altas. É um artefato conhecido, e some quando a amostra cresce — foi o que aconteceu aqui.

## 2. A regra completa, com a trava manual, em 2.020 jogos reais

Filtro do radar: Super Fav ≤ 1,40 **e** empate b365 ≥ 5,00 (3.811 jogos, 65% dos Super Fav).
Execução: só entra se a odd de lay **real** estiver dentro do teto. Liability 1u, IC95 por bootstrap
de blocos-dia (2.000 reamostragens).

| trava manual no OrbitX | N | % que entra | WR | break-even | ROI c=5% | ROI c=14% | IC95 (c=5%) |
|---|---:|---:|---:|---:|---:|---:|---|
| **lay ≤ 7,50 (a proposta)** | 2.020 | 53,0% | 83,37% | 84,34% | **−1,14%** | −2,60% | [−3,09%, +0,64%] |
| lay ≤ 8,00 | 2.361 | 62,0% | 83,95% | 84,83% | −1,03% | −2,45% | [−2,68%, +0,58%] |
| lay ≤ 6,50 | 1.301 | 34,1% | 82,63% | 83,29% | −0,78% | −2,35% | [−3,32%, +1,68%] |
| sem trava | 3.811 | 100% | 86,33% | 87,97% | −1,76% | −2,89% | [−2,95%, −0,60%] |

**A trava funciona — e não basta.** Ela melhora o resultado de −1,76% para −1,14%, exatamente como
esperado de um filtro que descarta preço ruim. Mas em todas as variantes o **WR fica abaixo do
break-even**. O empate acontece com mais frequência do que o preço de lay permite lucrar.

### O corte em 5,00 não sobrevive a uma varredura

O 5,00 foi escolhido olhando os mesmos 118 jogos que serviram de prova. Varrendo o corte:

| corte na b365 | N | WR | break-even | ROI c=5% | IC95 |
|---|---:|---:|---:|---:|---|
| ≥ 4,00 | 3.618 | 82,42% | 83,40% | −1,14% | [−2,61%, +0,30%] |
| ≥ 4,50 | 3.013 | 82,74% | 83,79% | −1,24% | [−2,87%, +0,27%] |
| **≥ 5,00** | 2.020 | 83,37% | 84,34% | **−1,14%** | [−2,94%, +0,62%] |
| ≥ 5,50 | 1.010 | 83,96% | 85,08% | −1,31% | [−3,87%, +1,08%] |
| ≥ 6,00 | 439 | 85,88% | 85,60% | +0,35% | [−3,23%, +3,93%] |
| ≥ 6,50 | 166 | 86,75% | 86,02% | +0,85% | [−5,00%, +6,29%] |

Não há patamar: o ROI oscila em torno de −1,2% e só vira positivo onde a amostra encolhe para 439 e 166
jogos, com IC de ±4 a ±6 pontos. Isso é ruído, não zona de ouro.

## 3. O ponto mais sério: o forward foi gasto duas vezes

Os +6,17u e os 90,68% de WR vieram da **regra congelada** do Lay Draw — `liga_draw_rate < 0,23`,
pré-registrada justamente para não ser reajustada. A proposta agora usa **os mesmos 118 jogos** para
escolher um parâmetro novo (empate b365 ≥ 5,00) e depois apresenta o desempenho daqueles jogos como
evidência a favor dele.

Isso destrói o que o forward tinha de valioso. Um forward só vale como prova enquanto nada é ajustado
olhando para ele; no momento em que um corte é escolhido ali dentro, aquele resultado vira amostra de
treino, não de teste.

E os 90,68% não exigem explicação especial: com N = 118 e WR verdadeiro de 85%, o erro-padrão é 3,3
pontos — 90,68% está a 1,7 desvios, dentro do esperado por sorte.

**Pior: testei a própria regra congelada com odd de lay real.** Reconstruí `liga_draw_rate` as-of
(média expandida dos jogos anteriores da liga, mínimo 200), em 50.178 jogos:

| regra | N | WR | break-even | ROI c=5% | IC95 |
|---|---:|---:|---:|---:|---|
| **congelada: liga_draw_rate < 0,23** | 6.898 | 77,73% | 80,77% | **−3,49%** | **[−4,69%, −2,35%]** |
| congelada + Super Fav ≤ 1,40 | 1.524 | 87,07% | 88,21% | −1,10% | [−3,13%, +0,74%] |
| proposta (b365 ≥ 5,00 + lay ≤ 7,50) | 2.015 | 83,42% | 84,34% | −1,07% | [−2,92%, +0,84%] |
| as duas juntas | 493 | 83,98% | 84,44% | −0,59% | [−4,18%, +3,00%] |

A regra congelada, avaliada no preço real de lay, dá **−3,49% com IC inteiramente abaixo de zero**.

**Ressalva honesta:** minha reconstrução de `liga_draw_rate` pode não ser idêntica à de produção
(janela, mínimo de jogos, agrupamento de liga). Antes de declarar o Lay Draw morto, isso precisa ser
refeito com a definição exata do código de produção. Mas o sinal é forte o bastante para parar a
expansão até que seja checado — e é a mesma assinatura do Lay 0x0, cujo edge real era 1,1pp e não 34%
porque a convenção de stake inflava o número 16 vezes.

---

## 4. Respostas diretas

**1. `Odd_D_FT ≥ 5,00` é preditor robusto de spread baixo?** Não. Medido em 5.856 jogos, o spread é
plano em ~1,20x em toda a faixa e piora acima de 8,00. A relação relatada é artefato de 5 a 37
observações por célula.

**2. O teto de 7,50 preserva o edge?** Ele preserva a *liability*, que é outra coisa — e isso é
legítimo e bom. O que ele não faz é criar edge: a seleção é feita sobre o preço, e o preço está
eficiente. Resultado medido com o teto: −1,14% a 5% de comissão, −2,60% a 14%.

**3. Disciplina de mesa.** Essas regras valem para quando houver um método aprovado, e eu as imponho
desde já como pré-requisito — não como conserto:
- **Liquidez mínima objetiva**, em dinheiro, não "visível": exigir volume disponível ≥ 5× a sua
  liability na odd alvo, no primeiro nível do livro. "Tem dinheiro na tela" não é critério.
- **Teto absoluto, nunca relativo.** Se a odd passou do teto, pula. Sem "só dessa vez", sem perseguir.
- **Registrar a odd executada, não a planejada.** Foi exatamente a divergência entre as duas que
  produziu três falsos edges no ARKAD (Lay 0x1, CS, e a tríade de ontem).
- **Decidir fora do minuto final.** Entrar a 10–15 min do apito, nunca com o relógio correndo: pressa
  é o que faz aceitar preço pior.
- **Limite diário de entradas**, fixado antes de abrir a tela, para a amostra não virar função do humor.
- **Nada de recuperar red.** Stake fixo por entrada, definido antes.

**4. Veredito: não aprovo para dinheiro real.** Com preço real de execução, a regra proposta dá ROI
negativo em todas as variantes de teto e em todos os cortes de 4,00 a 5,50. O desenho semi-automático
está correto — o que falta é um método com edge no preço real para rodar dentro dele.

---

## 5. O que eu faria em vez disso

1. **Refazer `liga_draw_rate` com a definição exata de produção** e reavaliar o Lay Draw com
   `Odd_D_Lay` real. Essa é a pergunta mais importante em aberto hoje, e vale mais que qualquer método
   novo: ela decide se o segundo edge do mapa continua de pé.
2. **Parar de usar o forward para calibrar.** Se um corte novo precisa ser escolhido, que seja em
   dados até 31/07/2026 e testado de agosto em diante — não o contrário.
3. **Manter a trava manual no desenho.** Ela é a peça certa: ver o preço antes de comprometer dinheiro
   é o que faltou nos três estudos anteriores. Só não peça a ela que salve um método.

*Base: cruzamento de `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` com `..._Betfair.csv` por
(Data, Home, Away), 50.248 jogos de 16/03/2024 a 31/07/2026 — período que não inclui o forward de
agosto a outubro, portanto amostra independente da que gerou a proposta. ROI por liability:
`(1−c)/(odd−1)` no green, −1 no red. IC95 por bootstrap de blocos-dia, 2.000 reamostragens,
semente 20261008.*
