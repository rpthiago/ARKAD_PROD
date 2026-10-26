# Contexto para o Claude: Auditoria dos Filtros de Produção do Lay Draw e Simulação de Stop Red no Portfólio Limpo

Claude, após o seu relatório de 23/10 (onde você apontou que o forward acumulava −17,87u e que o Lay Draw era o grande vilão com −14,65u em 291 jogos), o Thiago fez um questionamento cirúrgico:
> *"Ainda tenho dúvida se vocês estão pegando os filtros corretos. Não precisa olhar o que não está no filtro."*
> *"E se tivéssemos feito o stop red nesse método, junto com os outros?"*

Fizemos uma autópsia forense profunda de linha por linha do `forward_ko_ledger.csv` cruzando com as regras congeladas em `sinais_ko_core.py` e rodamos uma simulação cronológica exata de Stop Red (KO + 115 min).

Os achados mudam substancialmente o diagnóstico. Gostaríamos da sua auditoria adversarial e validação dos 3 pontos abaixo.

---

## 1. A Descoberta da Contaminação da Base: 72% dos Jogos Estavam FORA dos Filtros

O número de **292 jogos e −14,48u** veio da leitura crua do ledger forward (que iniciou em 16/09, antes do deploy das travas de governança, teto de odd e spread em 08/10 e 13/10).

Ao reclassificar os 292 jogos contra as regras oficiais de produção (`sinais_ko_core.py`):
* `Odd_Fav <= 1.40`
* `4.50 <= Odd_Lay <= 7.00`
* `Liq_Lay >= R$ 200.00`
* `Governança de Ligas (eh_jogo_ignorado: sem ligas periféricas, divisões amadoras, seleções, feminino)`

| Conjunto | N | Greens | Reds | WR | BE Exigido | P&L Real |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Fora dos Filtros de Produção** | **211** | 170 | 41 | 80,6% | 86,8% | **−14,00 u** 🔴 |
| **Dentro dos Filtros de Produção** | **81** | 68 | 13 | 84,0% | 84,6% | **−0,49 u** ⚪ |
| **Total Bruto do Ledger** | **292** | 238 | 54 | 81,4% | 85,8% | **−14,48 u** |

### O que eliminou os 211 jogos sujos:
1. **Ligas Fora da Governança (131 jogos):** Butão, Ruanda, Bangladesh, Argélia 2, Tailândia 2, Hong Kong 3, futebol feminino (WSL, Frauen-Bundesliga), torneios sub-21 e amistosos.
2. **Odd Lay Acima do Teto $\le 7.00$ (108 jogos):** Odds de Lay 8.0, 9.0, 10.0, 12.0 (onde a liability explode e o break-even exigido sobe para 88%–92%).
3. **Liquidez Baixa < R$ 200 (134 jogos):** Livros sem volume real.

> **Fato 1:** **96,6% do prejuízo (−14,00u de −14,48u)** veio de jogos que violavam as regras atuais de produção. No filtro limpo, o Lay Draw perdeu apenas **−0,49 u em 81 jogos** (praticamente no break-even flat).

---

## 2. Simulação Cronológica do Stop Red no Portfólio 100% LIMPO

Aplicando os filtros de produção em **todos os métodos** no mesmo ledger, o portfólio limpo totaliza **162 jogos**.

Simulamos o Stop Red considerando o tempo real de liquidação ($T_{\text{KO}} + 115\text{ min}$): se um red ocorre às 11:00, ele só liquida às 12:55. Jogos simultâneos rolam juntos; o cancelamento atua apenas sobre partidas com kickoff posterior à liquidação do red.

### Tabela: Desempenho Método a Método no Portfólio Limpo

| Método (Filtros Oficiais) | Jogos Válidos | Sem Stop (Original) | Stop 1 Red no Dia (Isolado) | Stop 2 Reds ("Dois reds eu paro") |
|:---|:---:|:---:|:---:|:---:|
| **Lay Home / DC X2** (Teto 8.0 · Liq 200) | 58 | **+3,88 u** (53G / 5R) | **+4,27 u** 🟢 *(Melhora)* | **+3,88 u** (pulou 0) |
| **Lay Draw** (Teto 7.0 · Liq 200) | 81 | **−0,49 u** (68G / 13R) | **+0,18 u** 🟢 *(Vira Positivo!)* | **−1,56 u** 🔴 *(Piora)* |
| **Lay 0x3 Top 3** (Teto 30.0 · Liq 200) | 18 | **+0,76 u** (18G / 0R) | **+0,76 u** (zero reds) | **+0,76 u** (zero reds) |
| **Lay 2x2 Top 3** (Teto 14.0 · Liq 200) | 0 | *0 jogos (odd > 14)* | *0 jogos* | *0 jogos* |
| **Lay Over 4.5** (Teto 20.0 · Liq 200) | 5 | **−0,79 u** (4G / 1R) | **−0,79 u** (pulou 0) | **−0,79 u** (pulou 0) |
| **TOTAL DO PORTFÓLIO LIMPO** | **162** | **+3,36 u** | **+4,42 u** 🟢 | **+2,29 u** |

---

## 3. As Duas Descobertas Mecânicas

### A. O Stop 1 Red Transforma o Lay Draw em Positivo (+0,18u)
Nos 81 jogos limpos do Lay Draw, o Stop 1 Red pulou 16 jogos: 13 Greens e 3 Reds.
* No dia 10/10, o Ajax deu Red às 11h30.
* Com o Stop 1 Red ativo, o robô **cancelou os jogos do final da tarde/noite**:
  - *Vasco 0-0 Remo* (RED)
  - *Peñarol 1-1 Progreso* (RED)
  - *Junior Barranquilla 1-1 Deportivo Pereira* (RED)
* Ao cortar esses 3 reds agrupados no mesmo dia, o Lay Draw economizou 3 liabilities completas e **fechou no verde (+0,18 u)**.

### B. O Perigo do Stop Red Global (Interferência entre Métodos)
* Se o Stop Red for **global** (um red em qualquer método trava todos os outros métodos do dia), o Lay Draw "canibaliza" os lucros do Lay Home:
  - Nos dados brutos, o Stop 1 Red Global cancelou **90 jogos do Lay Home** (81 Greens e 9 Reds), jogando fora **+3,99 u de lucro**.
* **Conclusão de Engenharia:** O Stop Red **deve ser isolado por método** (ou proteger o Lay Home), para que a variância do Draw ou do Correct Score não aborte as entradas do melhor método do portfólio.

---

## Perguntas para sua Auditoria:

1. **Validação dos Filtros:** Você concorda que avaliar o Lay Draw pelos 292 jogos brutos (incluindo 211 jogos fora de governança / odds 8-12 / sem liquidez) violava a Lei 2 ("Backtest e live têm que ser o mesmo objeto") e a Lei 7 ("Uma base por método")?
2. **Veredito sobre o Lay Draw:** Sabendo que no filtro limpo ele está em **−0,49u** e que com **Stop 1 Red isolado ele vira +0,18u**, você mantém a recomendação de desligá-lo sumariamente, ou aprova mantê-lo ativo na VPS com o **disjuntor de Stop 1 Red diário** ativo?
3. **Arquitetura do Disjuntor na VPS:** Qual a sua visão sobre incorporar o disjuntor diário de "Stop 1 Red por Método" diretamente dentro de `sinais_ko_vps.py` / `stop_diario.py`?
