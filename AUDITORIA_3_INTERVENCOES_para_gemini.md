# AUDITORIA FORENSE — as três intervenções de 10/09/2026

> Cole tudo abaixo da linha no Gemini.

---

Auditei os commits `4f26bae`, `d92da7b` e `581a11e` contra as 5 Leis e o Hall of Shame do
GEMINI.md. Segue o que confirmei, o que não confirmei, e o que consertei. Tudo é reproduzível —
os scripts estão no repo (`auditar_liquidacao_77.py`, `reunificar_convencao_pnl.py`) e as medições
in-play saíram do coletor da VPS.

**Não estou pedindo concordância — quero a sua leitura, em especial nos pontos abertos do fim.**

---

## 1. Spreads fabricados (`4f26bae`) — ✅ CONFIRMADO, com resquício

**Confirmado:** `pages/01_🏆_Portfolio_Metodos_Aprovados.py` e `automacao_diaria_aprovados.py`
estão limpos — **zero** ocorrências de `back * 1.03` / `back * 1.05`, e agora usam
`_get_series(..., default=np.nan)`, que é o SKIP correto da Lei nº 3. A correção é real.

**Resquício encontrado — a fabricação sobreviveu em outros dois arquivos:**

```
_gerar_excel_backtest_saldo_menor.py:133    fillna(Odd_H_FT * 1.05 + 0.15)
_gerar_excel_todas_odds_saldo_menor.py:119  fillna(Odd_H_FT * 1.05 + 0.15)
```

São do Saldo Menor (já arquivado), então é risco **latente**, não ativo — mas quem rodar esses
geradores produz número fabricado sem aviso.

**Verificação extra que fiz por conta:** o tracker da goleada v1 (`acompanhar_goleada_v1.py`, que é
candidato **ATIVO** com pré-registro aberto) usa `back` — mas legitimamente, porque é método de
**Back Under 2.5**, não de Lay, e faz `dropna(subset=["back"])`. **Sem violação.**

---

## 2. Liquidação dos 77 (`d92da7b`) — ✅ a liquidação; 🔴 a SOMA

**Confirmado, e reproduzi exatamente:** 77 jogos na janela 03-09/09, **68 GREEN / 9 RED**,
`+17,80 u` conforme declarado. Os 9 reds seguem `−(odd−1)` e os greens `+0,95`. **Sem false green:**
os únicos mercados sensíveis a gol tardio na base são 12 linhas de Over/Under 4.5, e **nenhuma cai
na janela dos 77**.

**O problema está na agregação com o histórico. A base somava DUAS convenções de P&L:**

| grupo | N | RED | GREEN | comissão implícita | soma |
|---|---|---|---|---|---|
| até 02/09 | 187 | `−1,0` → **liability** | `(1−c)/(odd−1)` | **3,51%** | +12,27 u |
| 03-09/09 (os 77) | 77 | `−(odd−1)` → **stake** | `0,95` | 5,00% | +17,80 u |

`+12,27 + 17,80 = +30,07 u` **soma unidades diferentes**. No primeiro grupo cada aposta arrisca
1 unidade; no segundo arrisca `(odd−1)` — na odd 6,8 são 5,8 unidades. Escalas ~5× distintas.

**Achado que definiu o conserto:** as colunas em R$ dos 77 novos já usam **liability fixa de R$100**
(`Risco_Red_R$` = 100, `Stake_Sugerida_R$` = 100/(odd−1)). Ou seja, a convenção *pretendida* no
arquivo sempre foi a de **liability** — a mesma dos 187 antigos. O desvio estava só no `PnL_u`
das 77 linhas novas.

### O que executei (`reunificar_convencao_pnl.py`, com backup automático)

Toda a base recalculada numa convenção única: **LIABILITY = 1 unidade, comissão 5%** (Lei nº 4).
`GREEN = (1−0,05)/(odd−1)` · `RED = −1,0`. Adicionei `PnL_stake_u` (convenção stake, para
referência) e `Convencao_PnL` documentando cada linha. Colunas em R$ recoerentes com liability
de R$100.

```
soma do PnL_u ANTES (misturado):  +30,07 u
soma do PnL_u DEPOIS (unificado): +12,89 u
para referência, em convenção STAKE=1u:  +93,50 u

por janela, na convenção única:
  antigos (até 02/09)  N=187  WR 91,98%  BE 86,51%  gap +5,47pp  PnL +11,841 u
  os 77   (03-09/09)   N= 77  WR 88,31%  BE 86,89%  gap +1,42pp  PnL  +1,048 u

por método:
  Lay Draw (Fav <=1.40)          N=165  WR 89,09%  BE 85,71%  gap +3,38pp  +6,365 u
  Lay Home / DC X2 (Fav Visit.)  N= 81  WR 92,59%  BE 86,67%  gap +5,92pp  +5,522 u
  Lay Over 4.5 FT (Under Pesado) N= 18  WR 100,0%  BE 94,75%  gap +5,25pp  +1,001 u
```

**Duas consequências que mudam a leitura:**
1. Os 77 jogos contribuem com **+1,05 u**, não +17,80 u — a contribuição estava superestimada ~17×
   pelo descasamento de convenção.
2. O gap sobre o break-even **caiu de +5,47pp (histórico) para +1,42pp (os 77)**. Não é negativo,
   mas é bem menos folgado do que o número anterior sugeria.

**Ponto adicional:** a comissão implícita do grupo antigo era **3,51%**, não 4,5% nem 5%. A Betfair
BR cobra 5% (4,5% com desconto). Comissão subdimensionada infla resultado; a reunificação
padronizou em 5%.

---

## 3. Lay 0x1 In-Play, Rota C (`581a11e`) — ✅ o código; 🔴 a medição

### O que está correto, e é mérito

- **Break-even** `(odd−1)/(odd−comissão)` exatamente conforme a Lei nº 4.
- **P&L** `+(1−c)` no green, `−(odd−1)` no red — correto.
- **Sem odd fabricada:** odd ausente/NaN → rejeita.
- **Sem look-ahead:** a validação só recebe estado corrente (minuto, placar atual, odd atual).
- **Stake-zero genuíno:** `Stake_Real = 0.0` no logger, com `Stake_Nominal = 100.0` separado apenas
  para dimensionar liability. Correto.

### E a faixa de odd EXISTE — medida, não suposta

Medi no coletor da VPS, filtrando pelo placar real (runner de menor lay do `CORRECT_SCORE`):

```
jogos com 0x0 no minuto 55-75:              643
com odd de lay do CS 0-1 capturada:         624
  ABAIXO de 2,00:      0
  NA FAIXA 2,00-5,50:  187  (30%)
  ACIMA de 5,50:       437
odd média 41,38 · min 2,96 · max 810,00
```

**Isso é uma melhora real** em relação às duas propostas anteriores: o Radar HT tinha 3 sinais na
banda suposta em 360, e o Late Goal 4 em 77. Aqui a banda é 30% da população. A odd não foi assumida.

### Mas o número que decide reprova

Cruzei os mesmos gatilhos com o placar **final** (última captura pós-FT do coletor):

```
0-0 no min 55-75, casados com placar final: N=610
  TODOS                          terminaram 0x1: 112 de 610 = 18,36%
  SÓ na faixa 2,00-5,50 (a regra):  52 de 181 = 28,73%

odd de lay média na faixa: 4,58  →  break-even exige 79,05% de não-0x1
WR real do lay na faixa:                        71,27%
margem sobre o break-even:                       −7,78 pp
```

**A faixa de odd seleciona exatamente os jogos onde o 0x1 é mais provável** — 28,73% contra 18,36%
na população geral. O mercado não está errando: a odd baixa é *consequência* do risco maior.

**Viés declarado da minha medição:** reconstruí o placar final pelo CS do coletor, que **perde gol
tardio**. Um gol tardio do mandante viraria um 0x1 em 1x1 (GREEN), então o viés é **a favor** do
método — o número real tende a ser pior que −7,78pp, não melhor.

### Três defeitos técnicos

**(a) Filtro permissivo — CONSERTADO por mim.** O código tinha:
```python
if odd_h_back_pre is not None and pd.notna(odd_h_back_pre):
    if odd_h_back_pre > ODD_H_BACK_PRE_MAX: return False, ...
```
Se a odd pré-jogo faltasse, o jogo **passava**. É a linha do Hall of Shame *"filtro permissivo:
NaN passa → NaN = SKIP"*. E o favoritismo pré-jogo é parte da tese da Rota C, então sem a odd o
gatilho não pode ser avaliado. Agora: `odd_h_back_pre` ausente ou NaN → **SKIP**. Testado:

```
sem odd pre-jogo        -> SKIP  SEM_ODD_PRE_JOGO (Lei nº 3)
odd pre-jogo NaN        -> SKIP  SEM_ODD_PRE_JOGO
mandante zebra (2.80)   -> SKIP  MANDANTE_ZEBRA_PRE
valido (fav 1.80)       -> PASSA APROVADO_LAY_0X1_INPLAY
placar 1x0              -> SKIP  PLACAR_INVALIDO
odd de lay ausente      -> SKIP  ODD_LAY_FORA_FAIXA
```

**(b) A pressão é medida mas nunca exigida.** O `score_pressao` (SoT, xG, posse) é calculado e
gravado, mas **não filtra nada**. A tese diz *"se o mandante estiver pressionando, a chance de 0x1
é reduzida"* — o código não impõe isso. Ou a tese muda, ou o filtro entra. **Não mexi**: é decisão
de desenho, não bug.

**(c) Break-even declarado impreciso.** O texto diz "66% a 78%". Na faixa 2,00-5,50 o range real é
**51,3% a 82,6%**. Os 66-78% correspondem a odds 2,95-4,50.

---

## O que eu quero de você

1. **A reunificação da convenção está correta?** Escolhi LIABILITY=1u com comissão 5%, por três
   razões: é a régua economicamente correta para lay (o capital em risco é a liability); é o que as
   colunas em R$ dos 77 já faziam; e é o que os 187 antigos já usavam. Se discordar, qual convenção
   e por quê — e note que a escolha muda o total de +12,89 u para +93,50 u.

2. **A comissão de 3,51% do grupo antigo tem origem conhecida?** Não achei de onde veio. Se for
   desconto real de volume da conta, me diga e eu reponho.

3. **A Rota C, diante dos −7,78pp:** você mantém, mata, ou reformula? Se reformular, o único pedaço
   da tese que **ainda não foi testado** é a pressão (item b) — o `score_pressao` como filtro
   obrigatório. Vale pré-registrar isso como hipótese nova, ou é garimpar no mesmo dado?

4. **Os dois geradores do Saldo Menor** que ainda fabricam odd: neutralizo, ou têm uso que eu não
   conheço?

5. **Uma pergunta de método:** o gap sobre o break-even caiu de +5,47pp (187 antigos) para +1,42pp
   (77 novos). Na sua leitura isso é variância, deterioração real, ou diferença de composição de
   métodos entre as duas janelas?

Sem hedge motivacional; nenhuma afirmação de resultado sem intervalo de confiança. Se faltar dado
para responder algo, diga qual coluna ou campo precisa passar a ser coletado.
