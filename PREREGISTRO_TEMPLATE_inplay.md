# TEMPLATE — pré-registro de hipótese in-play (preencher ANTES de olhar qualquer resultado)

> Copie este arquivo para `PREREGISTRO_<nome>.md`, preencha, **commite**, e só então rode o harness.
> Harness genérico: `varredura_over_inplay.py` (Over/Under por minuto × estado × favoritismo, estado pelas linhas
> O/U batidas, placar oficial). Para outro mercado, copie o harness e mude só a função de resultado.
> Dados: coletor da VPS (odds minuto a minuto, todos os mercados, desde 16/08/2026) + `placares_ft.csv` (oficial).

## 1. Mecanismo (obrigatório — sem isto a hipótese não entra)

**Por que o mercado ao vivo erraria aqui?** Uma frase, concreta, falsificável.
Exemplos do que NÃO é mecanismo: "o time está pressionando" (o mercado vê), "78% dos jogos têm gol depois disso"
(P(evento) ≠ edge), "a odd está baixa" (odd baixa é risco, não desconto).
Exemplos do que É mecanismo: "o mercado X suspende por N minutos após o evento Y e reabre com preço defasado em
relação a Z" (testável: medir o preço na reabertura vs o preço justo).

**Mecanismo:** ______________________________________________

**O que já foi testado nessa direção e morreu** (ler `INVENTARIO_METODOS_REPROVADOS.md` seção D antes):
______________________________________________

## 2. A regra (congelada)

| campo | valor |
|---|---|
| mercado / runner | |
| lado (back/lay) | |
| estado do jogo (placar via linhas O/U batidas — nunca pelo CS de menor lay) | |
| janela de minuto | |
| favoritismo pré-jogo | |
| faixa de odd (real, do coletor, na primeira captura da janela) | |
| uma aposta por jogo? | |

## 3. Critério de decisão (3 vias)

- APROVA: N ≥ ___ **e** piso do IC95 (bloco-dia, ≥6 dias) > 0 **e** reds observados ≥ 5.
- REPROVA: teto do IC95 < 0.
- INCONCLUSIVO: resto. Inconclusivo não autoriza escalar nem "refinar".

## 4. Dado de julgamento

- Só jogos com KO ≥ ____ (a data deste commit). O que já existe no coletor pode servir de **primeiro olhar**, e
  fica declarado como tal.
- Snapshot 1: ____ · Snapshot 2: ____ (mínimo 30 dias depois) · Snapshot 3: ____.

## 5. O que NÃO fazer

- Não mudar janela, faixa ou estado depois de ver resultado. Não somar células. Não olhar antes do snapshot.
- Não usar o Correct Score de menor lay como "placar atual" (27,5% certo aos 10-25 min — é o placar final mais provável).
- Não usar guard que dependa do resultado (ex.: "FT < parcial → fora").
