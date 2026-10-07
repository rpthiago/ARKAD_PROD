#!/usr/bin/env bash
# testar_acesso_betfair.sh — roda em QUALQUER maquina e diz o que a Betfair responde de la.
# Nao usa credencial nenhuma.
#
#   bash testar_acesso_betfair.sh
#
# Distingue tres coisas que a versao anterior confundia:
#   BLOQUEIO   -> redireciona para brasilsembets.gov.br (plataforma fora do ar por determinacao da MP)
#   DESAFIO    -> Cloudflare "Attention Required" (host alcancavel, exige cabecalho/certificado certos)
#   ALCANCAVEL -> o proprio servidor da Betfair respondeu (erro de API, 400, JSON) — e o que queremos
# Um 405 em GET num endpoint que so aceita POST NAO e bloqueio: o teste usa o metodo certo em cada um.

set -u
echo "=== onde estou ==="
IP=$(curl -s -m 8 https://api.ipify.org || echo "?")
echo "IP: $IP | $(curl -s -m 8 "http://ip-api.com/line/$IP?fields=country,city,isp" | tr '\n' ' ')"
echo

classifica() {
  local nome="$1" code="$2" corpo="$3" location="$4"
  if printf '%s' "$location" | grep -qi 'brasilsembets'; then
    echo "$nome -> BLOQUEIO DA MP (HTTP $code redireciona para brasilsembets.gov.br)"
  elif printf '%s' "$corpo" | grep -qiE 'brasilsembets|medida provis.ria|acesso bloqueado'; then
    echo "$nome -> BLOQUEIO DA MP (HTTP $code, pagina de bloqueio no corpo)"
  elif printf '%s' "$corpo" | grep -qiE 'attention required|cloudflare|cf-browser-verification'; then
    echo "$nome -> DESAFIO DO CLOUDFLARE (HTTP $code) — host alcancavel, mas exige cliente valido"
  elif printf '%s' "$corpo" | grep -qiE '\{|errorcode|faultcode|ANGX|INVALID_|NO_SESSION|error report'; then
    echo "$nome -> ALCANCAVEL (HTTP $code, respondeu o servidor da Betfair)"
  else
    echo "$nome -> indefinido (HTTP $code): $(printf '%s' "$corpo" | tr -d '\n' | head -c 90)"
  fi
}

prova() {  # nome, url, metodo, dados
  local nome="$1" url="$2" metodo="${3:-GET}" dados="${4:-}"
  local hdr body code loc
  hdr=$(curl -s -D - -o /tmp/_b.html -m 15 -X "$metodo" ${dados:+-d "$dados"} "$url" 2>/dev/null)
  code=$(printf '%s' "$hdr" | awk '/^HTTP/{c=$2} END{print c}')
  loc=$(printf '%s' "$hdr" | grep -i '^location:' | head -1)
  body=$(head -c 500 /tmp/_b.html 2>/dev/null)
  classifica "$nome" "${code:-?}" "$body" "$loc"
}

echo "=== endpoints ==="
prova "api.betfair.bet.br (conta BR)   " "https://api.betfair.bet.br/exchange/betting/rest/v1.0/listEventTypes/" POST '{}'
prova "api.betfair.com   (internacional)" "https://api.betfair.com/exchange/betting/rest/v1.0/listEventTypes/" POST '{}'
prova "identitysso-cert (login, POST)  " "https://identitysso-cert.betfair.com/api/certlogin" POST "username=x&password=y"
echo
echo "Leitura:"
echo "  BLOQUEIO DA MP em api.betfair.bet.br acontece de QUALQUER pais — a plataforma brasileira esta"
echo "  fora do ar, nao e geo-bloqueio. Mudar de host nao resolve isso."
echo "  DESAFIO/ALCANCAVEL em api.betfair.com e identitysso-cert significa que a Betfair internacional"
echo "  responde dali — mas ela exige conta .com, que e separada da conta .bet.br."
