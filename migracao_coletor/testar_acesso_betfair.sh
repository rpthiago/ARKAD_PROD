#!/usr/bin/env bash
# testar_acesso_betfair.sh — roda em QUALQUER maquina e diz se a API da Betfair esta acessivel de la.
# Nao usa credencial nenhuma: um endpoint alcancavel responde JSON de erro (sessao ausente);
# um endpoint bloqueado responde a pagina HTML do bloqueio nacional.
#
#   bash testar_acesso_betfair.sh
#
# Use isto ANTES de migrar qualquer coisa: se o host candidato nao enxergar a Betfair, nao adianta mudar.

set -u
echo "=== onde estou ==="
IP=$(curl -s -m 8 https://api.ipify.org || echo "?")
GEO=$(curl -s -m 8 "http://ip-api.com/line/$IP?fields=country,city,isp" | tr '\n' ' ' || echo "?")
echo "IP: $IP | $GEO"
echo

testa() {
  local url="$1" nome="$2"
  local corpo code
  corpo=$(curl -s -m 15 -w $'\n%{http_code}' "$url" 2>/dev/null) || { echo "$nome -> falha de rede"; return; }
  code=$(printf '%s' "$corpo" | tail -1)
  corpo=$(printf '%s' "$corpo" | sed '$d' | tr -d '\r' | head -c 400)
  if printf '%s' "$corpo" | grep -qiE '<!doctype html|<html|brasilsembets|medida provis'; then
    echo "$nome -> BLOQUEADO (HTTP $code, veio pagina HTML)"
  elif printf '%s' "$corpo" | grep -qiE '\{|faultcode|ANGX|INVALID_SESSION|NO_SESSION|errorCode'; then
    echo "$nome -> ACESSIVEL (HTTP $code, respondeu JSON/erro de API como esperado)"
  else
    echo "$nome -> indefinido (HTTP $code): $(printf '%s' "$corpo" | head -c 120)"
  fi
}

echo "=== endpoints ==="
testa "https://api.betfair.bet.br/exchange/betting/rest/v1.0/listEventTypes/" "api.betfair.bet.br (conta BR)"
testa "https://api.betfair.com/exchange/betting/rest/v1.0/listEventTypes/"    "api.betfair.com   (internacional)"
testa "https://identitysso-cert.betfair.com/api/certlogin"                     "login por certificado"
echo
echo "Leitura: para o coletor funcionar, 'api.betfair.bet.br' e 'login por certificado' precisam"
echo "aparecer como ACESSIVEL. Se so o .com estiver acessivel, a conta brasileira nao serve la."
