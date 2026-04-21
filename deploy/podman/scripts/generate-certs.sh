#!/bin/sh
set -eu

if [ -f /certs/keycloak.crt ]; then
  echo "TLS certs already exist, skipping generation"
  exit 0
fi

apk add --no-cache openssl > /dev/null

echo "Generating CA..."
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /certs/ca.key -out /certs/ca.crt \
  -days 3650 -subj "/CN=FleetShift Local CA" 2>/dev/null

echo "Generating Keycloak server certificate..."
openssl req -newkey rsa:2048 -nodes \
  -keyout /certs/keycloak.key -out /tmp/keycloak.csr \
  -subj "/CN=keycloak" 2>/dev/null

printf "subjectAltName=DNS:keycloak,DNS:localhost" > /tmp/san.cnf
openssl x509 -req -in /tmp/keycloak.csr \
  -CA /certs/ca.crt -CAkey /certs/ca.key -CAcreateserial \
  -out /certs/keycloak.crt -days 3650 \
  -extfile /tmp/san.cnf 2>/dev/null

rm -f /tmp/keycloak.csr /tmp/san.cnf /certs/ca.srl
chmod 644 /certs/*.crt
chmod 644 /certs/*.key
echo "TLS certs generated in /certs/"
