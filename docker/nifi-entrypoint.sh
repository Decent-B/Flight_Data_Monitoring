#!/bin/sh
set -eu

NIFI_HOME="${NIFI_HOME:-/opt/nifi/nifi-current}"
CERTS_DIR="${CERTS_DIR:-/opt/nifi/certs}"
NIFI_WEB_HTTPS_HOST="${NIFI_WEB_HTTPS_HOST:-0.0.0.0}"
NIFI_WEB_HTTPS_PORT="${NIFI_WEB_HTTPS_PORT:-8443}"
KEYSTORE_PATH="${KEYSTORE_PATH:-${CERTS_DIR}/keystore.jks}"
TRUSTSTORE_PATH="${TRUSTSTORE_PATH:-${CERTS_DIR}/truststore.jks}"
KEYSTORE_TYPE="${KEYSTORE_TYPE:-JKS}"
TRUSTSTORE_TYPE="${TRUSTSTORE_TYPE:-JKS}"
KEYSTORE_PASS_FILE="${CERTS_DIR}/keystore.pass"
TRUSTSTORE_PASS_FILE="${CERTS_DIR}/truststore.pass"

mkdir -p "${CERTS_DIR}"
umask 077

if [ -n "${KEYSTORE_PASS:-}" ]; then
  printf '%s' "${KEYSTORE_PASS}" > "${KEYSTORE_PASS_FILE}"
elif [ -f "${KEYSTORE_PASS_FILE}" ]; then
  KEYSTORE_PASS="$(cat "${KEYSTORE_PASS_FILE}")"
else
  KEYSTORE_PASS="$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32)"
  printf '%s' "${KEYSTORE_PASS}" > "${KEYSTORE_PASS_FILE}"
fi

if [ -n "${TRUSTSTORE_PASS:-}" ]; then
  printf '%s' "${TRUSTSTORE_PASS}" > "${TRUSTSTORE_PASS_FILE}"
elif [ -f "${TRUSTSTORE_PASS_FILE}" ]; then
  TRUSTSTORE_PASS="$(cat "${TRUSTSTORE_PASS_FILE}")"
else
  TRUSTSTORE_PASS="$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32)"
  printf '%s' "${TRUSTSTORE_PASS}" > "${TRUSTSTORE_PASS_FILE}"
fi

if [ ! -f "${KEYSTORE_PATH}" ]; then
  keytool -genkeypair -alias nifi-key \
    -keyalg RSA -keysize 2048 -validity 36500 \
    -keystore "${KEYSTORE_PATH}" -storepass "${KEYSTORE_PASS}" \
    -dname "CN=nifi, OU=IT, O=Project, L=City, ST=State, C=US" \
    -ext SAN=dns:nifi
fi

if [ ! -f "${CERTS_DIR}/nifi-cert.cer" ]; then
  keytool -export -alias nifi-key -file "${CERTS_DIR}/nifi-cert.cer" \
    -keystore "${KEYSTORE_PATH}" -storepass "${KEYSTORE_PASS}" -rfc
fi

if [ ! -f "${TRUSTSTORE_PATH}" ]; then
  keytool -import -trustcacerts -alias nifi-cert \
    -file "${CERTS_DIR}/nifi-cert.cer" -keystore "${TRUSTSTORE_PATH}" \
    -storepass "${TRUSTSTORE_PASS}" -noprompt
fi

sed -i \
  -e "s|^nifi.security.keystore=.*|nifi.security.keystore=${KEYSTORE_PATH}|" \
  -e "s|^nifi.security.keystoreType=.*|nifi.security.keystoreType=${KEYSTORE_TYPE}|" \
  -e "s|^nifi.security.keystorePasswd=.*|nifi.security.keystorePasswd=${KEYSTORE_PASS}|" \
  -e "s|^nifi.security.truststore=.*|nifi.security.truststore=${TRUSTSTORE_PATH}|" \
  -e "s|^nifi.security.truststoreType=.*|nifi.security.truststoreType=${TRUSTSTORE_TYPE}|" \
  -e "s|^nifi.security.truststorePasswd=.*|nifi.security.truststorePasswd=${TRUSTSTORE_PASS}|" \
  -e "s|^nifi.web.https.host=.*|nifi.web.https.host=${NIFI_WEB_HTTPS_HOST}|" \
  -e "s|^nifi.web.https.port=.*|nifi.web.https.port=${NIFI_WEB_HTTPS_PORT}|" \
  -e "s|^nifi.web.http.enabled=.*|nifi.web.http.enabled=false|" \
  "${NIFI_HOME}/conf/nifi.properties"

if [ -n "${SINGLE_USER_CREDENTIALS_USERNAME:-}" ] && [ -n "${SINGLE_USER_CREDENTIALS_PASSWORD:-}" ]; then
  "${NIFI_HOME}/bin/nifi.sh" set-single-user-credentials \
    "${SINGLE_USER_CREDENTIALS_USERNAME}" \
    "${SINGLE_USER_CREDENTIALS_PASSWORD}"
fi

exec "${NIFI_HOME}/bin/nifi.sh" run
