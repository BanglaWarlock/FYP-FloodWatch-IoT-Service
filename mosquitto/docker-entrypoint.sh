#!/bin/sh
# ============================================================
#  docker-entrypoint.sh
#
#  Checks whether TLS certificates are present.
#  If yes  → starts Mosquitto with mTLS config (port 8883)
#  If no   → starts Mosquitto in open mode   (port 1883)
#
#  To upgrade to TLS:
#    1. Uncomment the three COPY lines in Dockerfile.flyio
#    2. Run scripts/gen-certs.sh to generate the cert files
#    3. Rebuild and redeploy — this script detects them automatically
# ============================================================

set -e

# Volume mounts as root at runtime — fix ownership so mosquitto user can write
chown -R mosquitto:mosquitto /mosquitto/data /mosquitto/log 2>/dev/null || true

CERT_DIR="/mosquitto/certs"
CA_CRT="$CERT_DIR/ca.crt"
SERVER_CRT="$CERT_DIR/server.crt"
SERVER_KEY="$CERT_DIR/server.key"

if [ -f "$CA_CRT" ] && [ -f "$SERVER_CRT" ] && [ -f "$SERVER_KEY" ]; then
    echo "[entrypoint] All three cert files found."
    echo "[entrypoint] Starting Mosquitto with mTLS on port 8883."
    exec mosquitto -c /mosquitto/config/mosquitto-tls.conf
else
    echo "[entrypoint] =============================================="
    echo "[entrypoint] WARNING: No certificates found."
    echo "[entrypoint] Starting Mosquitto in OPEN mode on port 1883."
    echo "[entrypoint] All clients can connect without authentication."
    echo "[entrypoint] This is intentional for Phase 1 development."
    echo "[entrypoint] =============================================="
    exec mosquitto -c /mosquitto/config/mosquitto.conf
fi