#!/usr/bin/env bash
# Install/refresh the tunnel-svc binary + systemd unit on the VPS.
# Expects ./tunnel-svc (the linux binary) and ./tunnel.service in CWD.
# Used by the Deploy Tunnel GitHub workflow; safe to run by hand as root.
# Does NOT touch /opt/robotunnel-tunnel/config/.env.
set -euo pipefail

APP_DIR=/opt/robotunnel-tunnel
BIN_DIR="$APP_DIR/bin"

id -u robotunnel >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin robotunnel || true
install -d -o robotunnel -g robotunnel "$APP_DIR" "$BIN_DIR" "$APP_DIR/config"

install -m 0755 ./tunnel-svc "$BIN_DIR/tunnel-svc"
install -m 0644 ./tunnel.service /etc/systemd/system/robotunnel-tunnel.service

systemctl daemon-reload
systemctl enable robotunnel-tunnel
systemctl restart robotunnel-tunnel
echo "robotunnel-tunnel restarted; status:"
systemctl --no-pager --full status robotunnel-tunnel | head -n 8 || true
