#!/bin/bash
# =============================================================
#  FloodWatch — instance bootstrap (User Data script)
#
#  Works on:
#    AWS EC2    — paste into "User data" when creating a
#                 Launch Template or Instance
#    DigitalOcean — paste into "User data" when creating a
#                   Droplet or Droplet template
#
#  Runs ONCE on first boot as root.
#  Safe to re-run (idempotent).
#
#  After this runs the instance is Docker-ready.
#  GitHub Actions then SSH in and starts the actual containers.
# =============================================================
set -euo pipefail

# ── Docker ────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg

    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
      | tee /etc/apt/sources.list.d/docker.list > /dev/null

    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io \
        docker-buildx-plugin docker-compose-plugin
fi

systemctl enable --now docker

# ── Add default user to docker group ─────────────────────────
# EC2 default user is "ubuntu"; DO default is "root" (no-op) or a named user.
for USER in ubuntu ec2-user; do
    if id "$USER" &>/dev/null; then
        usermod -aG docker "$USER"
    fi
done

# ── App directory ─────────────────────────────────────────────
mkdir -p /opt/floodwatch
chmod 755 /opt/floodwatch

echo "Bootstrap complete — Docker $(docker --version)"
