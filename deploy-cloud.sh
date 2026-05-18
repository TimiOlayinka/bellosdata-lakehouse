#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# BellosData Cloud Bootstrap — Data Platform Instance
# Runs once via Lightsail user_data or manually via SSH
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

echo "═══════════════════════════════════════════════════"
echo "  BellosData Cloud Platform — Bootstrap"
echo "═══════════════════════════════════════════════════"

# ── System dependencies ──
echo "[1/5] Installing system packages..."
sudo yum update -y -q
sudo yum install -y -q docker git

# ── Docker ──
echo "[2/5] Configuring Docker..."
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user

# ── Docker Compose v2 ──
echo "[3/5] Installing Docker Compose..."
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# ── Clone repository ──
echo "[4/5] Cloning BellosData platform..."
cd /home/ec2-user
if [ -d "platform" ]; then
  cd platform && git pull origin main && cd ..
else
  git clone https://github.com/TimiOlayinka/boohoo-data-pipeline.git platform
fi

# ── Create directory structure ──
echo "[5/5] Setting up directories..."
mkdir -p /home/ec2-user/platform/unity-catalog/etc/conf

# ── Create .env template (replace with real keys) ──
if [ ! -f /home/ec2-user/platform/.env ]; then
  cat > /home/ec2-user/platform/.env <<'ENVFILE'
# BellosData Cloud Platform — Environment Variables
# Replace REPLACE_ME with actual IAM access keys
AWS_ACCESS_KEY_ID=REPLACE_ME
AWS_SECRET_ACCESS_KEY=REPLACE_ME
AWS_DEFAULT_REGION=eu-west-2
ENVFILE
fi

sudo chown -R ec2-user:ec2-user /home/ec2-user/platform

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Bootstrap complete!"
echo "═══════════════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo "  1. Edit .env with IAM access keys:"
echo "     nano /home/ec2-user/platform/.env"
echo ""
echo "  2. Start services:"
echo "     cd /home/ec2-user/platform"
echo "     docker compose -f unity-catalog/compose.cloud.yaml up -d"
echo "     docker compose -f airflow/docker-compose.cloud.yaml up -d"
echo ""
echo "  3. Register tables:"
echo "     pip3 install requests"
echo "     python3 unity-catalog/scripts/register_tables.py"
echo ""
