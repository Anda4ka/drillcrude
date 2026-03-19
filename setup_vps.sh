#!/bin/bash
# CRUDE Driller — VPS setup for Ubuntu 22.04
# Usage: curl/paste this script, run: bash setup_vps.sh
set -e

echo "=== CRUDE Driller VPS Setup ==="

# 1. Install Python 3.11+ and pip
echo "[1/5] Installing Python..."
sudo apt update -qq
sudo apt install -y python3 python3-pip python3-venv screen

# 2. Create working directory
echo "[2/5] Creating directory..."
mkdir -p ~/driller
cd ~/driller

# 3. Create venv and install deps
echo "[3/5] Installing Python packages..."
python3 -m venv venv
source venv/bin/activate
pip install -q aiohttp openai zai-sdk

# 4. Prompt for .env
echo "[4/5] Setting up .env..."
if [ ! -f .env ]; then
    cat > .env << 'ENVEOF'
BANKR_API_KEY=
DRILLER_ADDRESS=0x04b906d694d0b2bc0fb6be43189af018cd861686
COORDINATOR_URL=https://coordinator-production-38c0.up.railway.app

LLM_BACKEND=zai
LLM_MODEL=glm-5
ZAI_API_KEY=

DRILLER_DEBUG=false
DRILLER_QUIET=true
RECEIPT_COOLDOWN=30
ENVEOF
    echo ""
    echo ">>> EDIT .env NOW: nano ~/driller/.env"
    echo ">>> Fill in BANKR_API_KEY and ZAI_API_KEY"
    echo ""
else
    echo ".env already exists, skipping"
fi

# 5. Create systemd service for auto-restart
echo "[5/5] Creating systemd service..."
sudo tee /etc/systemd/system/crude-driller.service > /dev/null << 'SVCEOF'
[Unit]
Description=CRUDE Driller Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/driller
ExecStart=/root/driller/venv/bin/python3 /root/driller/crude_driller.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SVCEOF

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "  1. Copy crude_driller.py to ~/driller/"
echo "  2. Edit .env:  nano ~/driller/.env"
echo "  3. Fill in BANKR_API_KEY and ZAI_API_KEY"
echo "  4. Test run:   cd ~/driller && source venv/bin/activate && python3 crude_driller.py"
echo "  5. If works, enable service:"
echo "     sudo systemctl daemon-reload"
echo "     sudo systemctl enable crude-driller"
echo "     sudo systemctl start crude-driller"
echo ""
echo "Monitor:"
echo "  sudo journalctl -u crude-driller -f          # live logs"
echo "  sudo systemctl status crude-driller           # status"
echo "  sudo systemctl restart crude-driller          # restart"
echo "  cat ~/driller/crude_driller_state.json        # stats"
