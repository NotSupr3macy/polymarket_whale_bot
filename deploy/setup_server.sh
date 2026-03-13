#!/usr/bin/env bash
# One-time server setup for Ubuntu 22.04+ VPS.
# Run as root or with sudo.

set -euo pipefail

echo "=== Polymarket Whale Bot — Server Setup ==="

# System updates
apt-get update && apt-get upgrade -y

# Python 3.11+ and essentials
apt-get install -y python3 python3-pip python3-venv tmux git htop

# Create bot user (non-root)
if ! id -u botuser &>/dev/null; then
    useradd -m -s /bin/bash botuser
    echo "Created user: botuser"
fi

# Create project directory
BOT_DIR="/home/botuser/whale-bot"
mkdir -p "$BOT_DIR/logs"
chown -R botuser:botuser "$BOT_DIR"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Copy project files:  scp -r ./* botuser@YOUR_SERVER:~/whale-bot/"
echo "  2. SSH in:              ssh botuser@YOUR_SERVER"
echo "  3. Create venv:         cd ~/whale-bot && python3 -m venv venv"
echo "  4. Activate:            source venv/bin/activate"
echo "  5. Install deps:        pip install -r requirements.txt"
echo "  6. Create .env:         cp .env.example .env && nano .env"
echo "  7. Test dry run:        python3 cli.py --verbose"
echo "  8. Start bot:           ./deploy/start.sh --live --bankroll 500"
echo "  9. Add health cron:     crontab -e"
echo "     */5 * * * * /home/botuser/whale-bot/deploy/health_check.sh --live"
