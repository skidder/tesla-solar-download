#!/bin/bash
# Install Tesla Solar Download as a systemd service
#
# Usage:
#   ./install.sh [username]
#
# If no username is provided, the current user is used.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
USER_NAME="${1:-$USER}"

echo "Installing Tesla Solar Download service for user: $USER_NAME"
echo "Project directory: $PROJECT_DIR"

# Create user-specific service file
SERVICE_FILE="$SCRIPT_DIR/tesla-solar.service"
TIMER_FILE="$SCRIPT_DIR/tesla-solar.timer"

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "Installing as user systemd service (user mode)..."
    
    # Create user systemd directory
    mkdir -p ~/.config/systemd/user
    
    # Create customized service file
    cat > ~/.config/systemd/user/tesla-solar.service << EOF
[Unit]
Description=Tesla Solar Data Download and MQTT Publish
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/run_daily.py
EnvironmentFile=-$PROJECT_DIR/.env

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=tesla-solar

[Install]
WantedBy=default.target
EOF
    
    # Copy timer file
    cat > ~/.config/systemd/user/tesla-solar.timer << EOF
[Unit]
Description=Run Tesla Solar Download Daily

[Timer]
# Run daily at 11:30 PM (after the day's data is complete)
OnCalendar=*-*-* 23:30:00
# Also run 15 minutes after boot if we missed a run
OnBootSec=15min
# Add randomized delay to avoid API rate limits
RandomizedDelaySec=300
Persistent=true

[Install]
WantedBy=timers.target
EOF
    
    # Reload systemd
    systemctl --user daemon-reload
    
    # Enable and start timer
    systemctl --user enable tesla-solar.timer
    systemctl --user start tesla-solar.timer
    
    # Enable lingering so user services run without login
    echo "Enabling lingering for user $USER_NAME..."
    loginctl enable-linger "$USER_NAME" || echo "Note: Run 'sudo loginctl enable-linger $USER_NAME' to enable service to run without login"
    
    echo ""
    echo "Installation complete!"
    echo ""
    echo "Commands:"
    echo "  Check timer status:    systemctl --user status tesla-solar.timer"
    echo "  Check service status:  systemctl --user status tesla-solar.service"
    echo "  View logs:             journalctl --user -u tesla-solar.service -f"
    echo "  Run manually:          systemctl --user start tesla-solar.service"
    echo "  Stop timer:            systemctl --user stop tesla-solar.timer"
    echo "  Disable timer:         systemctl --user disable tesla-solar.timer"
    
else
    echo "Installing as system-wide service (root mode)..."
    
    # Copy service and timer to systemd directory
    cp "$SERVICE_FILE" /etc/systemd/system/tesla-solar@.service
    cp "$TIMER_FILE" /etc/systemd/system/tesla-solar@.timer
    
    # Update paths in service file
    sed -i "s|/home/scott/git/tesla-solar-download|$PROJECT_DIR|g" /etc/systemd/system/tesla-solar@.service
    
    # Reload systemd
    systemctl daemon-reload
    
    # Enable and start timer for specified user
    systemctl enable "tesla-solar@$USER_NAME.timer"
    systemctl start "tesla-solar@$USER_NAME.timer"
    
    echo ""
    echo "Installation complete!"
    echo ""
    echo "Commands:"
    echo "  Check timer status:    sudo systemctl status tesla-solar@$USER_NAME.timer"
    echo "  Check service status:  sudo systemctl status tesla-solar@$USER_NAME.service"
    echo "  View logs:             sudo journalctl -u tesla-solar@$USER_NAME.service -f"
    echo "  Run manually:          sudo systemctl start tesla-solar@$USER_NAME.service"
fi





