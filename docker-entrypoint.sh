#!/bin/bash
set -e

VNC_PWD=$(python -c "import json; print(json.load(open('config/settings.json'))['vnc']['password'])")

Xvfb :99 -screen 0 1920x1080x24 &
sleep 1

x11vnc -display :99 -forever -shared -passwd "$VNC_PWD" -rfbport 5900 &
sleep 1

fluxbox &
sleep 1

exec python server.py
