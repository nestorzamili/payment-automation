#!/bin/bash
set -e

VNC_PWD=$(python3 -c "import json; print(json.load(open('config/settings.json'))['vnc']['password'])")

Xvfb :99 -screen 0 1920x1080x24 &
sleep 2

fluxbox &
sleep 1

x11vnc -display :99 -forever -shared -passwd "$VNC_PWD" -rfbport 5900 -noxdamage &
sleep 1

/usr/share/novnc/utils/novnc_proxy --vnc localhost:5900 --listen 6080 --web /usr/share/novnc &
sleep 1

exec python server.py
