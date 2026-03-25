#!/bin/bash
set -euo pipefail

VNC_PWD=$(python3 -c "import json; print(json.load(open('config/settings.json'))['vnc']['password'])")
DISPLAY_NUM="${DISPLAY:-:99}"
DISPLAY_ID="${DISPLAY_NUM#:}"
VNC_PORT="${VNC_PORT:-5900}"
NOVNC_PORT="${NOVNC_PORT:-6080}"

log_info() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | INFO | $*"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | ERROR | $*" >&2
}

display_ready() {
    xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1
}

wait_for_display() {
    local attempt
    for attempt in $(seq 1 20); do
        if display_ready; then
            log_info "X display $DISPLAY_NUM is ready"
            return 0
        fi
        if ! kill -0 "$XVFB_PID" 2>/dev/null; then
            log_error "Xvfb exited before display $DISPLAY_NUM became ready"
            return 1
        fi
        sleep 1
    done
    log_error "Timed out waiting for X display $DISPLAY_NUM"
    return 1
}

wait_for_port() {
    local service_name="$1"
    local port="$2"
    local pid="$3"
    local attempt

    for attempt in $(seq 1 20); do
        if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
            log_info "$service_name is listening on port $port"
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "$service_name exited before port $port became ready"
            return 1
        fi
        sleep 1
    done
    log_error "Timed out waiting for $service_name on port $port"
    return 1
}

if ! display_ready; then
    if [ -e "/tmp/.X${DISPLAY_ID}-lock" ] || [ -S "/tmp/.X11-unix/X${DISPLAY_ID}" ]; then
        log_info "Removing stale X display artifacts for $DISPLAY_NUM"
        rm -f "/tmp/.X${DISPLAY_ID}-lock"
        rm -f "/tmp/.X11-unix/X${DISPLAY_ID}"
    fi

    log_info "Starting Xvfb on $DISPLAY_NUM"
    Xvfb "$DISPLAY_NUM" -screen 0 1920x1080x24 &
    XVFB_PID=$!
    wait_for_display
else
    log_info "Using existing X display $DISPLAY_NUM"
fi

log_info "Starting fluxbox"
DISPLAY="$DISPLAY_NUM" fluxbox >/tmp/fluxbox.log 2>&1 &
FLUXBOX_PID=$!
sleep 1
if ! kill -0 "$FLUXBOX_PID" 2>/dev/null; then
    log_error "fluxbox exited during startup"
    exit 1
fi

log_info "Starting x11vnc on $DISPLAY_NUM"
x11vnc -display "$DISPLAY_NUM" -forever -shared -passwd "$VNC_PWD" -rfbport "$VNC_PORT" -noxdamage &
X11VNC_PID=$!
wait_for_port "x11vnc" "$VNC_PORT" "$X11VNC_PID"

log_info "Starting noVNC on port $NOVNC_PORT"
/usr/share/novnc/utils/novnc_proxy --vnc "localhost:$VNC_PORT" --listen "$NOVNC_PORT" --web /usr/share/novnc &
NOVNC_PID=$!
wait_for_port "noVNC" "$NOVNC_PORT" "$NOVNC_PID"

exec python server.py
