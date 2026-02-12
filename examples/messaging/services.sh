#!/usr/bin/env bash
# Start/stop local services.

MINIO_PID_FILE="${TMPDIR:-/tmp}/messaging-minio.pid"

start() {
    echo "Starting services..."
    brew services start postgresql@18
    brew services start valkey
    nohup minio server ~/minio-data --console-address ":9001" >/dev/null 2>&1 &
    echo $! > "$MINIO_PID_FILE"
}

stop() {
    if [[ -f "$MINIO_PID_FILE" ]]; then
        kill "$(cat "$MINIO_PID_FILE")" 2>/dev/null
        rm -f "$MINIO_PID_FILE"
    fi
    brew services stop postgresql@18
    brew services stop valkey
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    *) echo "Usage: $0 start|stop" >&2; exit 1 ;;
esac
