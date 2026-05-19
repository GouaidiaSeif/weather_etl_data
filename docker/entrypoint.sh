#!/bin/sh
set -e

wait_for() {
  host="$1"
  port="$2"
  name="$3"
  echo "Waiting for ${name} at ${host}:${port}..."
  for i in $(seq 1 60); do
    if python -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('${host}', ${port})); s.close()" 2>/dev/null; then
      echo "${name} is ready."
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${name}." >&2
  exit 1
}

if [ "${STORAGE_BACKEND:-local}" = "minio" ]; then
  MINIO_HOST="${MINIO_ENDPOINT%%:*}"
  MINIO_PORT="${MINIO_ENDPOINT##*:}"
  wait_for "${MINIO_HOST}" "${MINIO_PORT}" "MinIO"
fi

if [ -n "${MONGODB_HOST:-}" ] && [ "${MONGODB_HOST}" != "localhost" ]; then
  wait_for "${MONGODB_HOST}" "${MONGODB_PORT:-27017}" "MongoDB"
fi

exec "$@"
