#!/usr/bin/env bash
set -euo pipefail

NAME="${NAME:-outbox-mysql-bench}"
PORT="${PORT:-3307}"
DATA_SIZE="${DATA_SIZE:-8g}"
DATA_DIR="${DATA_DIR:-/tmp/outbox-mysql-bench-data}"
ROOT_PASSWORD="${ROOT_PASSWORD:-secret}"
DATABASE="${DATABASE:-outbox}"
PROFILE="${PROFILE:-fast}"
FAST="${FAST:-}"
USE_TMPFS="${USE_TMPFS:-}"
REMOVE_CONTAINER="${REMOVE_CONTAINER:-1}"
BINLOG="${BINLOG:-}"
BINLOG_EXPIRE_SECONDS="${BINLOG_EXPIRE_SECONDS:-}"
BINLOG_MAX_SIZE="${BINLOG_MAX_SIZE:-}"

if [[ -z "${FAST}" ]]; then
  if [[ "${PROFILE}" == "prod" ]]; then
    FAST=0
  else
    FAST=1
  fi
fi

if [[ -z "${USE_TMPFS}" ]]; then
  if [[ "${PROFILE}" == "prod" ]]; then
    USE_TMPFS=0
  else
    USE_TMPFS=1
  fi
fi

if [[ -z "${BINLOG}" ]]; then
  if [[ "${PROFILE}" == "prod" ]]; then
    BINLOG=1
  else
    BINLOG=0
  fi
fi

if [[ -z "${BINLOG_EXPIRE_SECONDS}" ]]; then
  if [[ "${PROFILE}" == "prod" ]]; then
    BINLOG_EXPIRE_SECONDS=3600
  else
    BINLOG_EXPIRE_SECONDS=600
  fi
fi

if [[ -z "${BINLOG_MAX_SIZE}" ]]; then
  if [[ "${PROFILE}" == "prod" ]]; then
    BINLOG_MAX_SIZE=1G
  else
    BINLOG_MAX_SIZE=256M
  fi
fi

if docker ps -a --format '{{.Names}}' | grep -q "^${NAME}\$"; then
  echo "Container ${NAME} already exists."
  exit 1
fi

DOCKER_ARGS=(
  -d
  --name "${NAME}"
  --network host
  -e "MYSQL_ROOT_PASSWORD=${ROOT_PASSWORD}"
  -e "MYSQL_DATABASE=${DATABASE}"
)

if [[ "${REMOVE_CONTAINER}" == "1" ]]; then
  DOCKER_ARGS=(--rm "${DOCKER_ARGS[@]}")
fi

if [[ "${USE_TMPFS}" == "1" ]]; then
  DOCKER_ARGS+=(--tmpfs "/var/lib/mysql:rw,size=${DATA_SIZE}")
else
  mkdir -p "${DATA_DIR}"
  DOCKER_ARGS+=(-v "${DATA_DIR}:/var/lib/mysql")
fi

MYSQL_ARGS=(
  --port="${PORT}"
  --innodb_buffer_pool_size=1G
  --innodb_log_file_size=512M
  --innodb_io_capacity=5000
  --innodb_flush_method=O_DIRECT
)

if [[ "${FAST}" == "1" ]]; then
  MYSQL_ARGS+=(--innodb_flush_log_at_trx_commit=2 --sync_binlog=0)
else
  MYSQL_ARGS+=(--innodb_flush_log_at_trx_commit=1 --sync_binlog=1)
fi

if [[ "${BINLOG}" == "1" ]]; then
  MYSQL_ARGS+=(--binlog-expire-logs-seconds="${BINLOG_EXPIRE_SECONDS}" --max_binlog_size="${BINLOG_MAX_SIZE}")
else
  MYSQL_ARGS+=(--skip-log-bin)
fi

docker run "${DOCKER_ARGS[@]}" mysql:8.0.36 "${MYSQL_ARGS[@]}"

echo "MySQL started on host port ${PORT} (db=${DATABASE})."
