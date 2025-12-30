#!/usr/bin/env bash
set -euo pipefail

NAME="${NAME:-outbox-mysql-bench}"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${NAME}\$"; then
  echo "Container ${NAME} not found."
  exit 0
fi

docker stop "${NAME}"
