#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES=(. mysql cmd examples/delivery)

export GOTOOLCHAIN=go1.26.11

for command in go git tar zip docker golangci-lint govulncheck trivy; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

if [[ "$(go env GOVERSION)" != "go1.26.11" ]]; then
  echo "release gate requires Go 1.26.11" >&2
  exit 1
fi
if ! golangci-lint version | grep -Fq "version 2.12.2"; then
  echo "release gate requires golangci-lint 2.12.2" >&2
  exit 1
fi
if ! govulncheck -version | grep -Fq "Scanner: govulncheck@v1.7.0"; then
  echo "release gate requires govulncheck 1.7.0" >&2
  exit 1
fi
if ! trivy --version | grep -Fq "Version: 0.74.0"; then
  echo "release gate requires Trivy 0.74.0" >&2
  exit 1
fi

docker info >/dev/null 2>&1

workspace_dir="$(mktemp -d)"
trap 'rm -rf -- "${workspace_dir}"' EXIT
(
  cd "${workspace_dir}"
  GOWORK=off go work init "${ROOT_DIR}" "${ROOT_DIR}/mysql" "${ROOT_DIR}/cmd" "${ROOT_DIR}/examples/delivery"
)
export GOWORK="${workspace_dir}/go.work"
go work edit -go=1.26.11 \
  -replace="github.com/velmie/outbox@v0.6.0=${ROOT_DIR}" \
  -replace="github.com/velmie/outbox/mysql@v0.6.0=${ROOT_DIR}/mysql"

echo "==> release module graph"
bash "${ROOT_DIR}/scripts/verify-release-modules.sh"

for module in "${MODULES[@]}"; do
  module_dir="${ROOT_DIR}/${module}"
  echo "==> ${module}: unit, race, and vet"
  (cd "${module_dir}" && go test -mod=readonly -race ./... && go vet -mod=readonly ./...)

  echo "==> ${module}: lint"
  (cd "${module_dir}" && golangci-lint run --config "${ROOT_DIR}/.golangci.yml" ./...)

  echo "==> ${module}: reachable vulnerabilities"
  (cd "${module_dir}" && govulncheck ./... && govulncheck -tags=integration -test ./...)
done

echo "==> mysql: integration"
(cd "${ROOT_DIR}/mysql" && go test -count=1 -mod=readonly -tags=integration -timeout 12m ./...)

echo "==> cmd: integration"
(cd "${ROOT_DIR}/cmd" && go test -count=1 -mod=readonly -tags=integration -timeout 12m ./...)

echo "==> delivery example: integration"
(cd "${ROOT_DIR}/examples/delivery" && go test -count=1 -mod=readonly -tags=integration -timeout 12m ./...)

echo "==> repository: HIGH/CRITICAL Go dependency vulnerabilities"
trivy fs --scanners vuln --severity HIGH,CRITICAL --exit-code 1 "${ROOT_DIR}"
