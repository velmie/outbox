#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES=(. mysql cmd)

export GOTOOLCHAIN=go1.26.7
export GOWORK="${ROOT_DIR}/go.work"

for command in go git tar zip docker golangci-lint govulncheck trivy; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

if [[ "$(go env GOVERSION)" != "go1.26.7" ]]; then
  echo "release gate requires Go 1.26.7" >&2
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

echo "==> repository: HIGH/CRITICAL Go dependency vulnerabilities"
trivy fs --scanners vuln --severity HIGH,CRITICAL --exit-code 1 "${ROOT_DIR}"
