#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="v0.2.0"

for command in go git tar zip; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

if [[ "$(GOWORK=off GOTOOLCHAIN=go1.26.7 go env GOVERSION)" != "go1.26.7" ]]; then
  echo "release module check requires Go 1.26.7" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  chmod -R u+w "${tmp_dir}" 2>/dev/null || true
  rm -rf -- "${tmp_dir}"
}
trap cleanup EXIT

proxy_dir="${tmp_dir}/proxy"
snapshot_dir="${tmp_dir}/snapshot"
git_common_dir="$(git -C "${ROOT_DIR}" rev-parse --path-format=absolute --git-common-dir)"

mkdir -p "${tmp_dir}/objects" "${snapshot_dir}" "${proxy_dir}" "${tmp_dir}/modcache"

export GIT_INDEX_FILE="${tmp_dir}/index"
export GIT_OBJECT_DIRECTORY="${tmp_dir}/objects"
export GIT_ALTERNATE_OBJECT_DIRECTORIES="${git_common_dir}/objects"
git -C "${ROOT_DIR}" read-tree HEAD
git -C "${ROOT_DIR}" add -A -- .
candidate_tree="$(git -C "${ROOT_DIR}" write-tree)"
git -C "${ROOT_DIR}" archive --format=tar "${candidate_tree}" | tar -xf - -C "${snapshot_dir}"
unset GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES

echo "candidate tree: ${candidate_tree}"

create_proxy_version() {
  local module_path="$1"
  local module_dir="$2"
  local proxy_version_dir="${proxy_dir}/${module_path}/@v"
  local stage_dir="${tmp_dir}/stage-${module_dir//\//_}"
  local prefix="${module_path}@${VERSION}"
  local module_file

  mkdir -p "${proxy_version_dir}" "${stage_dir}/${prefix}"
  if [[ "${module_dir}" == "." ]]; then
    tar -C "${snapshot_dir}" --exclude='./cmd' --exclude='./mysql' --exclude='./vendor' -cf - . \
      | tar -C "${stage_dir}/${prefix}" -xf -
    module_file="${snapshot_dir}/go.mod"
  else
    tar -C "${snapshot_dir}/${module_dir}" --exclude='./vendor' -cf - . \
      | tar -C "${stage_dir}/${prefix}" -xf -
    if [[ ! -f "${stage_dir}/${prefix}/LICENSE" ]]; then
      cp -- "${snapshot_dir}/LICENSE" "${stage_dir}/${prefix}/LICENSE"
    fi
    module_file="${snapshot_dir}/${module_dir}/go.mod"
  fi

  (
    cd "${stage_dir}"
    find "${prefix}" -type f -print | LC_ALL=C sort \
      | zip -q -X "${proxy_version_dir}/${VERSION}.zip" -@
  )
  cp -- "${module_file}" "${proxy_version_dir}/${VERSION}.mod"
  printf '{"Version":"%s","Time":"1970-01-01T00:00:00Z"}\n' "${VERSION}" \
    >"${proxy_version_dir}/${VERSION}.info"
  printf '%s\n' "${VERSION}" >"${proxy_version_dir}/list"
}

create_proxy_version "github.com/velmie/outbox" "."
create_proxy_version "github.com/velmie/outbox/mysql" "mysql"
create_proxy_version "github.com/velmie/outbox/cmd" "cmd"

mod_cache="${tmp_dir}/modcache"
bin_dir="${tmp_dir}/bin"
mkdir -p "${mod_cache}" "${bin_dir}"

upstream_proxy="$(GOWORK=off go env GOPROXY)"
if [[ -z "${upstream_proxy}" || "${upstream_proxy}" == "off" ]]; then
  upstream_proxy="https://proxy.golang.org"
fi

no_sumdb="github.com/velmie/outbox,github.com/velmie/outbox/*"
if [[ -n "${GONOSUMDB:-}" ]]; then
  no_sumdb="${no_sumdb},${GONOSUMDB}"
fi

release_env=(
  "GOMODCACHE=${mod_cache}"
  "GONOPROXY=none"
  "GONOSUMDB=${no_sumdb}"
  "GOPROXY=file://${proxy_dir},${upstream_proxy}"
  "GOTOOLCHAIN=go1.26.7"
  "GOWORK=off"
)

for module_path in \
  github.com/velmie/outbox \
  github.com/velmie/outbox/mysql \
  github.com/velmie/outbox/cmd; do
  env \
    "GOMODCACHE=${mod_cache}" \
    "GONOPROXY=none" \
    "GONOSUMDB=${no_sumdb}" \
    "GOPROXY=file://${proxy_dir}" \
    "GOTOOLCHAIN=go1.26.7" \
    "GOWORK=off" \
    go mod download "${module_path}@${VERSION}"
done

for module in . mysql cmd; do
  module_dir="${ROOT_DIR}/${module}"
  echo "==> ${module}: GOWORK=off release metadata and tests"
  (
    cd "${module_dir}"
    env "${release_env[@]}" go mod tidy -compat=1.25 -diff
    env "${release_env[@]}" go mod verify
    env "${release_env[@]}" go test -mod=readonly ./...
    env "${release_env[@]}" go vet -mod=readonly ./...
  )
done

mysql_graph="$(cd "${ROOT_DIR}/mysql" && env "${release_env[@]}" go list -m -f '{{.Path}} {{.Version}}' all)"
cmd_graph="$(cd "${ROOT_DIR}/cmd" && env "${release_env[@]}" go list -m -f '{{.Path}} {{.Version}}' all)"

grep -Fxq "github.com/velmie/outbox ${VERSION}" <<<"${mysql_graph}"
grep -Fxq "github.com/velmie/outbox ${VERSION}" <<<"${cmd_graph}"
grep -Fxq "github.com/velmie/outbox/mysql ${VERSION}" <<<"${cmd_graph}"

if grep -Eq '^[[:space:]]*replace([[:space:]]|$)' \
  "${ROOT_DIR}/go.mod" "${ROOT_DIR}/mysql/go.mod" "${ROOT_DIR}/cmd/go.mod"; then
  echo "release modules must not contain replace directives" >&2
  exit 1
fi

for command_path in outbox-bench outbox-cleanup outbox-partitions; do
  env "${release_env[@]}" "GOBIN=${bin_dir}" \
    go install "github.com/velmie/outbox/cmd/${command_path}@${VERSION}"
  test -x "${bin_dir}/${command_path}"
done
