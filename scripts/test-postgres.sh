#!/usr/bin/env bash
set -euo pipefail

_AFDATA_BASH_SOURCE="$("${AFDATA_BIN:-afdata}" shell bash)"
source /dev/stdin <<<"$_AFDATA_BASH_SOURCE"
unset _AFDATA_BASH_SOURCE

afdata_args_begin "test-postgres.sh [OPTIONS]"
afdata_args_parse "$@"

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
container_id=""
cleanup() {
  if [ -n "$container_id" ]; then
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ -z "${SYNAPSE_TEST_POSTGRES_URL_SECRET:-}" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    afdata_error postgres_fixture_unavailable "Docker or SYNAPSE_TEST_POSTGRES_URL_SECRET is required"
    exit 1
  fi
  afdata_run --quiet docker info
  afdata_log info "Starting disposable PostgreSQL 18 fixture"
  container_id="$(docker run -d --rm --publish 127.0.0.1::5432 --env POSTGRES_HOST_AUTH_METHOD=trust postgres:18)"
  ready=false
  for _ in {1..60}; do
    if docker exec "$container_id" pg_isready -U postgres >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    afdata_error postgres_fixture_unavailable "PostgreSQL fixture did not become ready"
    exit 1
  fi
  port="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")"
  export SYNAPSE_TEST_POSTGRES_URL_SECRET="postgres://postgres@127.0.0.1:$port/postgres"
fi

afdata_log info "Running isolated PostgreSQL storage and crawler regressions"
afdata_run cargo test --all-features postgres_contract -- --ignored --test-threads=1
afdata_result "PostgreSQL regressions passed"
