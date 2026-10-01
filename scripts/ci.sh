#!/usr/bin/env bash
# Local and CI gate runner for smppflow.
#
# smppflow depends on github.com/oarkflow/broker via a `replace` directive
# pointing at a sibling checkout (../broker). This script verifies that
# dependency is present before doing any work so failures are actionable
# instead of an obscure module-resolution error.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER_DIR="${SMPPFLOW_BROKER_DIR:-$(cd "$ROOT/.." && pwd)/broker}"
MODE="${1:-all}"

if [[ ! -f "$BROKER_DIR/go.mod" ]]; then
  cat >&2 <<EOF
error: sibling broker module not found at: $BROKER_DIR

smppflow builds against github.com/oarkflow/broker through go.mod's
    replace github.com/oarkflow/broker => ../broker

Clone the broker repository next to smppflow:

    git clone <broker-remote> "$BROKER_DIR"

Or point at an existing checkout:

    SMPPFLOW_BROKER_DIR=/path/to/broker bash scripts/ci.sh
EOF
  exit 2
fi

cd "$ROOT"

run_fmt() {
  local out
  out="$(gofmt -l -- $(git ls-files '*.go' 2>/dev/null || find . -name '*.go' -not -path './.git/*'))"
  if [[ -n "$out" ]]; then
    echo "gofmt found unformatted files:" >&2
    echo "$out" >&2
    exit 1
  fi
  echo "gofmt: ok"
}

run_vet() {
  go vet ./...
  echo "go vet: ok"
}

run_test() {
  go test -count=1 ./...
  echo "go test: ok"
}

run_race() {
  go test -race -count=1 ./...
  echo "go test -race: ok"
}

run_vuln() {
  if ! command -v govulncheck >/dev/null 2>&1; then
    echo "govulncheck: not installed, skipping (go install golang.org/x/vuln/cmd/govulncheck@latest)" >&2
    return 0
  fi
  govulncheck ./...
  echo "govulncheck: ok"
}

run_build() {
  CGO_ENABLED=0 go build -trimpath -buildvcs=false ./...
  echo "go build: ok"
}

case "$MODE" in
  fmt) run_fmt ;;
  vet) run_vet ;;
  test) run_test ;;
  race) run_race ;;
  vuln) run_vuln ;;
  build) run_build ;;
  all)
    run_fmt
    run_vet
    run_test
    run_race
    run_build
    run_vuln
    ;;
  *)
    echo "usage: $0 [fmt|vet|test|race|vuln|build|all]" >&2
    exit 2
    ;;
esac
