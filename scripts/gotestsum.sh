#!/usr/bin/env bash
# Shared gotestsum wrapper for Nx Go test targets.
#
# Usage:
#   bash "${NX_WORKSPACE_ROOT}/scripts/gotestsum.sh" [options] [--] [go test args...]
#
# Options:
#   --jsonfile PATH     Write gotestsum JSON events
#   --junitfile PATH    Write JUnit XML (--junitfile-hide-empty-pkg)
#   --junit-name NAME   --junitfile-project-name (default: $NX_TASK_TARGET_PROJECT)
#   --slowest DURATION  After the run, print tests slower than DURATION
#                       from --jsonfile (requires --jsonfile)
#   --live PKG          Locally: build PKG to tmp/gotestlive and pipe
#                       standard-json into it. Ignored when GITHUB_ACTIONS is set.
#
# Env:
#   GOTESTSUM_FORMAT        default pkgname (CI sets github-actions)
#   GITHUB_ACTIONS          if unset, --hide-summary=skipped
#
# Always passes -count=1 (disables Go's test cache). A later -count wins.
set -euo pipefail

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \?//'
}

jsonfile=
junitfile=
junit_name="${NX_TASK_TARGET_PROJECT:-}"
slowest=
live=
gotest_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --jsonfile)
      jsonfile="${2:?--jsonfile requires a path}"
      shift 2
      ;;
    --junitfile)
      junitfile="${2:?--junitfile requires a path}"
      shift 2
      ;;
    --junit-name)
      junit_name="${2:?--junit-name requires a name}"
      shift 2
      ;;
    --slowest)
      slowest="${2:?--slowest requires a duration (e.g. 10s)}"
      shift 2
      ;;
    --live)
      live="${2:?--live requires a Go package path}"
      shift 2
      ;;
    --)
      shift
      gotest_args+=("$@")
      break
      ;;
    *)
      gotest_args+=("$@")
      break
      ;;
  esac
done

if [[ -n "$slowest" && -z "$jsonfile" ]]; then
  echo "gotestsum.sh: --slowest requires --jsonfile" >&2
  exit 2
fi

in_ci=0
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  in_ci=1
fi

use_live=0
if [[ -n "$live" && "$in_ci" -eq 0 ]]; then
  use_live=1
fi

format="${GOTESTSUM_FORMAT:-pkgname}"
if [[ "$use_live" -eq 1 ]]; then
  format=standard-json
fi

gts_args=(
  --format "$format"
  --format-icons hivis
  --format-hide-empty-pkg
)
if [[ "$in_ci" -eq 0 ]]; then
  gts_args+=(--hide-summary=skipped)
fi
if [[ -n "$jsonfile" ]]; then
  mkdir -p "$(dirname "$jsonfile")"
  gts_args+=(--jsonfile "$jsonfile")
fi
if [[ -n "$junitfile" ]]; then
  mkdir -p "$(dirname "$junitfile")"
  gts_args+=(--junitfile "$junitfile" --junitfile-hide-empty-pkg)
  if [[ -n "$junit_name" ]]; then
    gts_args+=(--junitfile-project-name "$junit_name")
  fi
fi
gts_args+=(-- -count=1 "${gotest_args[@]}")

print_slowest() {
  local file="$1"
  local threshold="$2"
  if [[ ! -f "$file" ]]; then
    return 0
  fi
  local slow
  slow="$(go tool gotestsum tool slowest --jsonfile "$file" --threshold "$threshold" || true)"
  if [[ -n "$slow" ]]; then
    echo
    echo "Slowest tests (≥ ${threshold})"
    echo "$slow"
  fi
}

status=0
if [[ "$use_live" -eq 1 ]]; then
  mkdir -p tmp
  go build -o tmp/gotestlive "$live"
  go tool gotestsum "${gts_args[@]}" | ./tmp/gotestlive || status=$?
else
  go tool gotestsum "${gts_args[@]}" || status=$?
fi

if [[ -n "$slowest" ]]; then
  print_slowest "$jsonfile" "$slowest"
fi
exit "$status"
