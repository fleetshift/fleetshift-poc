#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-clock-drift.sh [--machine NAME] [--threshold SECONDS]

Checks clock drift between the host and a Podman machine.

Options:
  -m, --machine NAME       Podman machine name. Defaults to the current
                           default machine, then podman-machine-default.
  -t, --threshold SECONDS  Exit non-zero when absolute drift exceeds this
                           many seconds. Default: 5.
  -h, --help               Show this help.

Environment:
  PODMAN_MACHINE_NAME                    Default machine name override.
  PODMAN_CLOCK_DRIFT_THRESHOLD_SECONDS   Default threshold override.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

host_epoch_ms() {
  if command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000'
  else
    printf '%s000\n' "$(date -u +%s)"
  fi
}

format_ms() {
  local ms="$1"
  local sign=""

  if [ "$ms" -lt 0 ]; then
    sign="-"
    ms=$((-ms))
  fi

  printf '%s%d.%03ds' "$sign" "$((ms / 1000))" "$((ms % 1000))"
}

resolve_default_machine() {
  local name

  if name=$(podman machine inspect --format '{{.Name}}' 2>/dev/null); then
    name=$(printf '%s\n' "$name" | awk 'NF { print $1; exit }')
    if [ -n "$name" ]; then
      printf '%s\n' "$name"
      return
    fi
  fi

  printf 'podman-machine-default\n'
}

machine_epoch_ms() {
  local machine="$1"
  local output
  local remote_date_cmd

  remote_date_cmd='ms=$(date -u +%s%3N 2>/dev/null || true); case "$ms" in ""|*N*) printf "%s000\n" "$(date -u +%s)" ;; *) printf "%s\n" "$ms" ;; esac'

  if ! output=$(podman machine ssh "$machine" "$remote_date_cmd" 2>&1); then
    printf '%s\n' "$output" >&2
    return 1
  fi

  printf '%s\n' "$output" | awk '/^[0-9]+$/ { value = $1 } END { if (value != "") print value; else exit 1 }'
}

machine="${PODMAN_MACHINE_NAME:-}"
threshold_seconds="${PODMAN_CLOCK_DRIFT_THRESHOLD_SECONDS:-5}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    -m|--machine)
      [ "$#" -ge 2 ] || die "$1 requires a machine name"
      machine="$2"
      shift 2
      ;;
    -t|--threshold)
      [ "$#" -ge 2 ] || die "$1 requires a number of seconds"
      threshold_seconds="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$threshold_seconds" in
  ''|*[!0-9]*) die "threshold must be a non-negative integer number of seconds" ;;
esac

command -v podman >/dev/null 2>&1 || die "podman is not installed"

if [ -z "$machine" ]; then
  machine=$(resolve_default_machine)
fi

if ! inspect_error=$(podman machine inspect "$machine" 2>&1 >/dev/null); then
  die "could not inspect Podman machine '$machine': $inspect_error"
fi

before_ms=$(host_epoch_ms)
if ! podman_ms=$(machine_epoch_ms "$machine"); then
  die "could not read clock from Podman machine '$machine'. Is it running? Try: podman machine start $machine"
fi
after_ms=$(host_epoch_ms)

mid_ms=$(((before_ms + after_ms) / 2))
round_trip_ms=$((after_ms - before_ms))
drift_ms=$((podman_ms - mid_ms))
abs_drift_ms="$drift_ms"
if [ "$abs_drift_ms" -lt 0 ]; then
  abs_drift_ms=$((-abs_drift_ms))
fi

threshold_ms=$((threshold_seconds * 1000))

printf 'Podman machine: %s\n' "$machine"
printf 'Round trip:     %s\n' "$(format_ms "$round_trip_ms")"
printf 'Drift:          '
if [ "$drift_ms" -gt 0 ]; then
  printf '+%s (machine ahead of host)\n' "$(format_ms "$drift_ms")"
elif [ "$drift_ms" -lt 0 ]; then
  printf '%s (machine behind host)\n' "$(format_ms "$drift_ms")"
else
  printf '0.000s\n'
fi

if [ "$abs_drift_ms" -gt "$threshold_ms" ]; then
  printf 'Status:         FAIL (threshold: %s)\n' "$(format_ms "$threshold_ms")"
  exit 1
fi

printf 'Status:         OK (threshold: %s)\n' "$(format_ms "$threshold_ms")"
