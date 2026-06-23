#!/usr/bin/env bash
# Reliability loop for the xaeroflux substrate tests: run the mesh + reliability suites N times,
# failing on the first red iteration. Offline only — no network required.
#
# Usage:  scripts/reliability.sh [N]      (default N=20)
set -euo pipefail

N="${1:-20}"
cd "$(dirname "$0")/.."

echo "Reliability: running substrate mesh + reliability suites ${N}x (fail on first red)…"
for i in $(seq 1 "$N"); do
    echo "──────────────────────────────────────────────────────────────"
    echo "iteration ${i}/${N}"
    if ! cargo test --test substrate_mesh --test substrate_reliability -- --test-threads=2; then
        echo "RED at iteration ${i}/${N}" >&2
        exit 1
    fi
done

echo "──────────────────────────────────────────────────────────────"
echo "All ${N} iterations GREEN."
