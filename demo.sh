#!/bin/sh
# Analyzes the degraded snapshot series (growing lag -> exit 2), then the healthy series.
set -u
export PYTHONPATH="$(dirname "$0")/src"

echo "=== Analyzing samples/lag-snapshots-degraded.json ==="
python3 -m oci_streaming_lag_monitor samples/lag-snapshots-degraded.json --threshold 10000
echo "exit code: $? (2 = a group is falling behind unbounded, expected for the seeded series)"
echo
echo "=== Analyzing samples/lag-snapshots-healthy.json ==="
python3 -m oci_streaming_lag_monitor samples/lag-snapshots-healthy.json --threshold 10000
echo "exit code: $? (0 = no group falling behind, expected for the healthy series)"
