# oci-streaming-lag-monitor

Offline lag analyzer for **simplified OCI Streaming consumer-group snapshots**. OCI Streaming is Kafka-compatible: consumers commit offsets and the gap to the partition's latest offset is the lag. A group whose lag grows snapshot over snapshot is falling behind; one that is flat and non-zero is stuck. This tool reads a static snapshot series and calls both out, with drain rates and drain ETAs.

## What it is

A command-line watch tool for lag-as-data. Point it at a snapshot series JSON, get per-group trends, findings, and an exit code suitable for CI gates or scheduled checks.

- Runs 100% locally. No OCI SDK, no credentials, no network calls.
- Four rules (LAG001-LAG004), each unit-tested on positive and negative inputs.
- Exit code 2 when any group is falling behind unbounded (HIGH); 0 otherwise; 1 on unreadable or malformed input.

## What it is NOT (honest limits)

- The input schema is a **simplified approximation**: one lag number per group per snapshot. Real lag is per-partition; per-partition hotspots, rebalances, and consumer membership are not modeled.
- Drain rate is a straight line between the first and last snapshot; bursty traffic makes the ETA optimistic or pessimistic. It does not fetch offsets from a stream.
- The backlog threshold (default 10000, `--threshold`) is a local convention; the right value is workload-specific.
- Snapshots carry no per-broker metadata; clock skew between snapshot producers is not accounted for.

## Input schema

```json
{
  "consumer_groups": [
    {
      "name": "payments-writer",
      "snapshots": [
        {"timestamp": "2026-11-09T18:00:00Z", "lag": 1200},
        {"timestamp": "2026-11-09T18:30:00Z", "lag": 2600}
      ]
    }
  ]
}
```

## Rules

| Rule | Severity | Finding |
| --- | --- | --- |
| LAG001 | HIGH | Lag growing snapshot over snapshot; consumers are falling behind |
| LAG002 | MEDIUM | Draining, but current lag still at/over the threshold (with ETA) |
| LAG003 | MEDIUM | Lag flat and non-zero; consumer group appears stuck |
| LAG004 | LOW | Fewer than two snapshots; trend cannot be assessed |

## Run

Requires Python 3.10+, no third-party dependencies.

```sh
PYTHONPATH=src python3 -m oci_streaming_lag_monitor samples/lag-snapshots-degraded.json --threshold 10000
PYTHONPATH=src python3 -m oci_streaming_lag_monitor samples/lag-snapshots-healthy.json
```

Or the end-to-end demo (degraded series -> healthy series, with expected exit codes):

```sh
sh demo.sh
```

Expected demo output: the degraded series flags payments-writer growing unbounded (HIGH), clickstream-loader above threshold with a drain ETA, audit-shipper stuck flat, and exits 2; the healthy series reports zero findings and exits 0.

## Test

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

19 unit tests cover series parsing, drain-rate and ETA math, every rule on positive and negative inputs, sort order, malformed-input handling, and CLI exit codes. CI runs the same tests plus the demo on Python 3.10/3.11/3.12.
