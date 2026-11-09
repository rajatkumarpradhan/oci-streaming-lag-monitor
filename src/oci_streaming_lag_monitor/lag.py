"""Lag analysis for simplified OCI Streaming consumer-group snapshots.

OCI Streaming is Kafka-compatible: consumers commit offsets, and the gap
between the partition's latest offset and the committed offset is the lag.
This module works on a static JSON series of lag snapshots per consumer
group: it computes the current lag, the drain rate from observed history,
an ETA to zero lag, and stuck/unbounded-growth signals. All analysis is
offline; no OCI calls.
"""

from dataclasses import dataclass, field

SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: str
    group: str
    message: str


@dataclass
class GroupReport:
    group: str
    current_lag: int = 0
    snapshots: int = 0
    drain_per_snapshot: float = 0.0  # negative means lag is growing
    eta_snapshots: float = None      # None when not draining


def _parse_snapshot(raw, group):
    if not isinstance(raw, dict):
        raise ValueError(f"group {group} has a non-object snapshot entry")
    try:
        lag = int(raw["lag"])
        ts = raw["timestamp"]
    except KeyError as exc:
        raise ValueError(f"group {group} snapshot missing required field: {exc}") from exc
    if lag < 0:
        raise ValueError(f"group {group} has negative lag {lag}")
    return ts, lag


def build_reports(document):
    """Parse the snapshot series into per-group reports."""
    if not isinstance(document, dict) or not isinstance(document.get("consumer_groups"), list):
        raise ValueError("document must contain a top-level 'consumer_groups' list")
    reports = []
    for raw_group in document["consumer_groups"]:
        if not isinstance(raw_group, dict):
            raise ValueError("each consumer group entry must be an object")
        name = raw_group.get("name")
        snapshots = raw_group.get("snapshots")
        if not name or not isinstance(snapshots, list):
            raise ValueError("consumer group entry needs 'name' and a 'snapshots' list")
        series = [_parse_snapshot(s, name) for s in snapshots]
        report = GroupReport(group=str(name), snapshots=len(series))
        if series:
            report.current_lag = series[-1][1]
        if len(series) >= 2:
            first_lag, last_lag = series[0][1], series[-1][1]
            report.drain_per_snapshot = (first_lag - last_lag) / (len(series) - 1)
            if report.drain_per_snapshot > 0 and last_lag > 0:
                report.eta_snapshots = last_lag / report.drain_per_snapshot
        reports.append(report)
    return reports


def audit_reports(reports, lag_threshold):
    """Apply alert rules to group reports."""
    findings = []
    for report in reports:
        if report.snapshots < 2:
            findings.append(Finding("LAG004", "LOW", report.group,
                                    "fewer than two snapshots; trend cannot be assessed"))
            continue
        if report.drain_per_snapshot < 0 and report.current_lag > 0:
            findings.append(Finding(
                "LAG001", "HIGH", report.group,
                f"lag is growing by {-report.drain_per_snapshot:.0f} msg/snapshot and stands at {report.current_lag}; consumers are falling behind",
            ))
        elif report.current_lag >= lag_threshold:
            eta = f"~{report.eta_snapshots:.0f} snapshots to drain" if report.eta_snapshots else "not draining"
            findings.append(Finding(
                "LAG002", "MEDIUM", report.group,
                f"lag {report.current_lag} is at/over the {lag_threshold} threshold ({eta})",
            ))
        if report.current_lag > 0 and report.drain_per_snapshot == 0:
            findings.append(Finding(
                "LAG003", "MEDIUM", report.group,
                f"lag is flat at {report.current_lag}; the consumer group appears stuck",
            ))
    return sorted(findings, key=lambda f: (SEVERITY_ORDER[f.severity], f.rule_id, f.group, f.message))
