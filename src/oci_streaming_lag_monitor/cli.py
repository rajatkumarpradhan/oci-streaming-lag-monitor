"""Command line interface for the streaming lag analyzer."""

import argparse
import json
import sys

from .lag import audit_reports, build_reports

EXIT_HIGH_FINDINGS = 2
EXIT_INPUT_ERROR = 1
EXIT_OK = 0


def _load(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc


def _render(reports, findings, threshold):
    lines = []
    for report in reports:
        if report.drain_per_snapshot > 0:
            trend = f"draining {report.drain_per_snapshot:.0f}/snapshot"
        elif report.drain_per_snapshot < 0:
            trend = f"growing {-report.drain_per_snapshot:.0f}/snapshot"
        else:
            trend = "flat"
        eta = f", eta ~{report.eta_snapshots:.0f} snapshots" if report.eta_snapshots else ""
        lines.append(
            f"group {report.group}: lag {report.current_lag} over {report.snapshots} snapshot(s), {trend}{eta}"
        )
    for finding in findings:
        lines.append(f"[{finding.severity:<6}] {finding.rule_id} {finding.group}: {finding.message}")
    high = sum(1 for f in findings if f.severity == "HIGH")
    lines.append(
        f"summary: {len(reports)} group(s), {len(findings)} finding(s), {high} growing unbounded (threshold {threshold})"
    )
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="oci-streaming-lag-monitor",
        description="Offline lag analysis for OCI Streaming consumer snapshots.",
    )
    parser.add_argument("path", help="lag snapshot series JSON")
    parser.add_argument("--threshold", type=int, default=10000,
                        help="lag level considered a backlog even when draining (default 10000)")
    args = parser.parse_args(argv)

    try:
        reports = build_reports(_load(args.path))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    findings = audit_reports(reports, args.threshold)
    print(_render(reports, findings, args.threshold))
    if any(f.severity == "HIGH" for f in findings):
        return EXIT_HIGH_FINDINGS
    return EXIT_OK
