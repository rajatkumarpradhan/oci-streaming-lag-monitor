import unittest

from oci_streaming_lag_monitor.lag import audit_reports, build_reports


def group(name, lags):
    return {"name": name, "snapshots": [
        {"timestamp": f"2026-11-09T18:{i:02d}:00Z", "lag": lag} for i, lag in enumerate(lags)
    ]}


def doc(groups):
    return {"consumer_groups": groups}


class TestBuildReports(unittest.TestCase):
    def test_valid_series_parses(self):
        reports = build_reports(doc([group("g", [100, 50, 0])]))
        self.assertEqual(reports[0].current_lag, 0)
        self.assertEqual(reports[0].snapshots, 3)

    def test_drain_rate_computed(self):
        reports = build_reports(doc([group("g", [100, 50, 0])]))
        self.assertAlmostEqual(reports[0].drain_per_snapshot, 50.0)
        self.assertIsNone(reports[0].eta_snapshots)  # already at zero

    def test_eta_computed_when_draining_nonzero(self):
        reports = build_reports(doc([group("g", [300, 200, 100])]))
        self.assertAlmostEqual(reports[0].eta_snapshots, 1.0)

    def test_growing_rate_negative(self):
        reports = build_reports(doc([group("g", [100, 200, 300])]))
        self.assertLess(reports[0].drain_per_snapshot, 0)

    def test_missing_list_rejected(self):
        with self.assertRaises(ValueError):
            build_reports({"groups": []})

    def test_missing_field_rejected(self):
        with self.assertRaises(ValueError):
            build_reports(doc([{"name": "g", "snapshots": [{"lag": 5}]}]))

    def test_negative_lag_rejected(self):
        with self.assertRaises(ValueError):
            build_reports(doc([group("g", [-5, 0])]))


class TestAuditReports(unittest.TestCase):
    def test_lag001_growing(self):
        findings = audit_reports(build_reports(doc([group("g", [100, 500, 900])])), 10000)
        self.assertEqual(findings[0].rule_id, "LAG001")
        self.assertEqual(findings[0].severity, "HIGH")

    def test_lag002_backlog_above_threshold(self):
        findings = audit_reports(build_reports(doc([group("g", [30000, 25000, 20000])])), 10000)
        self.assertEqual(findings[0].rule_id, "LAG002")
        self.assertEqual(findings[0].severity, "MEDIUM")

    def test_lag002_not_triggered_below_threshold(self):
        findings = audit_reports(build_reports(doc([group("g", [9000, 7000, 5000])])), 10000)
        self.assertEqual(findings, [])

    def test_lag003_stuck(self):
        findings = audit_reports(build_reports(doc([group("g", [400, 400, 400])])), 10000)
        self.assertEqual(findings[0].rule_id, "LAG003")

    def test_lag003_not_when_zero_lag(self):
        findings = audit_reports(build_reports(doc([group("g", [0, 0, 0])])), 10000)
        self.assertEqual(findings, [])

    def test_lag004_single_snapshot(self):
        findings = audit_reports(build_reports(doc([group("g", [100])])), 10000)
        self.assertEqual(findings[0].rule_id, "LAG004")

    def test_healthy_group_no_findings(self):
        findings = audit_reports(build_reports(doc([group("g", [900, 600, 300, 50])])), 10000)
        self.assertEqual(findings, [])

    def test_findings_sorted_high_first(self):
        reports = build_reports(doc([
            group("growing", [100, 500, 900]),
            group("stuck", [400, 400, 400]),
        ]))
        findings = audit_reports(reports, 10000)
        self.assertEqual(findings[0].severity, "HIGH")
        self.assertEqual(findings[-1].severity, "MEDIUM")


if __name__ == "__main__":
    unittest.main()
