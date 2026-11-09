import io
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout

from oci_streaming_lag_monitor.cli import main

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCli(unittest.TestCase):
    def test_missing_file_exits_1(self):
        with redirect_stderr(io.StringIO()):
            self.assertEqual(main(["/nonexistent/lag.json"]), 1)

    def test_invalid_json_exits_1(self):
        with redirect_stderr(io.StringIO()):
            self.assertEqual(main([os.path.join(ROOT, "pyproject.toml")]), 1)

    def test_degraded_sample_exits_2(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = main([os.path.join(ROOT, "samples", "lag-snapshots-degraded.json")])
        self.assertEqual(code, 2)
        text = out.getvalue()
        self.assertIn("LAG001 payments-writer", text)
        self.assertIn("LAG002 clickstream-loader", text)
        self.assertIn("LAG003 audit-shipper", text)
        self.assertIn("metrics-rollup: lag 40", text)
        self.assertIn("summary:", text)

    def test_healthy_sample_exits_0(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = main([os.path.join(ROOT, "samples", "lag-snapshots-healthy.json")])
        self.assertEqual(code, 0)
        self.assertIn("0 growing", out.getvalue())


if __name__ == "__main__":
    unittest.main()
