"""Run with an installed candidate and this matching source example.

From a separate working directory:
    python -I -B /absolute/source/examples/regression/test_host.py

The tests own fresh temporary recordings. The suite catches expected negative
cases and reports whether each was detected.
"""
from pathlib import Path
import runpy
import tempfile
import unittest

from mcp_flightrecorder.testing import EvidenceReadError, assert_evidence


# Load the sibling host example. The product comes from the installed package.
run_host = runpy.run_path(str(Path(__file__).with_name("host.py")))["run_host"]


class HostRegression(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="mfr-regression-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "recording.sqlite"

    def test_faithful_evidence(self):
        run_host(self.path)
        result = assert_evidence(self.path)
        self.assertEqual(result["data"]["excluded_count"], 0)

    def test_faithful_application(self):
        # Kept separate: an evidence failure cannot prevent this independent check.
        observed = run_host(self.path)
        self.assertEqual(observed, ("42",))

    def test_undeclared_mutation_fails_both_assertions(self):
        observed = run_host(self.path, scenario="mutated")
        with self.subTest(check="evidence"):
            with self.assertRaisesRegex(AssertionError, "VERDICT: FAIL"):
                assert_evidence(self.path)
        with self.subTest(check="application"):
            with self.assertRaises(AssertionError):
                self.assertEqual(observed, ("42",))

    def test_declared_preparation_can_pass_evidence_and_fail_application(self):
        observed = run_host(self.path, scenario="prepared_wrong")
        with self.subTest(check="evidence"):
            assert_evidence(self.path)
        # The independent application requirement remains unchanged.
        with self.subTest(check="application"):
            with self.assertRaises(AssertionError):
                self.assertEqual(observed, ("42",))

    def test_mixed_scope_is_intentional_and_does_not_verify_local_forwarding(self):
        observed = run_host(self.path, scenario="mixed")
        with self.subTest(check="default evidence"):
            with self.assertRaisesRegex(AssertionError, "VERDICT: UNKNOWN"):
                assert_evidence(self.path)
        with self.subTest(check="intentionally narrowed evidence"):
            result = assert_evidence(self.path, allow_local_tools=True)
            self.assertEqual(result["data"]["excluded_count"], 1)
            self.assertEqual(result["data"]["expectations"]["answers"], "mcp")
        with self.subTest(check="application"):
            self.assertEqual(observed, ("42", "local value"))

    def test_unreadable_recording_is_not_an_application_verdict(self):
        # No host is executed and no application assertion is made in this case.
        with self.assertRaises(EvidenceReadError) as raised:
            assert_evidence(self.path)
        self.assertEqual(raised.exception.reason, "storage_failure")
        self.assertFalse(self.path.exists())

    def test_local_scope_does_not_excuse_missing_mcp_result(self):
        observed = run_host(self.path, scenario="missing_result")
        with self.subTest(check="evidence"):
            with self.assertRaisesRegex(AssertionError, "VERDICT: UNKNOWN"):
                assert_evidence(self.path, allow_local_tools=True)
        with self.subTest(check="application"):
            self.assertEqual(observed, ("42",))

    def test_local_scope_does_not_excuse_shared_decision_ambiguity(self):
        observed = run_host(self.path, scenario="ambiguous")
        with self.subTest(check="evidence"):
            with self.assertRaisesRegex(AssertionError, "VERDICT: UNKNOWN"):
                assert_evidence(self.path, allow_local_tools=True)
        with self.subTest(check="application"):
            self.assertEqual(observed, ("42", "local value"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
