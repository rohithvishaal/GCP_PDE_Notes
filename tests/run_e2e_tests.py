#!/usr/bin/env python3
"""
Automated Master E2E Test Runner for GCP PDE Handbook Audit and Remediation.

Usage:
    python tests/run_e2e_tests.py
    python tests/run_e2e_tests.py --tier 1
    python tests/run_e2e_tests.py --tier 2
    python tests/run_e2e_tests.py --tier 3
    python tests/run_e2e_tests.py --tier 4
    python tests/run_e2e_tests.py --report custom_report.json
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import unittest
from typing import Any, Dict, List

# Ensure tests package and workspace root are in sys.path
TESTS_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = TESTS_DIR.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

# Tier Suite Mappings
TIER_SUITES = {
    "1": ("Tier 1 (Feature Coverage)", "tests.test_tier1_feature_coverage"),
    "2": ("Tier 2 (Boundary & Corner Cases)", "tests.test_tier2_boundary_corner"),
    "3": ("Tier 3 (Cross-Feature & Traceability)", "tests.test_tier3_traceability"),
    "4": ("Tier 4 (Real-World Execution Scenarios)", "tests.test_tier4_execution_scenarios"),
    "5": ("Tier 5 (Adversarial Hardening)", "tests.test_tier5_adversarial_hardening"),
}


class DetailedResultCollector(unittest.TestResult):
    """Custom TestResult that captures structured execution records per test."""

    def __init__(self, tier_label: str):
        super().__init__()
        self.tier_label = tier_label
        self.records: List[Dict[str, Any]] = []
        self._start_time: float = 0.0

    def startTest(self, test: unittest.TestCase):
        super().startTest(test)
        self._start_time = time.time()

    def addSuccess(self, test: unittest.TestCase):
        super().addSuccess(test)
        duration = time.time() - self._start_time
        doc = (test.shortDescription() or "").strip()
        self.records.append({
            "test_id": test.id(),
            "tier": self.tier_label,
            "status": "PASS",
            "description": doc or test._testMethodName,
            "duration_sec": round(duration, 4),
            "details": "",
        })

    def addFailure(self, test: unittest.TestCase, err):
        super().addFailure(test, err)
        duration = time.time() - self._start_time
        doc = (test.shortDescription() or "").strip()
        self.records.append({
            "test_id": test.id(),
            "tier": self.tier_label,
            "status": "FAIL",
            "description": doc or test._testMethodName,
            "duration_sec": round(duration, 4),
            "details": self._exc_info_to_string(err, test),
        })

    def addError(self, test: unittest.TestCase, err):
        super().addError(test, err)
        duration = time.time() - self._start_time
        doc = (test.shortDescription() or "").strip()
        self.records.append({
            "test_id": test.id(),
            "tier": self.tier_label,
            "status": "ERROR",
            "description": doc or test._testMethodName,
            "duration_sec": round(duration, 4),
            "details": self._exc_info_to_string(err, test),
        })

    def addSkip(self, test: unittest.TestCase, reason: str):
        super().addSkip(test, reason)
        duration = time.time() - self._start_time
        doc = (test.shortDescription() or "").strip()
        self.records.append({
            "test_id": test.id(),
            "tier": self.tier_label,
            "status": "SKIP",
            "description": doc or test._testMethodName,
            "duration_sec": round(duration, 4),
            "details": reason,
        })


def run_tier(tier_key: str, tier_info: tuple, verbose: bool = False) -> DetailedResultCollector:
    """Executes a single test tier and returns the result collector."""
    label, module_name = tier_info
    collector = DetailedResultCollector(label)
    loader = unittest.TestLoader()

    try:
        suite = loader.loadTestsFromName(module_name)
    except Exception as exc:
        collector.records.append({
            "test_id": f"{module_name}.LOAD_FAILURE",
            "tier": label,
            "status": "ERROR",
            "description": f"Failed to load suite {module_name}",
            "duration_sec": 0.0,
            "details": str(exc),
        })
        return collector

    suite.run(collector)
    return collector


def main():
    parser = argparse.ArgumentParser(
        description="GCP PDE Handbook Automated E2E Test Runner",
    )
    parser.add_argument(
        "--tier",
        choices=["1", "2", "3", "4", "5", "all"],
        default="all",
        help="Select specific test tier (1-5) or 'all' (default: all)",
    )
    parser.add_argument(
        "--report",
        default=str(TESTS_DIR / "e2e_test_report.json"),
        help="Path for machine-readable JSON report output",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()

    selected_tiers = (
        TIER_SUITES
        if args.tier == "all"
        else {args.tier: TIER_SUITES[args.tier]}
    )

    print("=" * 80)
    print(" GCP PROFESSIONAL DATA ENGINEER HANDBOOK - E2E TEST RUNNER")
    print(f" Execution Mode: Tier(s) {args.tier.upper()} | Python {sys.version.split()[0]}")
    print(f" Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)

    all_records: List[Dict[str, Any]] = []
    tier_stats: Dict[str, Dict[str, int]] = {}

    start_total_time = time.time()

    for t_key, t_info in selected_tiers.items():
        label, _ = t_info
        print(f"\n>>> Running {label}...")
        collector = run_tier(t_key, t_info, verbose=args.verbose)
        
        passed = sum(1 for r in collector.records if r["status"] == "PASS")
        failed = sum(1 for r in collector.records if r["status"] == "FAIL")
        errors = sum(1 for r in collector.records if r["status"] == "ERROR")
        skipped = sum(1 for r in collector.records if r["status"] == "SKIP")
        total = len(collector.records)

        tier_stats[f"tier_{t_key}"] = {
            "name": label,
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "skipped": skipped,
        }
        all_records.extend(collector.records)

        for r in collector.records:
            status_tag = f"[{r['status']}]"
            short_id = r["test_id"].split(".")[-1]
            desc = r["description"] or short_id
            print(f"  {status_tag:<8} {short_id:<32} {desc[:40]}")

    total_time = time.time() - start_total_time
    total_tests = len(all_records)
    total_passed = sum(1 for r in all_records if r["status"] == "PASS")
    total_failed = sum(1 for r in all_records if r["status"] == "FAIL")
    total_errors = sum(1 for r in all_records if r["status"] == "ERROR")
    total_skipped = sum(1 for r in all_records if r["status"] == "SKIP")
    pass_pct = (total_passed / total_tests * 100.0) if total_tests > 0 else 0.0

    print("\n" + "=" * 80)
    print(" E2E TEST EXECUTION SUMMARY")
    print("=" * 80)
    for t_key, stats in tier_stats.items():
        t_pass_pct = (stats["passed"] / stats["total"] * 100.0) if stats["total"] > 0 else 0.0
        print(
            f"  {stats['name']:<42}: "
            f"{stats['passed']:>2}/{stats['total']:<2} Passed "
            f"({t_pass_pct:>5.1f}%) | "
            f"Fail: {stats['failed']} | Err: {stats['errors']}"
        )
    print("-" * 80)
    print(
        f"  OVERALL TOTAL: {total_passed}/{total_tests} Passed "
        f"({pass_pct:.1f}%) | Failed: {total_failed} | Errors: {total_errors} | Skipped: {total_skipped}"
    )
    print(f"  Total Duration: {total_time:.2f} seconds")
    print("=" * 80)

    # Detailed failures and errors section
    failed_records = [r for r in all_records if r["status"] in ("FAIL", "ERROR")]
    if failed_records:
        print("\n" + "!" * 80)
        print(" FAILURE & DEFECT DIAGNOSTICS (EXPECTED DURING STAGE 1 BEFORE REMEDIATION)")
        print("!" * 80)
        for idx, rec in enumerate(failed_records, start=1):
            print(f"\n[{idx}] {rec['tier']} -> {rec['test_id']}")
            print(f"    Description: {rec['description']}")
            print("    Details:")
            for line in rec["details"].strip().splitlines()[-6:]:
                print(f"      {line}")
        print("!" * 80)

    # Write Machine-Readable JSON Report
    report_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_tests": total_tests,
        "passed": total_passed,
        "failed": total_failed,
        "errors": total_errors,
        "skipped": total_skipped,
        "pass_rate_percentage": round(pass_pct, 2),
        "total_duration_sec": round(total_time, 4),
        "tiers": tier_stats,
        "results": all_records,
    }

    report_path = Path(args.report)
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)
        print(f"\n[REPORT] Structured test report saved to: {report_path.resolve()}")
    except Exception as e:
        print(f"\n[WARNING] Could not save test report JSON: {e}")

    # Return non-zero exit code if failures exist
    sys.exit(0 if (total_failed == 0 and total_errors == 0) else 1)


if __name__ == "__main__":
    main()
