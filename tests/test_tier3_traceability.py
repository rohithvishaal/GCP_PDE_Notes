"""
Tier 3: Cross-Feature & Traceability Test Suite.

Verifies:
- Consistency between docs/audits/EXAM_COVERAGE_MATRIX.md and 08_phase7_mock_exam_engine/exam_coverage_tracker.md
- Remediation Backlog P0/P1 Issue Traceability in CHANGELOG.md
- Official Documentation Citation Traceability in SOURCES.md
- Lab Coverage Consistency between Curriculum & Tracker
"""

import re
import unittest
from pathlib import Path
from typing import List, Set

from tests.conftest_base import (
    OFFICIAL_EXAM_OBJECTIVES,
    WORKSPACE_ROOT,
    read_text_safe,
)


class TestTier3Traceability(unittest.TestCase):
    """Tier 3: Cross-Feature & Traceability Verification."""

    def test_01_matrix_and_tracker_objectives_consistency(self):
        """Asserts consistency in 19 objectives and domain weights between tracker and matrix."""
        tracker_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "exam_coverage_tracker.md"
        matrix_path = WORKSPACE_ROOT / "docs" / "audits" / "EXAM_COVERAGE_MATRIX.md"

        self.assertTrue(tracker_path.is_file(), f"Tracker file {tracker_path} must exist.")

        tracker_content = read_text_safe(tracker_path)

        # Verify all 19 objectives appear in tracker
        missing_in_tracker = []
        for obj_id in OFFICIAL_EXAM_OBJECTIVES:
            if not re.search(rf'\b{re.escape(obj_id)}\b', tracker_content):
                missing_in_tracker.append(obj_id)

        self.assertEqual(
            missing_in_tracker,
            [],
            f"exam_coverage_tracker.md missing objectives: {missing_in_tracker}",
        )

        # Verify domain weights in tracker
        expected_weights = ["22%", "25%", "20%", "15%", "18%"]
        for weight in expected_weights:
            self.assertIn(
                weight,
                tracker_content,
                f"exam_coverage_tracker.md missing official domain weight {weight}",
            )

        # If EXAM_COVERAGE_MATRIX.md exists, cross-check mutual consistency
        if matrix_path.is_file():
            matrix_content = read_text_safe(matrix_path)
            for obj_id in OFFICIAL_EXAM_OBJECTIVES:
                self.assertIn(
                    obj_id,
                    matrix_content,
                    f"EXAM_COVERAGE_MATRIX.md missing objective {obj_id} present in tracker.",
                )
        else:
            self.fail(f"Stage 1 deliverable {matrix_path} does not exist yet for cross-check.")

    def test_02_remediation_backlog_p0_p1_logged_in_changelog(self):
        """Asserts all P0 and P1 issues from REMEDIATION_BACKLOG.md are logged in root CHANGELOG.md."""
        backlog_path = WORKSPACE_ROOT / "docs" / "audits" / "REMEDIATION_BACKLOG.md"
        changelog_path = WORKSPACE_ROOT / "CHANGELOG.md"

        self.assertTrue(
            backlog_path.is_file(),
            f"Remediation backlog {backlog_path} does not exist yet.",
        )
        self.assertTrue(
            changelog_path.is_file(),
            f"Root governance file {changelog_path} does not exist yet.",
        )

        backlog_content = read_text_safe(backlog_path)
        changelog_content = read_text_safe(changelog_path)

        # Extract P0 and P1 issues (pattern: ISSUE-P0-XX, ISSUE-P1-XX or P0/P1 headings/tags)
        p0_p1_ids: Set[str] = set(re.findall(r'\b(ISSUE-P[01]-\d+)\b', backlog_content, re.IGNORECASE))

        if not p0_p1_ids:
            # Fallback check: If tabular format with Priority column P0/P1
            # Search for item identifiers
            p0_p1_items = re.findall(r'\|\s*(P[01])\s*\|\s*([^\|]+)\|', backlog_content)
            self.assertTrue(
                len(p0_p1_items) > 0 or len(p0_p1_ids) > 0,
                "REMEDIATION_BACKLOG.md must contain explicit P0 and P1 priority items.",
            )

        # For any identified issue ID, check for presence in CHANGELOG.md
        unlogged_issues: List[str] = []
        for issue_id in p0_p1_ids:
            if issue_id.upper() not in changelog_content.upper():
                unlogged_issues.append(issue_id)

        self.assertEqual(
            unlogged_issues,
            [],
            f"The following P0/P1 issues are missing from CHANGELOG.md: {unlogged_issues}",
        )

    def test_03_official_sources_registry_traceability(self):
        """Asserts SOURCES.md catalogs official GCP documentation references with review metadata."""
        sources_path = WORKSPACE_ROOT / "SOURCES.md"
        self.assertTrue(
            sources_path.is_file(),
            f"Root governance file {sources_path} does not exist yet.",
        )

        content = read_text_safe(sources_path)
        # Check for presence of official Google Cloud doc links
        official_links = re.findall(r'https://cloud\.google\.com/[^\s\)\|\"\']+', content)
        self.assertGreaterEqual(
            len(official_links),
            5,
            f"SOURCES.md must catalog official documentation URLs, found {len(official_links)}",
        )

        # Check for review metadata / dates
        has_dates = bool(re.search(r'\b202[4-6]-\d{2}-\d{2}\b|\bNovember 2026\b|\b2026\b', content))
        self.assertTrue(
            has_dates,
            "SOURCES.md must record review or publication dates for cited documentation.",
        )

    def test_04_lab_coverage_consistency_with_phase6_files(self):
        """Asserts that the 4 hands-on labs in the tracker map 1:1 to physical files in 07_phase6_hands_on_labs/."""
        tracker_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "exam_coverage_tracker.md"
        tracker_content = read_text_safe(tracker_path)

        expected_labs = [
            "lab_01_bigquery_optimization.md",
            "lab_02_beam_dataflow_streaming.md",
            "lab_03_datastream_cdc_bigquery.md",
            "lab_04_cloud_composer_orchestration.md",
        ]

        for lab_file in expected_labs:
            physical_file = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / lab_file
            self.assertTrue(
                physical_file.is_file(),
                f"Physical lab file {physical_file} is missing.",
            )
            self.assertIn(
                lab_file,
                tracker_content,
                f"Tracker does not reference required lab file {lab_file}",
            )


if __name__ == "__main__":
    unittest.main()
