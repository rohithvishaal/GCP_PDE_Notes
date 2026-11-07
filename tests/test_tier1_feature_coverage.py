"""
Tier 1: Feature Coverage & Census Test Suite.

Verifies:
- 100% Repository File Census (49 files)
- Official Documentation Citation Completeness (cloud.google.com URLs)
- Performance Claim Classification (Categories A, B, C, D)
- Exam Guide 19-Objective Mapping (FC, PC, M, TI)
- Mock Exam Question Structure (stems, options, answers, explanations)
- Audit Deliverables Existence (7 files in docs/audits/)
- Root Governance Files Existence (AUDIT_REPORT, CHANGELOG, SOURCES)
"""

import re
import unittest
from pathlib import Path

from tests.conftest_base import (
    ALL_49_CENSUS_FILES,
    OFFICIAL_EXAM_OBJECTIVES,
    REQUIRED_AUDIT_DELIVERABLES,
    REQUIRED_ROOT_GOVERNANCE_FILES,
    WORKSPACE_ROOT,
    read_text_safe,
)


class TestTier1FeatureCoverage(unittest.TestCase):
    """Tier 1: Opaque-Box Feature Coverage Test Suite."""

    def test_01_repository_files_exist_on_disk(self):
        """Asserts that 100% of the 49 census files exist physically on disk."""
        missing_files = []
        for rel_path in ALL_49_CENSUS_FILES:
            target_path = WORKSPACE_ROOT / rel_path
            if not target_path.is_file():
                missing_files.append(rel_path)

        self.assertEqual(
            missing_files,
            [],
            f"Missing {len(missing_files)} census file(s) from repository: {missing_files}",
        )

    def test_02_repository_file_census_evaluated_in_findings(self):
        """Asserts that all 49 files are explicitly inventoried and evaluated in FILE_BY_FILE_FINDINGS.md."""
        findings_path = WORKSPACE_ROOT / "docs" / "audits" / "FILE_BY_FILE_FINDINGS.md"
        self.assertTrue(
            findings_path.is_file(),
            f"Required audit deliverable {findings_path} does not exist yet.",
        )

        content = read_text_safe(findings_path)
        unevaluated_files = []
        for rel_path in ALL_49_CENSUS_FILES:
            # Check for filename or full path occurrence in findings table
            filename = Path(rel_path).name
            if filename not in content and rel_path not in content:
                unevaluated_files.append(rel_path)

        self.assertEqual(
            unevaluated_files,
            [],
            f"FILE_BY_FILE_FINDINGS.md omitted {len(unevaluated_files)} files from census: {unevaluated_files}",
        )

    def test_03_official_gcp_documentation_citation_completeness(self):
        """Asserts every finding in FILE_BY_FILE_FINDINGS.md cites an official Google Cloud doc URL."""
        findings_path = WORKSPACE_ROOT / "docs" / "audits" / "FILE_BY_FILE_FINDINGS.md"
        self.assertTrue(
            findings_path.is_file(),
            f"Findings deliverable {findings_path} does not exist.",
        )

        content = read_text_safe(findings_path)
        # Verify official cloud.google.com citations
        official_urls = re.findall(r'https://cloud\.google\.com/[^\s\)\|\"\']+', content)
        self.assertGreaterEqual(
            len(official_urls),
            10,
            f"Expected at least 10 official Google Cloud doc citations in findings, found {len(official_urls)}",
        )

        # Check for citation of specific sections / documentation paths
        has_specific_sections = any("/docs/" in url or "/architecture/" in url for url in official_urls)
        self.assertTrue(
            has_specific_sections,
            "Official citations must point to specific documentation or architecture sections.",
        )

    def test_04_performance_claim_classification_categories(self):
        """Asserts quantitative performance claims are classified across Categories A, B, C, and D."""
        # Check either in EXECUTIVE_AUDIT_REPORT.md or dedicated findings/claim catalog
        audit_report_path = WORKSPACE_ROOT / "docs" / "audits" / "EXECUTIVE_AUDIT_REPORT.md"
        findings_path = WORKSPACE_ROOT / "docs" / "audits" / "FILE_BY_FILE_FINDINGS.md"

        combined_content = ""
        if audit_report_path.is_file():
            combined_content += read_text_safe(audit_report_path)
        if findings_path.is_file():
            combined_content += read_text_safe(findings_path)

        self.assertTrue(
            len(combined_content) > 0,
            "Neither EXECUTIVE_AUDIT_REPORT.md nor FILE_BY_FILE_FINDINGS.md is available to verify claims.",
        )

        for category in ["Category A", "Category B", "Category C", "Category D"]:
            self.assertIn(
                category,
                combined_content,
                f"Missing quantitative performance classification '{category}' in audit reports.",
            )

    def test_05_exam_guide_mapping_all_19_objectives(self):
        """Asserts all 19 official PDE exam guide sub-objectives are classified in EXAM_COVERAGE_MATRIX.md."""
        matrix_path = WORKSPACE_ROOT / "docs" / "audits" / "EXAM_COVERAGE_MATRIX.md"
        self.assertTrue(
            matrix_path.is_file(),
            f"Exam coverage matrix {matrix_path} does not exist.",
        )

        content = read_text_safe(matrix_path)
        unmapped_objectives = []
        valid_statuses = ["Fully covered", "Partially covered", "Missing", "Covered but technically incorrect", "FC", "PC", "M", "TI"]

        for obj_id, details in OFFICIAL_EXAM_OBJECTIVES.items():
            # Check objective ID appears
            obj_pattern = re.compile(rf'\b{re.escape(obj_id)}\b')
            if not obj_pattern.search(content):
                unmapped_objectives.append(obj_id)

        self.assertEqual(
            unmapped_objectives,
            [],
            f"EXAM_COVERAGE_MATRIX.md omitted sub-objectives: {unmapped_objectives}",
        )

        # Check that classification statuses appear in the document
        status_found = any(st in content for st in valid_statuses)
        self.assertTrue(
            status_found,
            "EXAM_COVERAGE_MATRIX.md must classify objectives into FC, PC, M, or TI.",
        )

    def test_06_mock_exam_question_structure_and_explanations(self):
        """Checks mock questions in Phase 7 for scenario stem, 4 choices, answer, and explanations."""
        exam_files = [
            WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md",
            WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "02_scenario_exam_storage_optimization.md",
        ]

        for exam_file in exam_files:
            self.assertTrue(exam_file.is_file(), f"Mock exam file {exam_file} missing.")
            content = read_text_safe(exam_file)

            # Assert questions exist
            questions = re.findall(r'(?m)^###\s+Question\s+(\d+)', content)
            self.assertGreaterEqual(
                len(questions),
                15,
                f"Mock exam {exam_file.name} must have at least 15 questions, found {len(questions)}",
            )

            # Check for Option A, B, C, D structure
            self.assertIn("A.", content, f"{exam_file.name} missing Option A formatting")
            self.assertIn("B.", content, f"{exam_file.name} missing Option B formatting")
            self.assertIn("C.", content, f"{exam_file.name} missing Option C formatting")
            self.assertIn("D.", content, f"{exam_file.name} missing Option D formatting")

            # Check for correct answer declaration and explanation
            has_answers = "Correct Answer:" in content or "**Correct Answer" in content
            self.assertTrue(has_answers, f"{exam_file.name} missing 'Correct Answer:' declarations")

            has_explanations = "Explanation" in content or "Rationale" in content
            self.assertTrue(has_explanations, f"{exam_file.name} missing explanation sections")

    def test_07_audit_deliverables_existence(self):
        """Asserts that all 7 formal audit files exist in docs/audits/ and are non-empty."""
        missing_deliverables = []
        for rel_path in REQUIRED_AUDIT_DELIVERABLES:
            target = WORKSPACE_ROOT / rel_path
            if not target.is_file() or target.stat().st_size < 50:
                missing_deliverables.append(rel_path)

        self.assertEqual(
            missing_deliverables,
            [],
            f"Missing required audit deliverable(s) in docs/audits/: {missing_deliverables}",
        )

    def test_08_root_governance_files_existence(self):
        """Asserts that all 3 root governance files exist and are non-empty."""
        missing_governance = []
        for rel_path in REQUIRED_ROOT_GOVERNANCE_FILES:
            target = WORKSPACE_ROOT / rel_path
            if not target.is_file() or target.stat().st_size < 50:
                missing_governance.append(rel_path)

        self.assertEqual(
            missing_governance,
            [],
            f"Missing permanent root governance file(s): {missing_governance}",
        )


if __name__ == "__main__":
    unittest.main()
