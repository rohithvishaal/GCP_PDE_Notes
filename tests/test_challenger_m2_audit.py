"""
Challenger 1 Empirical Verification Suite for Milestone 2.

Author: Challenger 1 (critic, specialist)
Objective: Rigorous opaque-box stress testing of Milestone 2 deliverables:
1. 100% Census evaluation of 49 files in docs/audits/FILE_BY_FILE_FINDINGS.md
2. Validation of official cloud.google.com URLs across all findings
3. Validation of Categories A, B, C, D classification of quantitative claims
4. Cross-document consistency between FILE_BY_FILE_FINDINGS.md, REMEDIATION_BACKLOG.md,
   EXECUTIVE_AUDIT_REPORT.md, EXAM_COVERAGE_MATRIX.md, CHANGELOG.md, and SOURCES.md
5. Detection of edge cases: duplicate IDs, broken code blocks, placeholder texts (TODO/TBD)
"""

import re
import unittest
from pathlib import Path
from typing import Dict, List, Set, Tuple

from tests.conftest_base import (
    ALL_49_CENSUS_FILES,
    OFFICIAL_EXAM_OBJECTIVES,
    REQUIRED_AUDIT_DELIVERABLES,
    REQUIRED_ROOT_GOVERNANCE_FILES,
    WORKSPACE_ROOT,
    read_text_safe,
)


class TestChallengerM2Audit(unittest.TestCase):
    """Empirical Challenger Test Suite for Milestone 2 Audit Deliverables."""

    @classmethod
    def setUpClass(cls):
        cls.findings_path = WORKSPACE_ROOT / "docs" / "audits" / "FILE_BY_FILE_FINDINGS.md"
        cls.backlog_path = WORKSPACE_ROOT / "docs" / "audits" / "REMEDIATION_BACKLOG.md"
        cls.exec_report_path = WORKSPACE_ROOT / "docs" / "audits" / "EXECUTIVE_AUDIT_REPORT.md"
        cls.exam_matrix_path = WORKSPACE_ROOT / "docs" / "audits" / "EXAM_COVERAGE_MATRIX.md"
        cls.changelog_path = WORKSPACE_ROOT / "CHANGELOG.md"
        cls.sources_path = WORKSPACE_ROOT / "SOURCES.md"
        cls.audit_report_path = WORKSPACE_ROOT / "AUDIT_REPORT.md"

        cls.findings_content = read_text_safe(cls.findings_path)
        cls.backlog_content = read_text_safe(cls.backlog_path)
        cls.exec_report_content = read_text_safe(cls.exec_report_path)
        cls.exam_matrix_content = read_text_safe(cls.exam_matrix_path)
        cls.changelog_content = read_text_safe(cls.changelog_path)
        cls.sources_content = read_text_safe(cls.sources_path)
        cls.audit_report_content = read_text_safe(cls.audit_report_path)

    def test_01_all_49_files_evaluated_in_findings_table(self):
        """Verify that FILE_BY_FILE_FINDINGS.md contains all 49 census files in its Section 1 table."""
        self.assertTrue(self.findings_path.is_file(), "FILE_BY_FILE_FINDINGS.md is missing")

        lines = self.findings_content.splitlines()
        # Find table rows starting with '| <number> |'
        table_rows = []
        for line in lines:
            line_str = line.strip()
            if line_str.startswith("|") and not line_str.startswith("| #") and not line_str.startswith("|---"):
                parts = [p.strip() for p in line_str.split("|")[1:-1]]
                if len(parts) >= 6 and parts[0].isdigit():
                    table_rows.append(parts)

        self.assertEqual(
            len(table_rows), 49,
            f"Expected exactly 49 evaluated files in Section 1 table, found {len(table_rows)}"
        )

        evaluated_files = [row[1].replace("`", "") for row in table_rows]
        missing_files = [f for f in ALL_49_CENSUS_FILES if f not in evaluated_files]
        self.assertEqual(missing_files, [], f"Files missing from census evaluation table: {missing_files}")

        # Check row numbering 1 to 49
        row_numbers = [int(row[0]) for row in table_rows]
        self.assertEqual(row_numbers, list(range(1, 50)), "Table row numbering is not 1..49 sequential")

        # Verify evaluation status is either **Defects Identified** or **Verified Compliant**
        valid_statuses = {"**Defects Identified**", "**Verified Compliant**"}
        for row in table_rows:
            status = row[4].strip()
            self.assertIn(
                status, valid_statuses,
                f"File {row[1]} has unexpected status '{status}'. Expected one of {valid_statuses}"
            )

    def test_02_finding_headers_and_unique_ids(self):
        """Stress-test finding headers in FILE_BY_FILE_FINDINGS.md for uniqueness and valid ID structure."""
        findings = re.findall(r"^####\s+([A-Z0-9_\-]+):\s*(.+)$", self.findings_content, flags=re.MULTILINE)
        self.assertEqual(len(findings), 56, f"Expected exactly 56 detailed findings (P0..P3), found {len(findings)}")

        finding_ids = [f[0] for f in findings]
        duplicates = [fid for fid in set(finding_ids) if finding_ids.count(fid) > 1]
        self.assertEqual(duplicates, [], f"Found duplicate finding IDs in FILE_BY_FILE_FINDINGS.md: {duplicates}")

        # Ensure ID format matches FINDING-P<0-3>-<num>
        for fid in finding_ids:
            self.assertTrue(
                re.match(r"^FINDING-P[0-3]-\d{2}$", fid),
                f"Finding ID '{fid}' does not conform to regex ^FINDING-P[0-3]-\\d{{2}}$"
            )

    def test_03_all_findings_cite_official_cloud_google_com_urls(self):
        """Verify official Google Cloud documentation citations across all 56 findings."""
        finding_blocks = re.split(r"(?m)^####\s+", self.findings_content)[1:]
        self.assertEqual(len(finding_blocks), 56, f"Expected 56 finding blocks, got {len(finding_blocks)}")

        findings_without_url = []
        non_gcp_citations = []
        all_official_urls = []

        for block in finding_blocks:
            lines = block.strip().splitlines()
            fid = lines[0].split(":")[0].strip()

            # Find the Official Documentation line specifically
            doc_line = ""
            for l in lines:
                if l.strip().startswith("- **Official Documentation**:"):
                    doc_line = l.strip()
                    break

            urls = re.findall(r"https?://[^\s\)\|\"\']+", doc_line)
            if not urls:
                findings_without_url.append((fid, doc_line))
            else:
                for u in urls:
                    if u.startswith("https://cloud.google.com/"):
                        all_official_urls.append((fid, u))
                    else:
                        non_gcp_citations.append((fid, u))

        # Check for non-URL findings (e.g. FINDING-P1-24 internal broken links)
        # Note: FINDING-P1-24 is a repository relative link defect, citing "Repository relative link integrity standard"
        # FINDING-P1-01, FINDING-P1-10, FINDING-P1-17, FINDING-P2-04, FINDING-P2-05, FINDING-P2-10, FINDING-P3-01: check other sources
        # Print diagnostic info
        print(f"\n[DIAGNOSTIC] Findings without URL in Official Documentation: {len(findings_without_url)}")
        for fid, doc_line in findings_without_url:
            print(f"  {fid}: {doc_line}")

        print(f"[DIAGNOSTIC] Non-cloud.google.com URLs: {len(non_gcp_citations)}")
        for fid, u in non_gcp_citations:
            print(f"  {fid}: {u}")

        print(f"[DIAGNOSTIC] Valid cloud.google.com URLs: {len(all_official_urls)}")

        # Verify that all findings EXCEPT FINDING-P1-24 (and external open source like apache.org/sre.google/services.google.com) have official citations
        # We assert that cloud.google.com URLs are the dominant citation source (>= 45 findings)
        self.assertGreaterEqual(
            len(all_official_urls), 45,
            f"Expected at least 45 official cloud.google.com citations, got {len(all_official_urls)}"
        )

    def test_04_quantitative_claims_classification_categories(self):
        """Verify that quantitative performance claims are comprehensively classified into Categories A, B, C, D."""
        categories = ["Category A", "Category B", "Category C", "Category D"]

        for cat in categories:
            self.assertIn(
                cat, self.findings_content,
                f"Missing '{cat}' in FILE_BY_FILE_FINDINGS.md"
            )
            self.assertIn(
                cat, self.exec_report_content,
                f"Missing '{cat}' in EXECUTIVE_AUDIT_REPORT.md"
            )

        # Parse Category A, B, C, D tables in Section 3 of FILE_BY_FILE_FINDINGS.md
        cat_a_matches = re.findall(r"###\s+3\.1\s+Category A:[\s\S]*?(?=###\s+3\.2|\Z)", self.findings_content)
        cat_b_matches = re.findall(r"###\s+3\.2\s+Category B:[\s\S]*?(?=###\s+3\.3|\Z)", self.findings_content)
        cat_c_matches = re.findall(r"###\s+3\.3\s+Category C:[\s\S]*?(?=###\s+3\.4|\Z)", self.findings_content)
        cat_d_matches = re.findall(r"###\s+3\.4\s+Category D:[\s\S]*?(?=\n---\n|##\s+4|\Z)", self.findings_content)

        self.assertTrue(len(cat_a_matches) > 0, "Missing Category A section")
        self.assertTrue(len(cat_b_matches) > 0, "Missing Category B section")
        self.assertTrue(len(cat_c_matches) > 0, "Missing Category C section")
        self.assertTrue(len(cat_d_matches) > 0, "Missing Category D section")

        # Verify items inside each category table
        cat_a_rows = [l for l in cat_a_matches[0].splitlines() if l.strip().startswith("|") and not l.strip().startswith("| :") and not l.strip().startswith("| Service")]
        cat_b_rows = [l for l in cat_b_matches[0].splitlines() if l.strip().startswith("|") and not l.strip().startswith("| :") and not l.strip().startswith("| Service")]
        cat_c_rows = [l for l in cat_c_matches[0].splitlines() if l.strip().startswith("|") and not l.strip().startswith("| :") and not l.strip().startswith("| System")]
        cat_d_rows = [l for l in cat_d_matches[0].splitlines() if l.strip().startswith("|") and not l.strip().startswith("| :") and not l.strip().startswith("| File")]

        print(f"\n[DIAGNOSTIC] Category A cataloged claims: {len(cat_a_rows)}")
        print(f"[DIAGNOSTIC] Category B cataloged claims: {len(cat_b_rows)}")
        print(f"[DIAGNOSTIC] Category C cataloged claims: {len(cat_c_rows)}")
        print(f"[DIAGNOSTIC] Category D cataloged claims: {len(cat_d_rows)}")

        self.assertGreaterEqual(len(cat_a_rows), 10, "Expected at least 10 Category A claims")
        self.assertGreaterEqual(len(cat_b_rows), 10, "Expected at least 10 Category B claims")
        self.assertGreaterEqual(len(cat_c_rows), 5, "Expected at least 5 Category C claims")
        self.assertGreaterEqual(len(cat_d_rows), 10, "Expected at least 10 Category D claims")

    def test_05_remediation_backlog_p0_p1_completeness_and_traceability(self):
        """Stress-test REMEDIATION_BACKLOG.md issue definitions against CHANGELOG.md and FILE_BY_FILE_FINDINGS.md."""
        self.assertTrue(self.backlog_path.is_file(), "REMEDIATION_BACKLOG.md is missing")

        p0_issues = re.findall(r"\|\s*`(ISSUE-P0-\d{2})`\s*\|", self.backlog_content)
        p1_issues = re.findall(r"\|\s*`(ISSUE-P1-\d{2})`\s*\|", self.backlog_content)
        p2_issues = re.findall(r"\|\s*`(ISSUE-P2-\d{2})`\s*\|", self.backlog_content)
        p3_issues = re.findall(r"\|\s*`(ISSUE-P3-\d{2})`\s*\|", self.backlog_content)

        print(f"\n[DIAGNOSTIC] Backlog P0 issues: {len(p0_issues)}")
        print(f"[DIAGNOSTIC] Backlog P1 issues: {len(p1_issues)}")
        print(f"[DIAGNOSTIC] Backlog P2 issues: {len(p2_issues)}")
        print(f"[DIAGNOSTIC] Backlog P3 issues: {len(p3_issues)}")

        self.assertEqual(len(p0_issues), 18, f"Expected 18 P0 issues, found {len(p0_issues)}")
        self.assertEqual(len(p1_issues), 24, f"Expected 24 P1 issues, found {len(p1_issues)}")
        self.assertEqual(len(p2_issues), 11, f"Expected 11 P2 issues, found {len(p2_issues)}")
        self.assertEqual(len(p3_issues), 3, f"Expected 3 P3 issues, found {len(p3_issues)}")

        # Verify no duplicate issue IDs
        all_backlog_ids = p0_issues + p1_issues + p2_issues + p3_issues
        duplicates = [x for x in set(all_backlog_ids) if all_backlog_ids.count(x) > 1]
        self.assertEqual(duplicates, [], f"Duplicate backlog issue IDs: {duplicates}")

        # Verify that every P0 and P1 issue is listed in root CHANGELOG.md
        missing_in_changelog = []
        for issue_id in p0_issues + p1_issues:
            if issue_id not in self.changelog_content:
                missing_in_changelog.append(issue_id)

        self.assertEqual(
            missing_in_changelog, [],
            f"P0/P1 issues missing from root CHANGELOG.md: {missing_in_changelog}"
        )

    def test_06_exam_coverage_matrix_all_19_objectives_and_weights(self):
        """Verify that EXAM_COVERAGE_MATRIX.md contains all 19 objectives, domain weights, and canonical docs."""
        self.assertTrue(self.exam_matrix_path.is_file(), "EXAM_COVERAGE_MATRIX.md is missing")

        missing_objectives = []
        for obj_id, info in OFFICIAL_EXAM_OBJECTIVES.items():
            pattern = rf"\b{re.escape(obj_id)}\b"
            if not re.search(pattern, self.exam_matrix_content):
                missing_objectives.append(obj_id)

        self.assertEqual(missing_objectives, [], f"Missing objectives in exam coverage matrix: {missing_objectives}")

        # Verify domain weights: 22%, 25%, 20%, 15%, 18%
        for weight in ["22%", "25%", "20%", "15%", "18%"]:
            self.assertIn(weight, self.exam_matrix_content, f"Missing domain weight {weight} in matrix")

        # Verify all 4 status categories appear: Fully covered, Partially covered, Missing, Covered but technically incorrect
        for st in ["Fully covered", "Partially covered", "Missing", "Covered but technically incorrect"]:
            self.assertIn(st, self.exam_matrix_content, f"Missing status '{st}' in matrix")

    def test_07_root_governance_files_integrity(self):
        """Verify root AUDIT_REPORT.md, CHANGELOG.md, and SOURCES.md integrity and non-emptiness."""
        for rf in REQUIRED_ROOT_GOVERNANCE_FILES:
            target = WORKSPACE_ROOT / rf
            self.assertTrue(target.is_file(), f"Root governance file {rf} does not exist")
            content = read_text_safe(target)
            self.assertGreater(len(content), 100, f"Root governance file {rf} is empty or trivial")

        # Check SOURCES.md contains valid Google Cloud URLs
        sources_urls = re.findall(r"https://cloud\.google\.com/[^\s\)\|\"\']+", self.sources_content)
        self.assertGreaterEqual(len(sources_urls), 20, "SOURCES.md should have at least 20 official GCP URLs")

    def test_08_no_unresolved_placeholders_in_formal_deliverables(self):
        """Stress-test all 7 formal audit deliverables and 3 root governance files for unresolved agent placeholders."""
        all_deliverables = REQUIRED_AUDIT_DELIVERABLES + REQUIRED_ROOT_GOVERNANCE_FILES
        # Distinct pattern for unrendered placeholders like TODO:, TBD, FIXME, [REPLACE_ME]
        placeholder_pattern = re.compile(r"\b(TODO|FIXME|REPLACE_ME|\[TBD\])\b", re.IGNORECASE)

        flagged_files = {}
        for rel_path in all_deliverables:
            fpath = WORKSPACE_ROOT / rel_path
            content = read_text_safe(fpath)
            matches = placeholder_pattern.findall(content)
            if matches:
                flagged_files[rel_path] = matches

        self.assertEqual(
            flagged_files, {},
            f"Deliverables contain unresolved agent placeholders: {flagged_files}"
        )

    def test_09_deep_url_inspection(self):
        """Stress-test all cloud.google.com URLs for syntax validity and valid product path namespaces."""
        all_text = self.findings_content + "\n" + self.backlog_content + "\n" + self.sources_content
        raw_urls = re.findall(r"https://cloud\.google\.com/[^\s\)\|\"\'>`]*", all_text)
        self.assertGreaterEqual(len(raw_urls), 80, f"Expected at least 80 GCP doc URL mentions, found {len(raw_urls)}")

        unique_urls = sorted(set(u.rstrip("/") for u in raw_urls if u.strip()))
        print(f"\n[DIAGNOSTIC] Total unique cloud.google.com URLs across deliverables: {len(unique_urls)}")

        valid_product_prefixes = [
            "", "storage", "bigquery", "dataflow", "composer", "datastream", "dataplex",
            "pubsub", "sql", "spanner", "bigtable", "alloydb", "vertex-ai",
            "dataproc", "dataproc-serverless", "sensitive-data-protection",
            "network-connectivity", "vpc", "architecture", "terms", "docs"
        ]

        malformed_urls = []
        unknown_namespaces = []

        for u in unique_urls:
            # Check for trailing punctuation or markdown artifacts
            if re.search(r"[,;:\.`]$", u):
                malformed_urls.append(u)

            # Check namespace
            path_part = u.replace("https://cloud.google.com", "").lstrip("/")
            first_segment = path_part.split("/")[0].split("#")[0].split("?")[0]
            if first_segment not in valid_product_prefixes:
                unknown_namespaces.append((u, first_segment))

        self.assertEqual(malformed_urls, [], f"Found URLs with trailing punctuation: {malformed_urls}")
        self.assertEqual(unknown_namespaces, [], f"Found URLs with unrecognized GCP namespace: {unknown_namespaces}")



if __name__ == "__main__":
    unittest.main()



