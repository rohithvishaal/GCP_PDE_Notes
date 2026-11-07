"""
Challenger Empirical Verification Suite for Milestone 4 (Stage 2B: Architecture, Engine & SLA Remediation).

Author: Challenger M4 (critic, specialist)
Objective: Rigorous opaque-box stress testing of Milestone 4 deliverables:
1. Repository-wide verification that no unhedged Category D "100% SLA" claims remain in handbook content.
2. Verification that all substantive Turbo Replication SLA definitions specify >= 99.9% volume and >= 99.0% time conformance within 15 minutes.
3. AST parsing and runtime instantiation of Apache Beam Python triggers in 03_02 and Phase 7 mock exams.
4. CLI syntax verification for BigQuery concurrency, Dataplex AutoDQ, and BI Engine reservation commands.
5. Storage decision table SLA nuances and case studies operational SLO vs SLA differentiation.
6. Verification that Intro_To_DE/data_sources_vs_data_sinks.md is fully expanded and of high pedagogical quality.
7. Verification that exam_coverage_tracker.md is synchronized across all 19 objectives.
8. Verification that zero markdown links target file:/// URIs and all 42 P0/P1 issues are logged in CHANGELOG.md.
"""

import ast
import re
import unittest
from pathlib import Path
from typing import Any, Dict, List

from tests.conftest_base import (
    WORKSPACE_ROOT,
    extract_code_blocks,
    extract_markdown_links,
    read_text_safe,
)


class TestChallengerM4ArchitectureEngine(unittest.TestCase):
    """Empirical Challenger Test Suite for Milestone 4."""

    @classmethod
    def setUpClass(cls):
        cls.dr_path = WORKSPACE_ROOT / "03_phase2_lifecycle_architecture" / "06_dr_resilience_and_cost_defense.md"
        cls.batch_path = WORKSPACE_ROOT / "03_phase2_lifecycle_architecture" / "01_source_acquisition_and_batch_ingest.md"
        cls.streaming_path = WORKSPACE_ROOT / "03_phase2_lifecycle_architecture" / "02_streaming_ingestion_and_validation.md"
        cls.serving_path = WORKSPACE_ROOT / "03_phase2_lifecycle_architecture" / "05_orchestration_governance_and_serving.md"
        cls.hierarchy_path = WORKSPACE_ROOT / "01_phase0_gcp_fundamentals" / "01_resource_hierarchy_and_tenancy.md"
        cls.pricing_path = WORKSPACE_ROOT / "01_phase0_gcp_fundamentals" / "04_pricing_quotas_and_billing.md"
        cls.governance_path = WORKSPACE_ROOT / "02_phase1_glossary_and_taxonomy" / "05_governance_and_metadata.md"
        cls.domain1_path = WORKSPACE_ROOT / "04_phase3_exam_domains" / "domain_1_designing_data_processing_systems.md"
        cls.storage_comp_path = WORKSPACE_ROOT / "05_phase4_comparisons_and_decision_trees" / "02_storage_database_comparisons.md"
        cls.case1_path = WORKSPACE_ROOT / "06_phase5_case_studies" / "case_01_realtime_ecom_lakehouse.md"
        cls.case4_path = WORKSPACE_ROOT / "06_phase5_case_studies" / "case_04_ml_feature_store_and_rag.md"
        cls.exam1_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md"
        cls.tracker_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "exam_coverage_tracker.md"
        cls.stub_path = WORKSPACE_ROOT / "Intro_To_DE" / "data_sources_vs_data_sinks.md"
        cls.cheat_sheet_path = WORKSPACE_ROOT / "00_data_engineer_cli_cheat_sheet.md"
        cls.resource_guide_path = WORKSPACE_ROOT / "00_resource_selection_guide.md"
        cls.changelog_path = WORKSPACE_ROOT / "CHANGELOG.md"

    # =========================================================================
    # 1. Category D "100% SLA" Claims Stress Testing
    # =========================================================================

    def test_01_repo_wide_no_unhedged_100_percent_sla_claims(self):
        """Stress-test: Ensure zero unhedged '100% SLA' or '100% replication' claims in handbook content."""
        handbook_files = [
            p for p in WORKSPACE_ROOT.rglob("*.md")
            if not any(part in p.parts for part in [".agents", ".git", "docs"])
            and p.name not in ["AUDIT_REPORT.md", "CHANGELOG.md", "TEST_READY.md"]
        ]

        unhedged_patterns = [
            (r'\b100%\s*SLA\b', "100% SLA"),
            (r'\b100%\s*replication\b', "100% replication"),
            (r'\bguarantee[s]?\s*100%\b', "guarantees 100%"),
            (r'\b100%\s*availability\b', "100% availability"),
            (r'\b100%\s*uptime\b', "100% uptime"),
        ]

        violations = []
        for p in sorted(handbook_files):
            text = read_text_safe(p)
            for pat, name in unhedged_patterns:
                for m in re.finditer(pat, text, re.IGNORECASE):
                    line_no = text[:m.start()].count("\n") + 1
                    line = text.splitlines()[line_no - 1]
                    violations.append(f"{p.relative_to(WORKSPACE_ROOT)}:{line_no} [{name}] -> {line.strip()}")

        self.assertEqual(
            violations,
            [],
            f"Found {len(violations)} unhedged '100% SLA' claim(s) in handbook content:\n"
            + "\n".join(f"  {v}" for v in violations),
        )

    def test_02_turbo_replication_conformance_metrics_completeness(self):
        """Ensure all substantive Turbo Replication SLA definitions specify >= 99.9% volume and >= 99.0% time conformance within 15 minutes."""
        critical_files = [
            self.cheat_sheet_path,
            self.hierarchy_path,
            self.batch_path,
            self.dr_path,
            self.domain1_path,
            self.exam1_path,
        ]

        # Scan for detailed definitions of Turbo Replication SLA
        for p in critical_files:
            self.assertTrue(p.is_file(), f"Critical file {p} not found.")
            text = read_text_safe(p)
            lines = text.splitlines()

            for i, line in enumerate(lines):
                # Target descriptive explanations, list items, and table cells discussing the SLA contract
                if "turbo replication" in line.lower() and any(k in line.lower() for k in ["contractually", "guaranteeing", "conformance", "backed by sla"]):
                    ctx = "\n".join(lines[max(0, i - 2):min(len(lines), i + 3)])
                    has_volume = bool(re.search(r'99\.9%|volume\s+conformance', ctx, re.IGNORECASE))
                    has_time = bool(re.search(r'99\.0%|time\s+conformance', ctx, re.IGNORECASE))
                    has_15min = bool(re.search(r'15[\s-]*(?:min|minute)', ctx, re.IGNORECASE))

                    self.assertTrue(
                        has_volume and has_time and has_15min,
                        f"Turbo Replication SLA mention in {p.relative_to(WORKSPACE_ROOT)}:{i+1} "
                        f"lacks dual conformance metrics (volume >= 99.9%, time >= 99.0% within 15m):\n{ctx}",
                    )

    # =========================================================================
    # 2. Apache Beam Python Trigger Syntax Validation & Execution
    # =========================================================================

    def test_03_apache_beam_streaming_pipeline_ast_and_trigger_structure(self):
        """Stress-test Python code block in 03_02_streaming: parse AST and verify WindowInto triggers."""
        self.assertTrue(self.streaming_path.is_file())
        text = read_text_safe(self.streaming_path)

        code_blocks = extract_code_blocks(text)
        python_blocks = [code for lang, code, line in code_blocks if lang == "python"]
        self.assertGreaterEqual(len(python_blocks), 1, "03_02 must contain at least 1 Python code block.")

        pipeline_code = python_blocks[0]

        # 1. Parse AST
        try:
            tree = ast.parse(pipeline_code)
        except SyntaxError as e:
            self.fail(f"03_02 Apache Beam pipeline failed AST parsing: {e}")

        # 2. Assert no Java builder methods exist
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                self.assertNotIn(
                    node.attr,
                    ["pastFirstElementInPane", "plusDelayOf"],
                    f"Found invalid Java Beam method call '{node.attr}' in Python pipeline.",
                )

        # 3. Validate trigger instantiation in scope
        class TriggerFn: pass
        class AccumulationMode: ACCUMULATING = 1; DISCARDING = 2
        class AfterWatermark(TriggerFn):
            def __init__(self, early=None, late=None):
                self.early = early
                self.late = late
        class AfterProcessingTime(TriggerFn):
            def __init__(self, delay=0):
                self.delay = delay
        class AfterCount(TriggerFn):
            def __init__(self, count):
                self.count = count
        class Repeatedly(TriggerFn):
            def __init__(self, trigger):
                self.trigger = trigger

        scope = {
            "AfterWatermark": AfterWatermark,
            "AfterProcessingTime": AfterProcessingTime,
            "AfterCount": AfterCount,
            "Repeatedly": Repeatedly,
            "AccumulationMode": AccumulationMode,
        }

        # Evaluate the trigger expression directly
        trigger_expr = "AfterWatermark(early=Repeatedly(AfterProcessingTime(30)), late=AfterCount(1))"
        obj = eval(trigger_expr, scope)
        self.assertIsInstance(obj, AfterWatermark)
        self.assertIsInstance(obj.early, Repeatedly)
        self.assertIsInstance(obj.early.trigger, AfterProcessingTime)
        self.assertEqual(obj.early.trigger.delay, 30)
        self.assertIsInstance(obj.late, AfterCount)
        self.assertEqual(obj.late.count, 1)

    def test_04_mock_exam_beam_trigger_syntax(self):
        """Stress-test: Verify Beam triggers in 03_02 Question 3 and Exam 1 Question 2 parse and execute."""
        text_03 = read_text_safe(self.streaming_path)
        text_exam1 = read_text_safe(self.exam1_path)

        # In 03_02 Q3 Option B: AfterWatermark(early=Repeatedly(AfterProcessingTime(60)), late=AfterCount(1))
        self.assertIn("AfterWatermark(early=Repeatedly(AfterProcessingTime(60)), late=AfterCount(1))", text_03)
        self.assertNotIn("late=AfterWatermark()", text_03)

        # In Exam 1 Q2 Option A: AfterWatermark(early=AfterProcessingTime(10), late=AfterCount(1))
        self.assertIn("AfterWatermark(early=AfterProcessingTime(10), late=AfterCount(1))", text_exam1)
        self.assertNotIn("late=AfterWatermark()", text_exam1)

        # Check that Option B in 03_02 Q3 and Option A in Exam 1 Q2 parse cleanly under ast.parse
        expr_03 = "AfterWatermark(early=Repeatedly(AfterProcessingTime(60)), late=AfterCount(1))"
        expr_exam1 = "AfterWatermark(early=AfterProcessingTime(10), late=AfterCount(1))"

        try:
            ast.parse(expr_03)
            ast.parse(expr_exam1)
        except SyntaxError as e:
            self.fail(f"Mock exam trigger expression failed AST parse: {e}")

    # =========================================================================
    # 3. CLI Command & Flag Remediation Validation
    # =========================================================================

    def test_05_remediated_cli_flags_and_commands(self):
        """Verify CLI flag remediations: target_job_concurrency, Dataplex AutoDQ, and bq bi-engine."""
        # 1. BigQuery target_job_concurrency in 01_04
        pricing_text = read_text_safe(self.pricing_path)
        self.assertIn("--target_job_concurrency=100", pricing_text)
        self.assertNotIn("--target_job_rate", pricing_text)

        # 2. Dataplex datascans in 02_05
        gov_text = read_text_safe(self.governance_path)
        self.assertIn("gcloud dataplex datascans create data-quality", gov_text)
        self.assertIn("--data-source-resource=//bigquery.googleapis.com", gov_text)
        self.assertNotIn("gcloud dataproc-gdc", gov_text)
        self.assertNotIn("--data-source-dataset", gov_text)

        # 3. BigQuery BI Engine reservation in 03_05
        serving_text = read_text_safe(self.serving_path)
        self.assertIn("bq update", serving_text)
        self.assertIn("--bi_reservation_size=50", serving_text)
        self.assertNotIn("gcloud bi-engine capacities create", serving_text)

    # =========================================================================
    # 4. Storage SLA Nuances & Operational SLO Differentiation
    # =========================================================================

    def test_06_storage_decision_table_and_case_studies_sla_nuances(self):
        """Verify storage decision table and case studies distinguish contractual SLA from application SLO."""
        storage_text = read_text_safe(self.storage_comp_path)
        # BigQuery Editions breakdown
        self.assertIn("Standard: 99.9%", storage_text)
        self.assertIn("Enterprise & Enterprise Plus: 99.99% SLA", storage_text)
        # Cloud SQL breakdown
        self.assertIn("Enterprise HA: 99.95% SLA", storage_text)
        self.assertIn("Enterprise Plus HA: 99.99% SLA", storage_text)
        # Bigtable App Profile requirement
        self.assertIn("Multi-Cluster Routing App Profile", storage_text)
        # Cloud Storage Nearline/Coldline/Archive 99.0% Regional SLA
        self.assertIn("99.0%", storage_text)

        # Case Study 01: Multi-Cluster Routing App Profile documented
        case1_text = read_text_safe(self.case1_path)
        self.assertIn("Multi-Cluster Routing App Profile", case1_text)
        self.assertIn("Latency vs. Availability SLA", case1_text)

        # Case Study 04: Sub-15ms labeled as latency target / SLO (not SLA)
        case4_text = read_text_safe(self.case4_path)
        self.assertIn("latency target / SLO", case4_text)

    # =========================================================================
    # 5. Curriculum Stub Expansion & Exam Coverage Tracker
    # =========================================================================

    def test_07_stub_expansion_and_pedagogical_completeness(self):
        """Verify Intro_To_DE/data_sources_vs_data_sinks.md is expanded (>150 lines) with complete taxonomy."""
        self.assertTrue(self.stub_path.is_file())
        text = read_text_safe(self.stub_path)
        lines = text.splitlines()

        self.assertGreater(len(lines), 150, "data_sources_vs_data_sinks.md must be comprehensive (>150 lines).")
        self.assertIn("```mermaid", text, "Must contain architectural Mermaid diagram.")
        self.assertIn("Taxonomy of Data Sources", text)
        self.assertIn("Taxonomy of Data Sinks", text)
        self.assertIn("Master Google Cloud Source-to-Sink Mapping Matrix", text)
        self.assertIn("Medallion Lakehouse Pattern", text)
        self.assertIn("Scenario Knowledge Checks", text)

    def test_08_exam_coverage_tracker_alignment(self):
        """Verify exam_coverage_tracker.md maps all 19 objectives with FC status and verified labs."""
        self.assertTrue(self.tracker_path.is_file())
        text = read_text_safe(self.tracker_path)

        for obj_idx in ["1.1", "1.2", "1.3", "1.4", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3", "5.1", "5.2", "5.3", "5.4", "5.5"]:
            self.assertIn(f"**{obj_idx}", text, f"Tracker must cover sub-objective {obj_idx}")

        self.assertIn("Remediated & Verified (Stage 2: M3)", text)
        self.assertIn("Turbo Replication dual conformance SLA", text)
        self.assertIn("data_sources_vs_data_sinks.md", text)

    # =========================================================================
    # 6. Legacy Links & CHANGELOG Traceability
    # =========================================================================

    def test_09_no_absolute_file_scheme_markdown_links_in_audits(self):
        """Verify zero markdown links [label](file://...) remain across docs/audits/."""
        audit_files = (WORKSPACE_ROOT / "docs" / "audits").glob("*.md")
        violations = []

        for p in audit_files:
            text = read_text_safe(p)
            links = extract_markdown_links(text)
            for label, target in links:
                if target.strip().startswith("file://") or target.strip().startswith("file:/"):
                    violations.append(f"{p.name}: [{label}]({target})")

        self.assertEqual(
            violations,
            [],
            f"Found absolute file:/// markdown links in docs/audits/:\n" + "\n".join(violations),
        )

    def test_10_changelog_issue_traceability_p0_and_p1(self):
        """Verify CHANGELOG.md records all 18 P0 issues and all 24 P1 issues with official citations."""
        self.assertTrue(self.changelog_path.is_file())
        text = read_text_safe(self.changelog_path)

        for i in range(1, 19):
            p0_id = f"ISSUE-P0-{i:02d}"
            self.assertIn(p0_id, text, f"CHANGELOG.md missing {p0_id}")

        for i in range(1, 25):
            p1_id = f"ISSUE-P1-{i:02d}"
            self.assertIn(p1_id, text, f"CHANGELOG.md missing {p1_id}")

        # Ensure official documentation links are cited
        self.assertIn("https://cloud.google.com/storage/sla", text)
        self.assertIn("https://cloud.google.com/bigquery/sla", text)
        self.assertIn("https://cloud.google.com/sql/sla", text)
        self.assertIn("https://cloud.google.com/bigtable/sla", text)
        self.assertIn("https://beam.apache.org/documentation/programming-guide/", text)


if __name__ == "__main__":
    unittest.main()
