"""
Tier 4: Real-World Execution Scenarios & Static Analysis Test Suite.

Verifies:
- Python AST Parsing for Labs and Composer DAGs (zero SyntaxErrors, no JS booleans like 'false')
- CLI Command Anti-Hallucination Auditing (no 'gcloud bi-engine', no '--enable-turbo-replication')
- BigQuery SQL DDL / DML Syntax Correctness (no OPTIONS on SELECT)
- Datastream CLI Connectivity Parameter Completeness
- Beam Pipeline Trigger Syntax Correctness
- Contractual SLA Accuracy (no '100% SLA' claims for Turbo Replication)
"""

import ast
import re
import unittest
from pathlib import Path
from typing import List

from tests.conftest_base import (
    WORKSPACE_ROOT,
    extract_code_blocks,
    read_text_safe,
)


class TestTier4ExecutionScenarios(unittest.TestCase):
    """Tier 4: Real-World Execution Scenarios & Static Analysis."""

    def test_01_lab04_composer_dag_ast_and_keywords(self):
        """Verifies Python Airflow DAG in Lab 04 parses cleanly via AST and contains no lowercase 'false'."""
        lab4_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_04_cloud_composer_orchestration.md"
        self.assertTrue(lab4_path.is_file(), f"Lab 04 file {lab4_path} not found.")

        content = read_text_safe(lab4_path)
        code_blocks = extract_code_blocks(content)
        python_blocks = [code for lang, code, line in code_blocks if lang == "python"]

        self.assertGreaterEqual(len(python_blocks), 1, "Lab 04 must contain at least one Python code block.")

        # Find the DAG definition block
        dag_block = None
        for code in python_blocks:
            if "from airflow" in code or "DAG(" in code:
                dag_block = code
                break

        self.assertIsNotNone(dag_block, "Could not find Airflow DAG Python block in Lab 04.")

        # AST Parse check
        try:
            tree = ast.parse(dag_block)
        except SyntaxError as err:
            self.fail(f"Lab 04 Airflow DAG failed Python AST parse with SyntaxError: {err}")

        # Check for uncapitalized JS keywords used as Python identifiers: false, true, null
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                self.assertNotIn(
                    node.id,
                    ["false", "true", "null"],
                    f"Found uncapitalized JavaScript keyword '{node.id}' in Lab 04 Python DAG. "
                    f"In Python, this causes NameError (must be capitalized '{node.id.capitalize()}').",
                )

    def test_02_lab01_bi_engine_cli_anti_hallucination(self):
        """Asserts Lab 01 does not contain hallucinated 'gcloud bi-engine' commands."""
        lab1_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_01_bigquery_optimization.md"
        self.assertTrue(lab1_path.is_file(), f"Lab 01 file {lab1_path} not found.")

        content = read_text_safe(lab1_path)
        # Check for invalid gcloud bi-engine command group
        has_bi_engine_cli = bool(re.search(r'gcloud\s+bi-engine\b', content))
        self.assertFalse(
            has_bi_engine_cli,
            "Lab 01 contains hallucinated command 'gcloud bi-engine'. "
            "BI Engine reservations must be created via 'bq update --bi_reservation_size=...' or BigQuery DDL.",
        )

    def test_03_cli_cheat_sheet_anti_hallucination(self):
        """Checks CLI cheat sheet for non-existent flags and invalid dual-region / BI Engine syntax."""
        cheat_sheet_path = WORKSPACE_ROOT / "00_data_engineer_cli_cheat_sheet.md"
        self.assertTrue(cheat_sheet_path.is_file(), f"CLI cheat sheet {cheat_sheet_path} not found.")

        content = read_text_safe(cheat_sheet_path)

        # 1. Non-existent --enable-turbo-replication flag
        self.assertNotIn(
            "--enable-turbo-replication",
            content,
            "Cheat sheet uses non-existent flag '--enable-turbo-replication'. "
            "Must use '--recovery-point-objective=ASYNC_TURBO' or '--rpo=ASYNC_TURBO'.",
        )

        # 2. Invalid dual-region syntax --location=us-central1,us-east4
        self.assertNotIn(
            "--location=us-central1,us-east4",
            content,
            "Cheat sheet uses invalid dual-region syntax '--location=us-central1,us-east4'. "
            "Must use '--location=US --placement=us-central1,us-east4'.",
        )

        # 3. Invalid bq connection type BI_ENGINE
        self.assertNotIn(
            "--connection_type=BI_ENGINE",
            content,
            "Cheat sheet uses invalid 'bq mk --connection_type=BI_ENGINE'. "
            "BI Engine reservations do not use connection type BI_ENGINE.",
        )

    def test_04_lab03_datastream_connectivity_and_sql_syntax(self):
        """Checks Lab 03 for valid Datastream connectivity flags and BigQuery SQL syntax."""
        lab3_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_03_datastream_cdc_bigquery.md"
        self.assertTrue(lab3_path.is_file(), f"Lab 03 file {lab3_path} not found.")

        content = read_text_safe(lab3_path)

        # Check Datastream connection profiles command
        datastream_create_matches = re.findall(
            r'gcloud\s+datastream\s+connection-profiles\s+create[^\n]+(?:\n\s+[^\n]+)*',
            content,
        )
        for cmd in datastream_create_matches:
            has_connectivity = (
                "--static-ip-connectivity" in cmd
                or "--private-connection" in cmd
                or "--forward-ssh-hostname" in cmd
            )
            self.assertTrue(
                has_connectivity,
                f"Datastream create command missing network connectivity flag in Lab 03:\n{cmd}",
            )

        # Check for invalid SQL: SELECT ... OPTIONS(max_staleness = ...)
        has_invalid_options = bool(
            re.search(r'SELECT\b[^;]+OPTIONS\s*\(\s*max_staleness\b', content, re.IGNORECASE | re.DOTALL)
        )
        self.assertFalse(
            has_invalid_options,
            "Lab 03 places 'OPTIONS(max_staleness = ...)' on a SELECT statement. "
            "In BigQuery, max_staleness is a table-level option set via ALTER TABLE ... SET OPTIONS(...).",
        )

    def test_05_mock_exam1_beam_trigger_syntax(self):
        """Checks Exam 1 Question 2 for impossible Beam trigger syntax 'late=AfterWatermark()'."""
        exam1_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md"
        self.assertTrue(exam1_path.is_file(), f"Exam 1 file {exam1_path} not found.")

        content = read_text_safe(exam1_path)
        # Search in Question 2
        q2_section = re.search(r'###\s+Question\s+2\b.*?(?=###\s+Question\s+3|\Z)', content, re.DOTALL)
        if q2_section:
            q2_text = q2_section.group(0)
            self.assertNotIn(
                "late=AfterWatermark()",
                q2_text,
                "Exam 1 Q2 contains impossible Beam trigger syntax 'late=AfterWatermark()'. "
                "Late triggers must be recurring processing-time triggers like 'AfterProcessingTime(...)'.",
            )

    def test_06_turbo_replication_sla_accuracy(self):
        """Asserts Turbo Replication is not characterized with an unsupported '100% SLA' claim."""
        dr_file_path = (
            WORKSPACE_ROOT
            / "03_phase2_lifecycle_architecture"
            / "06_dr_resilience_and_cost_defense.md"
        )
        self.assertTrue(dr_file_path.is_file(), f"Lifecycle DR file {dr_file_path} not found.")

        content = read_text_safe(dr_file_path)
        has_100_percent_sla = bool(
            re.search(r'guaranteeing\s+that\s+\*{0,2}100%', content, re.IGNORECASE)
            or re.search(r'100%\s+of\s+all\s+newly\s+written\s+objects', content, re.IGNORECASE)
        )
        self.assertFalse(
            has_100_percent_sla,
            "Phase 2 DR file incorrectly claims Turbo Replication has a contractual '100% SLA'. "
            "Google Cloud Storage SLA guarantees >= 99.9% replication volume conformance.",
        )

    def test_07_all_lab_python_code_blocks_ast_validity(self):
        """Asserts all standalone Python code blocks in Phase 6 labs parse without SyntaxError."""
        lab_files = (WORKSPACE_ROOT / "07_phase6_hands_on_labs").glob("*.md")
        parse_errors: List[str] = []

        for lab_path in sorted(lab_files):
            content = read_text_safe(lab_path)
            code_blocks = extract_code_blocks(content)
            for lang, code, start_line in code_blocks:
                if lang != "python":
                    continue
                # Skip partial snippets or pseudo-code if missing imports/structure
                if "import " not in code and "def " not in code:
                    continue

                try:
                    ast.parse(code)
                except SyntaxError as e:
                    parse_errors.append(f"{lab_path.name} line {start_line}: {e}")

        self.assertEqual(
            parse_errors,
            [],
            f"Found {len(parse_errors)} Python code block(s) with SyntaxError:\n"
            + "\n".join(f"  {err}" for err in parse_errors),
        )


if __name__ == "__main__":
    unittest.main()
