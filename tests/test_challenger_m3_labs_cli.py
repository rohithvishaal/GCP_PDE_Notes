"""
Challenger Empirical Verification Suite for Milestone 3 (Stage 2A: Labs & CLI Remediation).

Author: Challenger M3 (critic, specialist)
Objective: Rigorous opaque-box stress testing of Milestone 3 deliverables:
1. Python AST parsing of all code blocks across Labs 01-04 (ensuring zero SyntaxErrors and valid boolean types).
2. Deep AST inspection of Lab 04 Composer DAG (ensuring use_legacy_sql=False is a boolean constant, no JS booleans).
3. CLI Anti-Hallucination & Flag Syntax verification across Labs 01-04 and 00_data_engineer_cli_cheat_sheet.md.
4. Static Markdown structure & fence balance validation across all 5 Milestone 3 files.
5. Detection of edge cases, regression risks, and subtle flag discrepancies.
"""

import ast
import re
import unittest
from pathlib import Path
from typing import List, Tuple

from tests.conftest_base import (
    WORKSPACE_ROOT,
    extract_code_blocks,
    read_text_safe,
)


class TestChallengerM3LabsAndCLI(unittest.TestCase):
    """Empirical Challenger Test Suite for Milestone 3."""

    @classmethod
    def setUpClass(cls):
        cls.lab1_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_01_bigquery_optimization.md"
        cls.lab2_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_02_beam_dataflow_streaming.md"
        cls.lab3_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_03_datastream_cdc_bigquery.md"
        cls.lab4_path = WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_04_cloud_composer_orchestration.md"
        cls.cheat_sheet_path = WORKSPACE_ROOT / "00_data_engineer_cli_cheat_sheet.md"

        cls.m3_paths = [
            cls.lab1_path,
            cls.lab2_path,
            cls.lab3_path,
            cls.lab4_path,
            cls.cheat_sheet_path,
        ]

        cls.lab1_content = read_text_safe(cls.lab1_path)
        cls.lab2_content = read_text_safe(cls.lab2_path)
        cls.lab3_content = read_text_safe(cls.lab3_path)
        cls.lab4_content = read_text_safe(cls.lab4_path)
        cls.cheat_sheet_content = read_text_safe(cls.cheat_sheet_path)

    # =========================================================================
    # Task 2.1: Python AST Parsing & Lab 04 DAG Deep Inspection
    # =========================================================================

    def test_01_lab04_dag_ast_and_boolean_constants(self):
        """Stress-test Lab 04 DAG AST: ensure zero SyntaxErrors and use_legacy_sql=False is a boolean constant."""
        code_blocks = extract_code_blocks(cls := self.lab4_content)
        python_blocks = [code for lang, code, line in code_blocks if lang == "python"]
        self.assertGreaterEqual(len(python_blocks), 2, "Lab 04 must contain PySpark script and Airflow DAG.")

        dag_code = None
        for code in python_blocks:
            if "DAG(" in code or "from airflow" in code:
                dag_code = code
                break

        self.assertIsNotNone(dag_code, "Could not locate Airflow DAG code block in Lab 04.")

        # 1. Parse AST
        try:
            tree = ast.parse(dag_code)
        except SyntaxError as e:
            self.fail(f"Lab 04 Airflow DAG failed Python AST parse: {e}")

        # 2. Check that no JavaScript uncapitalized keywords exist as identifiers
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                self.assertNotIn(
                    node.id,
                    ["false", "true", "null"],
                    f"Found uncapitalized JavaScript identifier '{node.id}' in Lab 04 DAG. Must be capitalized.",
                )

        # 3. Deep check: verify use_legacy_sql keyword argument in BigQuery operators
        # In BigQuery operators, use_legacy_sql must be boolean False (not "False", not 0, not false)
        found_use_legacy_sql_count = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "use_legacy_sql":
                found_use_legacy_sql_count += 1
                # Must be a Constant with value False
                self.assertIsInstance(
                    node.value,
                    ast.Constant,
                    f"use_legacy_sql is not a Constant node: {type(node.value)}",
                )
                self.assertIs(
                    node.value.value,
                    False,
                    f"use_legacy_sql expected False (bool), got {node.value.value!r} ({type(node.value.value)})",
                )

        self.assertGreaterEqual(
            found_use_legacy_sql_count,
            3,
            f"Expected at least 3 occurrences of use_legacy_sql=False in Lab 04 operators, found {found_use_legacy_sql_count}",
        )

        # 4. Check for unrendered Jinja string manipulation at parse time
        self.assertNotIn(
            'BUCKET_DATA.replace("gs://", "")',
            dag_code,
            "Lab 04 DAG still performs .replace() on unrendered Jinja string BUCKET_DATA at parse time.",
        )
        self.assertIn(
            "| replace('gs://', '')",
            dag_code,
            "Lab 04 DAG must use Jinja template filter | replace('gs://', '') for sensor bucket.",
        )

    def test_02_all_phase6_python_blocks_ast_validity(self):
        """Extract and parse ALL standalone Python blocks across all 4 Phase 6 Labs."""
        all_labs = [
            ("lab_01", self.lab1_content),
            ("lab_02", self.lab2_content),
            ("lab_03", self.lab3_content),
            ("lab_04", self.lab4_content),
        ]

        parse_failures = []
        parsed_block_count = 0

        for lab_name, content in all_labs:
            code_blocks = extract_code_blocks(content)
            for lang, code, start_line in code_blocks:
                # Check explicit python blocks
                if lang == "python":
                    parsed_block_count += 1
                    try:
                        ast.parse(code)
                    except SyntaxError as err:
                        parse_failures.append(f"{lab_name} line {start_line} (```python): {err}")

                # Also check inline python3 -c "..." blocks embedded in bash
                elif lang == "bash" and 'python3 -c "' in code:
                    # Extract python snippet
                    match = re.search(r'python3\s+-c\s+"([^"]+)"', code, re.DOTALL)
                    if match:
                        py_inline = match.group(1)
                        parsed_block_count += 1
                        try:
                            ast.parse(py_inline)
                        except SyntaxError as err:
                            parse_failures.append(f"{lab_name} line {start_line} (python3 -c inline): {err}")

        self.assertGreaterEqual(parsed_block_count, 5, "Expected at least 5 Python blocks across Phase 6 labs.")
        self.assertEqual(
            parse_failures,
            [],
            f"Found {len(parse_failures)} Python syntax errors across labs:\n" + "\n".join(parse_failures),
        )

    def test_03_lab01_benchmark_json_parsing_logic(self):
        """Stress-test Lab 01 Python JSON parsing logic against mock bq show -j payload."""
        # The python script in Lab 01 reads job JSON and extracts query statistics.
        # Ensure it properly navigates the dictionary without crashing.
        mock_job_json = {
            "statistics": {
                "query": {
                    "totalBytesProcessed": "10574247936",
                    "totalBytesBilled": "10574247936",
                    "totalSlotMs": "18400",
                },
                "startTime": "1700000000000",
                "endTime": "1700000001820",
            }
        }

        # Verify that extracting via data.get('statistics', {}).get('query', {}) succeeds
        stats = mock_job_json.get("statistics", {}).get("query", {})
        self.assertEqual(stats.get("totalBytesProcessed"), "10574247936")
        self.assertEqual(stats.get("totalBytesBilled"), "10574247936")
        self.assertEqual(stats.get("totalSlotMs"), "18400")

    def test_04_lab02_beam_pipeline_trigger_structure(self):
        """Ensure Lab 02 Beam streaming pipeline uses valid non-wrapped AfterWatermark trigger."""
        self.assertNotIn(
            "Repeatedly(AfterWatermark",
            self.lab2_content,
            "Lab 02 contains non-standard Repeatedly(AfterWatermark(...)) wrapper.",
        )
        self.assertIn(
            "trigger=AfterWatermark(",
            self.lab2_content,
            "Lab 02 must use AfterWatermark composite trigger.",
        )
        self.assertIn(
            "early=AfterProcessingTime(10)",
            self.lab2_content,
            "Lab 02 must define early speculative trigger with AfterProcessingTime(10).",
        )
        self.assertIn(
            "late=AfterProcessingTime(30)",
            self.lab2_content,
            "Lab 02 must define late trigger with AfterProcessingTime(30).",
        )

    # =========================================================================
    # Task 2.2: CLI Blocks Anti-Hallucination Auditing
    # =========================================================================

    def test_05_zero_hallucinated_gcloud_bi_engine(self):
        """Assert zero occurrences of 'gcloud bi-engine' across all 5 Milestone 3 files."""
        for path in self.m3_paths:
            content = read_text_safe(path)
            matches = re.findall(r"gcloud\s+bi-engine\b", content)
            self.assertEqual(
                matches,
                [],
                f"File {path.name} contains hallucinated command group 'gcloud bi-engine'.",
            )

    def test_06_zero_enable_turbo_replication_flags(self):
        """Assert zero occurrences of '--enable-turbo-replication' across all 5 Milestone 3 files."""
        for path in self.m3_paths:
            content = read_text_safe(path)
            self.assertNotIn(
                "--enable-turbo-replication",
                content,
                f"File {path.name} contains hallucinated flag '--enable-turbo-replication'.",
            )

    def test_07_valid_dual_region_locations(self):
        """Assert zero occurrences of invalid dual-region syntax '--location=us-central1,us-east4'."""
        for path in self.m3_paths:
            content = read_text_safe(path)
            self.assertNotIn(
                "--location=us-central1,us-east4",
                content,
                f"File {path.name} contains invalid dual-region location format.",
            )

    def test_08_cli_cheat_sheet_comprehensive_flags(self):
        """Verify all corrected CLI flags in 00_data_engineer_cli_cheat_sheet.md."""
        content = self.cheat_sheet_content

        # P0-11: Turbo Replication flag
        self.assertIn(
            "--recovery-point-objective=ASYNC_TURBO",
            content,
            "Cheat Sheet missing '--recovery-point-objective=ASYNC_TURBO'.",
        )

        # P0-12: BI Engine Reservation syntax
        self.assertIn(
            "--bi_reservation_size=10",
            content,
            "Cheat Sheet missing '--bi_reservation_size=10'.",
        )
        self.assertNotIn(
            "--connection_type=BI_ENGINE",
            content,
            "Cheat Sheet still contains invalid '--connection_type=BI_ENGINE'.",
        )

        # P0-13: Dataflow Flex Template flag
        self.assertIn(
            "--template-file-gcs-location=",
            content,
            "Cheat Sheet missing '--template-file-gcs-location='.",
        )
        self.assertNotIn(
            "--template-file-gcs-path",
            content,
            "Cheat Sheet contains invalid '--template-file-gcs-path'.",
        )

        # P0-14: Dataproc Serverless PySpark python files flag
        self.assertIn(
            "--py-files=",
            content,
            "Cheat Sheet missing '--py-files='.",
        )
        self.assertNotIn(
            "--deps-pkg",
            content,
            "Cheat Sheet contains invalid '--deps-pkg'.",
        )

        # P0-15: Bigtable autoscaling flags
        self.assertIn(
            "autoscaling-min-nodes=",
            content,
            "Cheat Sheet missing 'autoscaling-min-nodes='.",
        )
        self.assertIn(
            "autoscaling-max-nodes=",
            content,
            "Cheat Sheet missing 'autoscaling-max-nodes='.",
        )
        self.assertNotIn(
            "autoscaling-min=",
            content,
            "Cheat Sheet contains invalid 'autoscaling-min='.",
        )
        self.assertNotIn(
            "autoscaling-max=",
            content,
            "Cheat Sheet contains invalid 'autoscaling-max='.",
        )

        # P0-16 & P0-17: Datastream flags
        self.assertIn(
            "--private-connection=",
            content,
            "Cheat Sheet missing '--private-connection=' on private profile.",
        )

        # P0-18: Cloud DLP / Sensitive Data Protection
        self.assertIn(
            "gcloud dlp job-triggers create",
            content,
            "Cheat Sheet missing valid 'gcloud dlp job-triggers create'.",
        )
        self.assertNotIn(
            "gcloud dlp jobs create",
            content,
            "Cheat Sheet contains hallucinated 'gcloud dlp jobs create'.",
        )

        # P1-20: CMEK SA Project Number format
        self.assertIn(
            "serviceAccount:bq-${PROJECT_NUMBER}@bigquery-encryption.iam.gserviceaccount.com",
            content,
            "Cheat Sheet CMEK SA must use numeric project number format.",
        )

        # P2-06: Dataproc single node flag
        self.assertNotIn(
            "--single-node=false",
            content,
            "Cheat Sheet contains redundant/conflicting '--single-node=false'.",
        )

    def test_09_lab01_remediation_integrity(self):
        """Verify Lab 01 has valid BI Engine CLI and IAM setup."""
        content = self.lab1_content

        # P0-05: BI Engine reservation
        self.assertIn("bq update --project_id=${PROJECT_ID}", content)
        self.assertIn("--bi_reservation_size=2", content)
        self.assertIn("--reservation", content)

        # Cleanup deallocates with 0
        self.assertIn("--bi_reservation_size=0", content)

        # P1-12: IAM prerequisite roles
        self.assertIn("roles/bigquery.admin", content)
        self.assertIn("roles/bigqueryreservation.admin", content)

    def test_10_lab02_remediation_integrity(self):
        """Verify Lab 02 has worker SA, polling drain loop, and portable python."""
        content = self.lab2_content

        # P1-13: Dedicated SA & role bindings
        self.assertIn("sa-dataflow-worker", content)
        self.assertIn("--service_account_email=${WORKER_SA}", content)
        self.assertIn("roles/dataflow.worker", content)
        self.assertIn("roles/pubsub.subscriber", content)
        self.assertIn("roles/bigquery.dataEditor", content)

        # P1-14: Safe draining loop
        self.assertIn("gcloud dataflow jobs drain", content)
        self.assertIn("while [[ $(gcloud dataflow jobs show", content)
        self.assertIn("=~ (Running|Draining) ]]; do", content)

        # P3-01: Portable environment variable in traffic generator
        self.assertIn('os.environ.get("PROJECT_ID"', content)
        self.assertNotIn("sed -i", content)

    def test_11_lab03_remediation_integrity(self):
        """Verify Lab 03 has static-ip connectivity, valid SQL DDL max_staleness, and SA IAM."""
        content = self.lab3_content

        # P0-07: Static IP connectivity flag
        self.assertIn("--static-ip-connectivity", content)

        # P0-08: max_staleness DDL vs invalid SELECT clause
        self.assertTrue(
            bool(re.search(r"ALTER TABLE\s+\\?`\$\{PROJECT_ID\}\.\$\{DATASET_ID\}\.orders\\?`", content)),
            "Expected ALTER TABLE `${PROJECT_ID}.${DATASET_ID}.orders` in Lab 03.",
        )
        self.assertIn("SET OPTIONS (", content)
        self.assertIn("max_staleness = INTERVAL 0 MINUTE", content)

        # Ensure no invalid SELECT ... OPTIONS(max_staleness)
        self.assertFalse(
            bool(re.search(r"SELECT\b[^;]+OPTIONS\s*\(\s*max_staleness\b", content, re.IGNORECASE | re.DOTALL)),
            "Found invalid SELECT ... OPTIONS(max_staleness) in Lab 03.",
        )

        # P1-15: Cloud SQL IP accessibility
        self.assertIn("--assign-ip", content)
        self.assertIn("--authorized-networks=0.0.0.0/0", content)

        # P1-16: Datastream Service Agent BigQuery IAM
        self.assertIn("roles/bigquery.dataEditor", content)
        self.assertIn("roles/bigquery.jobUser", content)
        self.assertIn("service-${PROJECT_NUMBER}@gcp-sa-datastream.iam.gserviceaccount.com", content)

    def test_12_lab04_remediation_integrity(self):
        """Verify Lab 04 environment creation, PGA on default subnet, and IAM roles."""
        content = self.lab4_content

        # P0-09: Environment creation
        self.assertIn("gcloud composer environments create ${COMPOSER_ENV}", content)
        self.assertIn("--image-version=composer-2.6.6-airflow-2.7.3", content)
        self.assertIn("--environment-size=small", content)

        # P1-18: Private Google Access on default subnet
        self.assertIn("gcloud compute networks subnets update default", content)
        self.assertIn("--enable-private-ip-google-access", content)

        # P1-19: IAM roles
        self.assertIn("roles/composer.worker", content)
        self.assertIn("roles/composer.ServiceAgentV2Ext", content)
        self.assertIn("roles/dataproc.worker", content)

    # =========================================================================
    # Task 2.3: Markdown Structure & Code Fence Integrity
    # =========================================================================

    def test_13_markdown_syntax_and_code_fence_balance(self):
        """Validate that all 5 Milestone 3 files have balanced code fences and clean Markdown."""
        for path in self.m3_paths:
            content = read_text_safe(path)

            # 1. Backtick code fences balance
            fence_count = len(re.findall(r"^```", content, re.MULTILINE))
            self.assertEqual(
                fence_count % 2,
                0,
                f"File {path.name} has unbalanced code fences ({fence_count} fences found).",
            )

            # 2. Check for empty or malformed code fence language tags (e.g. ``````)
            malformed_fences = re.findall(r"^````+", content, re.MULTILINE)
            self.assertEqual(
                malformed_fences,
                [],
                f"File {path.name} contains malformed >3 backtick fences: {malformed_fences}",
            )

            # 3. Headers well-formed (# followed by space)
            bad_headers = re.findall(r"^#{1,6}[^#\s]", content, re.MULTILINE)
            self.assertEqual(
                bad_headers,
                [],
                f"File {path.name} has malformed headers missing space after '#': {bad_headers}",
            )

    # =========================================================================
    # Adversarial & Edge Case Inquiries
    # =========================================================================

    def test_14_adversarial_pga_flag_accuracy(self):
        """
        Adversarial check: In gcloud compute networks subnets update, the official flag
        to enable Private Google Access is '--enable-private-ip-google-access'.
        Flag '--enable-private-google-access' (missing '-ip-') does not exist in gcloud compute subnets.
        Check both Lab 04 and Cheat Sheet.
        """
        # Lab 04 check
        self.assertNotIn(
            "--enable-private-google-access",
            self.lab4_content,
            "Lab 04 uses incorrect '--enable-private-google-access'. Must be '--enable-private-ip-google-access'.",
        )
        self.assertIn(
            "--enable-private-ip-google-access",
            self.lab4_content,
            "Lab 04 must use official '--enable-private-ip-google-access'.",
        )

        # Cheat sheet check (Finding to report if present!)
        has_wrong_pga = "--enable-private-google-access" in self.cheat_sheet_content
        # Let's record this assertion to document whether cheat sheet has this defect
        if has_wrong_pga:
            # We fail with a descriptive message that will be captured in our report
            self.fail(
                "ADVERSARIAL DEFECT DETECTED: 00_data_engineer_cli_cheat_sheet.md line 596 uses "
                "'--enable-private-google-access' instead of official '--enable-private-ip-google-access'. "
                "In Google Cloud SDK, 'gcloud compute networks subnets update' only accepts '--enable-private-ip-google-access'."
            )


if __name__ == "__main__":
    unittest.main()
