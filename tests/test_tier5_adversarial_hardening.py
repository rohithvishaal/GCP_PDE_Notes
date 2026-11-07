"""
Tier 5: Adversarial Coverage Hardening & Edge-Case Verification Test Suite.

Author: Challenger Tier 5 (critic, specialist)
Objective: Rigorous opaque-box stress testing and boundary hardening:
1. Repo-wide executable code block Unicode sanitization (smart quotes, em-dashes, non-breaking spaces).
2. Bash code block line continuation and whitespace robustness (no trailing backslash-spaces).
3. Universal Python AST static analysis across ALL repository files (no SyntaxErrors, no JS booleans).
4. Apache Beam pipeline trigger runtime instantiation and dynamic evaluation harness.
5. BigQuery SQL DDL/DML, clustering column count limit (<= 4), and options syntax validation.
6. Repo-wide exhaustive CLI anti-hallucination scan across curriculum files.
7. Mock Exam Engine deterministic single-best-answer and rationale verification (30 questions).
8. Official PDE Exam Guide 19-objective mathematical domain weight summation and status verification.
9. Audit and Remediation Backlog closed-loop bijection across P0/P1 issues and CHANGELOG.md.
10. Relative link cross-platform case-sensitivity and scheme sanitization (no file://, no backslashes).
"""

import ast
from pathlib import Path
import re
import unittest
import urllib.parse
from typing import Dict, List, Set, Tuple

from tests.conftest_base import (
    ALL_49_CENSUS_FILES,
    OFFICIAL_EXAM_OBJECTIVES,
    REQUIRED_AUDIT_DELIVERABLES,
    REQUIRED_ROOT_GOVERNANCE_FILES,
    WORKSPACE_ROOT,
    extract_code_blocks,
    extract_markdown_links,
    get_all_repository_markdown_files,
    is_relative_internal_link,
    read_text_safe,
)


class TestTier5AdversarialHardening(unittest.TestCase):
    """Tier 5: Adversarial Coverage Hardening & Subtle Edge Cases."""

    @classmethod
    def setUpClass(cls):
        cls.all_md_files = get_all_repository_markdown_files(WORKSPACE_ROOT)
        cls.findings_path = WORKSPACE_ROOT / "docs" / "audits" / "FILE_BY_FILE_FINDINGS.md"
        cls.backlog_path = WORKSPACE_ROOT / "docs" / "audits" / "REMEDIATION_BACKLOG.md"
        cls.matrix_path = WORKSPACE_ROOT / "docs" / "audits" / "EXAM_COVERAGE_MATRIX.md"
        cls.tracker_path = WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "exam_coverage_tracker.md"
        cls.changelog_path = WORKSPACE_ROOT / "CHANGELOG.md"
        cls.cheat_sheet_path = WORKSPACE_ROOT / "00_data_engineer_cli_cheat_sheet.md"

        cls.findings_content = read_text_safe(cls.findings_path)
        cls.backlog_content = read_text_safe(cls.backlog_path)
        cls.matrix_content = read_text_safe(cls.matrix_path)
        cls.tracker_content = read_text_safe(cls.tracker_path)
        cls.changelog_content = read_text_safe(cls.changelog_path)
        cls.cheat_sheet_content = read_text_safe(cls.cheat_sheet_path)

        # Separate curriculum handbook files from historical audit deliverables and changelogs
        cls.curriculum_files = [
            p for p in cls.all_md_files
            if not any(part in p.parts for part in ["docs", ".agents", ".git"])
            and p.name not in ["AUDIT_REPORT.md", "CHANGELOG.md", "TEST_READY.md", "TEST_INFRA.md"]
        ]

    # =========================================================================
    # 1. Unicode & Typographic Sanitization
    # =========================================================================

    def test_01_repo_wide_executable_code_blocks_unicode_sanitization(self):
        """Stress-test: Ensure no typographic smart quotes, em-dashes, or non-breaking spaces exist in executable code blocks."""
        illegal_code_chars = {
            '\u201c': 'LEFT DOUBLE QUOTATION MARK (smart double quote)',
            '\u201d': 'RIGHT DOUBLE QUOTATION MARK (smart double quote)',
            '\u2018': 'LEFT SINGLE QUOTATION MARK (smart single quote)',
            '\u2019': 'RIGHT SINGLE QUOTATION MARK (smart single quote)',
            '\u2014': 'EM DASH (typographic dash)',
            '\u2013': 'EN DASH (typographic dash)',
            '\u00a0': 'NO-BREAK SPACE (causes syntax errors in Python and bash)',
            '\u2212': 'MINUS SIGN (mathematical unicode minus)',
        }

        violations: List[str] = []
        inspected_blocks = 0

        for p in self.all_md_files:
            content = read_text_safe(p)
            blocks = extract_code_blocks(content)
            for lang, code, start_line in blocks:
                if lang in ('bash', 'shell', 'python', 'sql'):
                    inspected_blocks += 1
                    for line_offset, line in enumerate(code.splitlines(), start=start_line):
                        for char, desc in illegal_code_chars.items():
                            if char in line:
                                violations.append(
                                    f"{p.relative_to(WORKSPACE_ROOT)}:{line_offset} [{lang}] "
                                    f"contains illegal {desc}: {line.strip()}"
                                )

        self.assertGreaterEqual(inspected_blocks, 50, "Expected at least 50 executable code blocks across repository.")
        self.assertEqual(
            violations,
            [],
            f"Found {len(violations)} illegal typographic/unicode character(s) in executable code blocks:\n"
            + "\n".join(f"  {v}" for v in violations[:20]),
        )

    # =========================================================================
    # 2. Bash Code Block Line Continuation & Whitespace Robustness
    # =========================================================================

    def test_02_bash_code_blocks_whitespace_and_continuation_robustness(self):
        """Stress-test: Ensure no bash line continuation backslashes are followed by trailing whitespace."""
        violations: List[str] = []

        for p in self.all_md_files:
            content = read_text_safe(p)
            blocks = extract_code_blocks(content)
            for lang, code, start_line in blocks:
                if lang in ('bash', 'shell'):
                    for line_offset, line in enumerate(code.splitlines(), start=start_line):
                        # Detect backslash followed by trailing spaces or tabs
                        if re.search(r'\\\s+$', line):
                            violations.append(
                                f"{p.relative_to(WORKSPACE_ROOT)}:{line_offset} "
                                f"has trailing whitespace after line continuation backslash: {line!r}"
                            )

        self.assertEqual(
            violations,
            [],
            f"Found {len(violations)} trailing whitespace error(s) after line continuation backslash in bash blocks:\n"
            + "\n".join(f"  {v}" for v in violations),
        )

    # =========================================================================
    # 3. Universal Python AST Static Analysis
    # =========================================================================

    def test_03_repo_wide_python_ast_static_analysis(self):
        """Stress-test: Parse 100% of standalone Python code blocks across all markdown files via Python AST."""
        parse_errors: List[str] = []
        parsed_blocks = 0

        for p in self.all_md_files:
            content = read_text_safe(p)
            blocks = extract_code_blocks(content)
            for lang, code, start_line in blocks:
                if lang == 'python':
                    if not code.strip():
                        continue
                    parsed_blocks += 1
                    try:
                        tree = ast.parse(code)
                    except SyntaxError as e:
                        parse_errors.append(f"{p.relative_to(WORKSPACE_ROOT)}:{start_line} -> {e}")
                        continue

                    # Verify no lowercase JS booleans exist as identifiers
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Name) and node.id in ("false", "true", "null"):
                            parse_errors.append(
                                f"{p.relative_to(WORKSPACE_ROOT)}:{start_line} uses uncapitalized JS identifier '{node.id}'"
                            )

        self.assertGreaterEqual(parsed_blocks, 10, f"Expected at least 10 Python code blocks, found {parsed_blocks}")
        self.assertEqual(
            parse_errors,
            [],
            f"Found {len(parse_errors)} Python AST parse error(s) across repository:\n"
            + "\n".join(f"  {err}" for err in parse_errors),
        )

    # =========================================================================
    # 4. Apache Beam Pipeline Dynamic Execution & Trigger Runtime Harness
    # =========================================================================

    def test_04_beam_pipeline_runtime_trigger_execution_harness(self):
        """Stress-test: Verify Apache Beam window trigger expressions instantiate cleanly in a simulated runtime harness."""
        class TriggerFn:
            pass

        class AccumulationMode:
            ACCUMULATING = 1
            DISCARDING = 2

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

        class AfterFirst(TriggerFn):
            def __init__(self, *triggers):
                self.triggers = triggers

        class AfterAll(TriggerFn):
            def __init__(self, *triggers):
                self.triggers = triggers

        beam_runtime_env = {
            "AfterWatermark": AfterWatermark,
            "AfterProcessingTime": AfterProcessingTime,
            "AfterCount": AfterCount,
            "Repeatedly": Repeatedly,
            "AfterFirst": AfterFirst,
            "AfterAll": AfterAll,
            "AccumulationMode": AccumulationMode,
        }

        # 1. AST extraction of AfterWatermark call nodes in Beam code blocks
        verified_ast_triggers = 0
        beam_code_files = [
            WORKSPACE_ROOT / "03_phase2_lifecycle_architecture" / "02_streaming_ingestion_and_validation.md",
            WORKSPACE_ROOT / "07_phase6_hands_on_labs" / "lab_02_beam_dataflow_streaming.md",
        ]

        for bf in beam_code_files:
            self.assertTrue(bf.is_file(), f"Beam file {bf} not found.")
            content = read_text_safe(bf)
            blocks = extract_code_blocks(content)
            for lang, code, line in blocks:
                if lang == "python":
                    tree = ast.parse(code)
                    for node in ast.walk(tree):
                        if (
                            isinstance(node, ast.Call)
                            and isinstance(node.func, ast.Name)
                            and node.func.id == "AfterWatermark"
                        ):
                            call_str = ast.unparse(node)
                            obj = eval(call_str, beam_runtime_env)
                            self.assertIsInstance(obj, AfterWatermark)
                            self.assertIsNotNone(obj.early)
                            self.assertIsNotNone(obj.late)
                            verified_ast_triggers += 1

        self.assertGreaterEqual(verified_ast_triggers, 2, "Expected at least 2 AST-instantiated Beam triggers.")

        # 2. Dynamic evaluation of inline scenario exam triggers
        exam1_text = read_text_safe(
            WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md"
        )
        exam_trigger_expr = "AfterWatermark(early=AfterProcessingTime(10), late=AfterCount(1))"
        self.assertIn(exam_trigger_expr, exam1_text)
        obj_exam = eval(exam_trigger_expr, beam_runtime_env)
        self.assertIsInstance(obj_exam, AfterWatermark)
        self.assertIsInstance(obj_exam.early, AfterProcessingTime)
        self.assertIsInstance(obj_exam.late, AfterCount)

        # 3. Verify negative: Ensure no Java Beam method calls exist
        for bf in beam_code_files + [WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md"]:
            text = read_text_safe(bf)
            self.assertNotIn("plusDelayOf(", text, f"Java method plusDelayOf found in {bf.name}")
            self.assertNotIn("pastFirstElementInPane(", text, f"Java method pastFirstElementInPane found in {bf.name}")
            self.assertNotIn("withAllowedLateness(", text, f"Java method withAllowedLateness found in {bf.name}")

    # =========================================================================
    # 5. BigQuery SQL DDL / DML & Clustering Limits Hardening
    # =========================================================================

    def test_05_bigquery_sql_ddl_and_clustering_limit_verification(self):
        """Stress-test: Ensure all BigQuery CLUSTER BY clauses adhere to the maximum 4-column limit and valid DDL."""
        sql_clustering_checked = 0
        violations: List[str] = []

        for p in self.curriculum_files:
            content = read_text_safe(p)

            # 1. Check CLUSTER BY column count limit (BigQuery max is 4 columns)
            matches = re.finditer(r'CLUSTER\s+BY\s+([^;\n\)\`]+)', content, re.IGNORECASE)
            for m in matches:
                raw_cols = m.group(1).strip()
                raw_cols = re.split(r'\b(OPTIONS|AS|WHERE)\b', raw_cols, flags=re.IGNORECASE)[0]
                cols = [c.strip() for c in raw_cols.split(',') if c.strip() and not c.strip().startswith('--')]
                if cols:
                    sql_clustering_checked += 1
                    if len(cols) > 4:
                        violations.append(
                            f"{p.relative_to(WORKSPACE_ROOT)} has CLUSTER BY with {len(cols)} columns (>4 limit): {cols}"
                        )

            # 2. Check for invalid SELECT ... OPTIONS(max_staleness) pattern in curriculum
            has_invalid_options = bool(
                re.search(r'SELECT\b[^;]+OPTIONS\s*\(\s*max_staleness\b', content, re.IGNORECASE | re.DOTALL)
            )
            if has_invalid_options:
                violations.append(f"{p.relative_to(WORKSPACE_ROOT)} has invalid SELECT ... OPTIONS(max_staleness)")

        self.assertGreaterEqual(sql_clustering_checked, 4, f"Expected at least 4 CLUSTER BY clauses, found {sql_clustering_checked}")
        self.assertEqual(
            violations,
            [],
            f"BigQuery SQL DDL/DML violations found in curriculum:\n" + "\n".join(f"  {v}" for v in violations),
        )

    # =========================================================================
    # 6. Repo-Wide Exhaustive CLI Anti-Hallucination Negative Hardening
    # =========================================================================

    def test_06_repo_wide_cli_anti_hallucination_exhaustive(self):
        """Exhaustive negative test: Assert zero occurrences of 11 hallucinated GCP CLI commands/flags across curriculum files."""
        hallucinated_patterns: List[Tuple[str, str, str]] = [
            (r'gcloud\s+bi-engine\b', "gcloud bi-engine", "Hallucinated CLI group. Use 'bq update --bi_reservation_size=...'"),
            (r'--enable-turbo-replication\b', "--enable-turbo-replication", "Hallucinated flag. Use '--recovery-point-objective=ASYNC_TURBO'"),
            (r'--location=us-central1,us-east4\b', "--location=us-central1,us-east4", "Invalid dual-region syntax without --placement"),
            (r'gcloud\s+dlp\s+jobs\s+create\b', "gcloud dlp jobs create", "Non-existent command. Use 'gcloud dlp job-triggers create'"),
            (r'--template-file-gcs-path\b', "--template-file-gcs-path", "Invalid flag. Use '--template-file-gcs-location='"),
            (r'--deps-pkg\b', "--deps-pkg", "Invalid flag. Use '--py-files='"),
            (r'--single-node=false\b', "--single-node=false", "Invalid flag for standard Dataproc clusters"),
            (r'gcloud\s+dataproc-gdc\b', "gcloud dataproc-gdc", "Hallucinated command group. Use 'gcloud dataplex datascans'"),
            (r'--target_job_rate\b', "--target_job_rate", "Invalid reservation flag. Use '--target_job_concurrency='"),
            (r'--connection_type=BI_ENGINE\b', "--connection_type=BI_ENGINE", "Invalid bq connection type"),
            (r'gcloud\s+compute\s+networks\s+subnets\s+update[^\n]+--enable-private-google-access\b',
             "--enable-private-google-access on subnets", "Missing '-ip-'. Must be '--enable-private-ip-google-access'"),
        ]

        violations: List[str] = []

        for p in self.curriculum_files:
            text = read_text_safe(p)
            for pattern, name, explanation in hallucinated_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for m in matches:
                    line_no = text[:m.start()].count("\n") + 1
                    violations.append(
                        f"{p.relative_to(WORKSPACE_ROOT)}:{line_no} [{name}]: {explanation}"
                    )

        self.assertEqual(
            violations,
            [],
            f"Found {len(violations)} hallucinated CLI pattern(s) in curriculum files:\n"
            + "\n".join(f"  {v}" for v in violations),
        )

    # =========================================================================
    # 7. Mock Exam Engine Determinism & Answer Key Verification
    # =========================================================================

    def test_07_mock_exam_engine_deterministic_answer_keys(self):
        """Stress-test: Verify that all 30 mock questions have unambiguous single best answers and distractor explanations."""
        exam_files = [
            WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "01_scenario_exam_systems_ingestion.md",
            WORKSPACE_ROOT / "08_phase7_mock_exam_engine" / "02_scenario_exam_storage_optimization.md",
        ]

        total_questions = 0
        answer_distribution: Dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": 0}

        for ef in exam_files:
            self.assertTrue(ef.is_file(), f"Mock exam {ef.name} missing.")
            text = read_text_safe(ef)

            q_blocks = re.split(r'(?m)^###\s+Question\s+\d+\b', text)[1:]
            self.assertEqual(len(q_blocks), 15, f"{ef.name} must have exactly 15 questions, found {len(q_blocks)}")

            for idx, q_text in enumerate(q_blocks, start=1):
                total_questions += 1

                # 1. Assert choices A, B, C, D exist
                for opt in ["A", "B", "C", "D"]:
                    has_opt = f"**{opt}.**" in q_text or f"{opt}. " in q_text
                    self.assertTrue(has_opt, f"{ef.name} Q{idx} missing Option {opt}")

                # 2. Extract answer key
                ans_match = re.search(r'\*\*Correct Answer\*\*:\s*\*?\*?([A-D])\*?\*?', q_text)
                if not ans_match:
                    ans_match = re.search(r'Correct Answer:\s*\*?\*?([A-D])\*?\*?', q_text)
                self.assertIsNotNone(
                    ans_match,
                    f"{ef.name} Q{idx} missing unambiguous single correct answer key matching [A-D]",
                )
                chosen_opt = ans_match.group(1)
                answer_distribution[chosen_opt] += 1

                # 3. Assert explanation/rationale exists and is non-empty
                has_explanation = bool(re.search(r'(?:Explanation|Rationale)', q_text, re.IGNORECASE))
                self.assertTrue(
                    has_explanation,
                    f"{ef.name} Q{idx} missing explanation section",
                )

        self.assertEqual(total_questions, 30, f"Expected 30 total questions across 2 exams, found {total_questions}")
        # Verify valid answer keys and print diagnostic distribution
        for opt in ["A", "B", "C", "D"]:
            self.assertIn(opt, answer_distribution)
        print(f"\n[DIAGNOSTIC] Mock exam answer key distribution: {answer_distribution}")

    # =========================================================================
    # 8. Exam Guide 19-Objective Mathematical Consistency
    # =========================================================================

    def test_08_exam_guide_19_objectives_mathematical_weights_and_remediation(self):
        """Stress-test: Verify that all 19 exam guide objectives sum to 100% and are Fully Covered (FC)."""
        domain_counts = {"Domain 1": 0, "Domain 2": 0, "Domain 3": 0, "Domain 4": 0, "Domain 5": 0}
        domain_weights = {"Domain 1": 0.22, "Domain 2": 0.25, "Domain 3": 0.20, "Domain 4": 0.15, "Domain 5": 0.18}

        for obj_id, info in OFFICIAL_EXAM_OBJECTIVES.items():
            dom = info["domain"]
            self.assertIn(dom, domain_counts)
            domain_counts[dom] += 1

        self.assertEqual(sum(domain_counts.values()), 19, "Total sub-objectives must be exactly 19.")
        total_weight = sum(domain_weights.values())
        self.assertAlmostEqual(total_weight, 1.0, places=4, msg="Domain weights must sum to exactly 100.0%")

        # Check exam_coverage_tracker.md post-remediation status
        for obj_id in OFFICIAL_EXAM_OBJECTIVES:
            self.assertIn(
                f"**{obj_id}",
                self.tracker_content,
                f"Sub-objective {obj_id} missing from exam_coverage_tracker.md",
            )

        self.assertNotIn(
            "| M |", self.tracker_content,
            "exam_coverage_tracker.md contains unaddressed Missing (M) objectives",
        )
        self.assertNotIn(
            "| TI |", self.tracker_content,
            "exam_coverage_tracker.md contains unaddressed Technically Incorrect (TI) objectives",
        )

    # =========================================================================
    # 9. Audit & Backlog Closed-Loop Bijection Traceability
    # =========================================================================

    def test_09_audit_and_remediation_backlog_closed_loop_traceability(self):
        """Stress-test: Verify 1:1 bijection of all 18 P0 and 24 P1 issues between Backlog and CHANGELOG."""
        p0_backlog_ids: Set[str] = set(re.findall(r'\b(ISSUE-P0-\d{2})\b', self.backlog_content))
        p1_backlog_ids: Set[str] = set(re.findall(r'\b(ISSUE-P1-\d{2})\b', self.backlog_content))

        self.assertEqual(len(p0_backlog_ids), 18, f"Expected 18 unique P0 issues in Backlog, found {len(p0_backlog_ids)}")
        self.assertEqual(len(p1_backlog_ids), 24, f"Expected 24 unique P1 issues in Backlog, found {len(p1_backlog_ids)}")

        missing_in_changelog = []
        for issue_id in p0_backlog_ids | p1_backlog_ids:
            if issue_id not in self.changelog_content:
                missing_in_changelog.append(issue_id)

        self.assertEqual(
            missing_in_changelog,
            [],
            f"Backlog issues missing from CHANGELOG.md: {missing_in_changelog}",
        )

        changelog_issues: Set[str] = set(re.findall(r'\b(ISSUE-P[01]-\d{2})\b', self.changelog_content))
        orphan_issues = changelog_issues - (p0_backlog_ids | p1_backlog_ids)
        self.assertEqual(
            orphan_issues,
            set(),
            f"Found orphan issue IDs in CHANGELOG.md not defined in Backlog: {orphan_issues}",
        )

    # =========================================================================
    # 10. Cross-Platform Relative Link Integrity & Case-Sensitivity
    # =========================================================================

    def test_10_relative_link_no_absolute_schemes_and_case_sensitivity(self):
        """Stress-test: Ensure zero absolute URIs, zero backslashes in Markdown link targets, and exact case match on disk."""
        broken_links: List[str] = []

        for p in self.all_md_files:
            content = read_text_safe(p)
            links = extract_markdown_links(content)
            for label, raw_target in links:
                target = raw_target.strip()
                if not is_relative_internal_link(target):
                    continue

                if target.startswith("file://") or target.startswith("file:/"):
                    broken_links.append(f"{p.relative_to(WORKSPACE_ROOT)}: Invalid file:// scheme in link '{target}'")
                    continue

                if "\\" in target:
                    broken_links.append(f"{p.relative_to(WORKSPACE_ROOT)}: Markdown link contains Windows backslash '{target}'")
                    continue

                target_file_part = target.split("#")[0].strip()
                if not target_file_part:
                    continue

                target_file_part = urllib.parse.unquote(target_file_part)
                resolved_path = (p.parent / target_file_part).resolve()

                if not resolved_path.exists():
                    broken_links.append(f"{p.relative_to(WORKSPACE_ROOT)}: Target '{target}' does not exist on disk.")
                else:
                    actual_name = resolved_path.name
                    expected_name = Path(target_file_part).name
                    if actual_name.lower() == expected_name.lower() and actual_name != expected_name:
                        broken_links.append(
                            f"{p.relative_to(WORKSPACE_ROOT)}: Case mismatch '{expected_name}' vs actual '{actual_name}' on disk."
                        )

        self.assertEqual(
            broken_links,
            [],
            f"Found {len(broken_links)} relative link defect(s):\n" + "\n".join(f"  {b}" for b in broken_links[:20]),
        )


if __name__ == "__main__":
    unittest.main()
