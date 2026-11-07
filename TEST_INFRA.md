# E2E Test Infrastructure Specification: GCP PDE Handbook Audit & Remediation

## 1. Test Philosophy & Principles

The End-to-End (E2E) Test Suite for the Google Cloud Professional Data Engineer (PDE) Handbook Audit and Remediation project is built upon rigorous, automated, opaque-box testing principles:

1. **Opaque-Box & Requirement-Driven**:
   Tests are derived strictly from user requirements and acceptance criteria specified in `ORIGINAL_REQUEST.md` and `PROJECT.md`. Tests validate *what* deliverables and technical guarantees must exist, not *how* an agent produced them.
2. **Authoritative Expected Output Derivation**:
   Every test case verifies outputs against authoritative ground truth:
   - The Official Google Cloud Professional Data Engineer Exam Guide (v4.2).
   - Official Google Cloud Documentation (`https://cloud.google.com/`).
   - Ground-truth repository census (100% of 49 Markdown files).
3. **Zero-Dependency Portability**:
   The entire test harness and modular test suites are implemented in standard Python (3.10+ / 3.14+) using the built-in `unittest` framework and standard library modules (`ast`, `re`, `pathlib`, `json`, `urllib`). No external pip dependencies are required to run the full E2E test harness.
4. **Independent Execution & Determinism**:
   Each test case sets up its own state, performs read-only inspection against the target workspace, and does not depend on execution order or side-effects. Non-deterministic elements (such as execution timestamps) use regex matching.
5. **No Facade Tests**:
   Tests execute real file parsing, link graph traversal, AST syntax validation, regex pattern matching, and schema verification. Tests fail when requirements are not satisfied.

---

## 2. Tiered Testing Architecture & Feature Inventory Mapping

The test harness organizes all project features into four progressive verification tiers:

```
+-------------------------------------------------------------------------+
|                       E2E TEST HARNESS SUITE                            |
+-------------------------------------------------------------------------+
       |
       +---> Tier 1: Feature Coverage (Core Requirements & Census)
       |      - 100% File Census (49 files) in FILE_BY_FILE_FINDINGS.md
       |      - Documentation Citation Completeness (cloud.google.com URLs)
       |      - Performance Claim Classification (Categories A, B, C, D)
       |      - Exam Guide 19-Objective Mapping (FC, PC, M, TI)
       |      - Mock Question Quality & Explanations (30+ questions)
       |      - Audit Deliverables Existence (7 files in docs/audits/)
       |      - Root Governance Files Existence (AUDIT_REPORT, CHANGELOG, SOURCES)
       |
       +---> Tier 2: Boundary & Corner Cases (Integrity & Formatting)
       |      - Git Tracking (.gitignore must not ignore docs/)
       |      - Internal Relative Link Graph (0 broken links across 49 files)
       |      - Markdown Syntax & Code Block Fence Balance
       |
       +---> Tier 3: Cross-Feature & Traceability (Coherence & Ledger)
       |      - Exam Matrix vs Exam Coverage Tracker Synchronization
       |      - Remediation Backlog P0/P1 Traceability in CHANGELOG.md
       |      - Official Documentation Citation URL Resolution Consistency
       |
       +---> Tier 4: Real-World & Execution Scenarios (Static AST & CLI)
              - Python AST Syntax Validation (Labs 01-04 scripts & Composer DAGs)
              - CLI Command Static Linting (gcloud, bq, cbt hallucination checks)
              - SQL DDL / DML Syntax Integrity
```

### 2.1 Feature Inventory to Tier Mapping

| Feature # | Feature Name | Requirement Source | Verification Tier | Test Suite File |
|:---:|:---|:---|:---:|:---|
| F-01 | 100% Repository Census Evaluation | R1, M1, Survey | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-02 | Official GCP Documentation Citations | R1, Acceptance | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-03 | Performance Claim Classification (A, B, C, D) | R2, M1 | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-04 | Exam Blueprint 19-Objective Mapping | R2, Survey | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-05 | Mock Exam Structure & Explanations | R2, Survey | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-06 | Stage 1 Audit Deliverables (7 Files) | R3, Acceptance | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-07 | Root Governance Files (3 Files) | R3, Acceptance | Tier 1 | `tests/test_tier1_feature_coverage.py` |
| F-08 | Git Tracking Configuration (`.gitignore`) | R3, Survey | Tier 2 | `tests/test_tier2_boundary_corner.py` |
| F-09 | Repository Relative Link Integrity | Acceptance, Survey | Tier 2 | `tests/test_tier2_boundary_corner.py` |
| F-10 | Markdown Formatting & Fence Balancing | Acceptance, R4 | Tier 2 | `tests/test_tier2_boundary_corner.py` |
| F-11 | Matrix vs Tracker Cross-Consistency | R3, R4 | Tier 3 | `tests/test_tier3_traceability.py` |
| F-12 | Backlog P0/P1 to CHANGELOG Traceability | R4, Acceptance | Tier 3 | `tests/test_tier3_traceability.py` |
| F-13 | Python Lab Code AST Parsing | R2, R4 | Tier 4 | `tests/test_tier4_execution_scenarios.py` |
| F-14 | CLI Anti-Hallucination & Flag Auditing | R1, R2, R4 | Tier 4 | `tests/test_tier4_execution_scenarios.py` |

---

## 3. Directory Layout & Test Suite Organization

```
e:\gcp_pde_revamp\
├── TEST_INFRA.md                   # This specification
├── TEST_READY.md                   # Execution summary & readiness publication
├── tests/
│   ├── __init__.py                 # Python package marker
│   ├── conftest_base.py            # Shared test base class, census constants, helper utilities
│   ├── test_tier1_feature_coverage.py      # Tier 1: Feature coverage test suite
│   ├── test_tier2_boundary_corner.py       # Tier 2: Boundary and corner cases test suite
│   ├── test_tier3_traceability.py          # Tier 3: Cross-feature traceability test suite
│   ├── test_tier4_execution_scenarios.py   # Tier 4: Real-world syntax and CLI scenarios
│   ├── run_e2e_tests.py            # Master test runner CLI and JSON report generator
│   └── e2e_test_report.json        # Structured machine-readable test execution output
```

---

## 4. Tiered Coverage Thresholds

1. **Tier 1 — Feature Coverage**:
   - $\ge 5$ discrete test cases per major feature or comprehensive census validation across all 49 files and 19 exam objectives.
   - 100% coverage of required deliverables:
     - 7 audit reports in `docs/audits/`
     - 3 root governance files (`AUDIT_REPORT.md`, `CHANGELOG.md`, `SOURCES.md`)
     - 4 performance claim categories (A, B, C, D)
     - 4 exam objective statuses (FC, PC, M, TI)
2. **Tier 2 — Boundary & Corner Cases**:
   - Evaluates link target combinations (anchors, relative directories `../`, parent folder traversal).
   - Validates code block fence delimiters across 316+ blocks in 49 files.
   - Verifies `.gitignore` pattern parsing (ensuring `docs/` is not matched).
3. **Tier 3 — Cross-Feature Traceability**:
   - 100% pairwise consistency between `docs/audits/EXAM_COVERAGE_MATRIX.md` and `08_phase7_mock_exam_engine/exam_coverage_tracker.md`.
   - 100% of P0 and P1 issue identifiers from `REMEDIATION_BACKLOG.md` accounted for in `CHANGELOG.md`.
4. **Tier 4 — Real-World Scenarios & Static Execution**:
   - 100% AST parse success on Python scripts and DAGs in Phase 6 Labs (`lab_01`, `lab_02`, `lab_04`).
   - Zero hallucinated CLI command patterns (`gcloud bi-engine`, `--enable-turbo-replication`, `use_legacy_sql=false`).
   - BigQuery SQL `OPTIONS` positioning validation.

---

## 5. Test Execution Instructions

### Master E2E Runner (Recommended)
Run the automated test runner to execute all tiers, display formatted results, and write `tests/e2e_test_report.json`:
```bash
python tests/run_e2e_tests.py
```

### Filtering by Tier
Run specific test tiers using the runner CLI:
```bash
python tests/run_e2e_tests.py --tier 1
python tests/run_e2e_tests.py --tier 2
python tests/run_e2e_tests.py --tier 3
python tests/run_e2e_tests.py --tier 4
```

### Running with Standard Python `unittest`
Any standard test runner can execute individual test suites:
```bash
python -m unittest discover -s tests -p "test_*.py" -v
python -m unittest tests/test_tier1_feature_coverage.py -v
python -m unittest tests/test_tier2_boundary_corner.py -v
python -m unittest tests/test_tier3_traceability.py -v
python -m unittest tests/test_tier4_execution_scenarios.py -v
```

---

## 6. Test Reporting & Machine-Readable Output

The runner outputs structured JSON to `tests/e2e_test_report.json` with the following schema:
```json
{
  "timestamp": "2026-11-07T...",
  "total_tests": 35,
  "passed": 30,
  "failed": 5,
  "errors": 0,
  "skipped": 0,
  "pass_rate_percentage": 85.7,
  "tiers": {
    "tier_1": { "total": 12, "passed": 10, "failed": 2, "errors": 0 },
    "tier_2": { "total": 8, "passed": 7, "failed": 1, "errors": 0 },
    "tier_3": { "total": 5, "passed": 4, "failed": 1, "errors": 0 },
    "tier_4": { "total": 10, "passed": 9, "failed": 1, "errors": 0 }
  },
  "results": [
    {
      "test_id": "tests.test_tier1_feature_coverage.TestFeatureCoverage.test_repository_file_census",
      "tier": "Tier 1",
      "status": "PASS",
      "description": "Asserts 100% of 49 files are evaluated in FILE_BY_FILE_FINDINGS.md",
      "details": ""
    }
  ]
}
```
