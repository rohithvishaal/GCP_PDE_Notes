# Documentation Improvement Plan & Technical Quality Governance

**Project**: Google Cloud Professional Data Engineer (PDE) Handbook Quality Governance  
**Deliverable**: Stage 1 Documentation Improvement Plan  
**Location**: `docs/audits/DOCUMENTATION_IMPROVEMENT_PLAN.md`  
**Standard**: Google Cloud Documentation Style Guide, Production Code Quality Standards, and Automated CI/CD Verification  
**Date**: November 2026  

---

## 1. Purpose & Strategic Objectives

The purpose of this **Documentation Improvement Plan** is to establish permanent quality gates, rigorous style guide rules, and continuous maintenance procedures for the Google Cloud Professional Data Engineer (PDE) Handbook repository. 

As an enterprise certification preparation curriculum and practitioner reference guide, the handbook must satisfy the highest standards of technical fidelity, syntactic accuracy, and architectural authority. This plan ensures that defects discovered during the Stage 1 audit (such as hallucinated CLI commands, broken relative links, and unsupported SLA claims) are systematically prevented from recurring.

---

## 2. Technical Quality Gates (Pre-Commit & CI/CD Gates)

Every proposed modification or contribution to the repository must satisfy five mandatory quality gates prior to merging:

```
+---------------------------------------------------------------------------------+
|                        MANDATORY REPOSITORY QUALITY GATES                       |
+---------------------------------------------------------------------------------+
| Gate 1: Authoritative Citation Gate                                             |
|         - Every technical claim, quota, or CLI flag must cite official          |
|           Google Cloud documentation (cloud.google.com/...) with specific anchor|
+---------------------------------------------------------------------------------+
| Gate 2: SLA vs. SLO Typology Gate                                               |
|         - Quantitative claims must conform to Category A, B, C, or D typology. |
|         - Zero "100% SLA" claims; operational latency must not be labeled "SLA" |
+---------------------------------------------------------------------------------+
| Gate 3: CLI & Anti-Hallucination Gate                                           |
|         - Commands (gcloud, bq, cbt, gsutil) must be verified against current    |
|           SDK reference documentation; zero non-existent command groups or flags|
+---------------------------------------------------------------------------------+
| Gate 4: Code Block Static AST & Lint Gate                                       |
|         - Standalone Python code must parse via ast.parse() with 0 SyntaxErrors.|
|         - Python booleans must be capitalized (True / False; no lowercase JS).  |
+---------------------------------------------------------------------------------+
| Gate 5: Zero Broken Internal Links Gate                                         |
|         - Internal Markdown links must resolve to existing files on disk.       |
|         - Anti-pattern: Absolute machine file:/// URIs are strictly rejected.   |
+---------------------------------------------------------------------------------+
```

### 2.1 Gate 1: Authoritative Citation Gate
- **Rule**: No technical assertion regarding pricing, availability, limits, or parameters may rely on secondary blogs or unverified forum posts.
- **Enforcement**: Commits and pull requests introducing new architectural claims must include corresponding entries in root `SOURCES.md` and reference canonical `https://cloud.google.com/` URLs.

### 2.2 Gate 2: SLA vs. SLO Typology Gate
- **Rule**: The term **Service Level Agreement (SLA)** is strictly reserved for Google Cloud contractual commitments backed by financial service credits (Category A).
- **Enforcement**:
  - Operational targets, point read latencies, and internal benchmarks must be labeled **Service Level Objectives (SLOs)** or **Performance Targets** (Category C).
  - Claims asserting "100% SLA" (e.g. Turbo Replication) are rejected as Category D defects.

### 2.3 Gate 3: CLI & Anti-Hallucination Gate
- **Rule**: Command-line snippets must use official Google Cloud SDK command groups, subcommands, and flags.
- **Enforcement**:
  - Prohibit non-existent command groups (e.g., `gcloud bi-engine`, `gcloud dataproc-gdc`).
  - Prohibit invalid flags (e.g., `--target_job_rate`, `--template-file-gcs-path`, `--deps-pkg`).
  - BigQuery reservations must be configured via `bq update --bi_reservation_size` or SQL DDL.

### 2.4 Gate 4: Code Block Static AST & Lint Gate
- **Rule**: All standalone Python snippets, Airflow DAGs, and Beam pipelines must be syntactically valid and parse cleanly with Python's built-in `ast` module.
- **Enforcement**:
  - Prohibit JavaScript keyword pollution (`false`, `true`, `null` in Python blocks).
  - Jinja template strings passed to operators must use template filters (`| replace(...)`) rather than Python string methods evaluated at DAG parse time.

### 2.5 Gate 5: Zero Broken Internal Links Gate
- **Rule**: All cross-module navigation links must be relative paths (`../` or `./`) that resolve to real files on disk.
- **Enforcement**:
  - Automated regex scanning flags any link containing `file:///` or machine-specific drive letters (`e:/`, `c:/`) as critical build failures.

---

## 3. Repository Style Guide Standards

To ensure visual consistency and readability across the 49 curriculum files, all Markdown files must adhere to the following conventions:

### 3.1 Document Heading Structure
1. **H1 Title (`# Title`)**: Exactly one top-level heading per document, located on line 1, stating the official module or chapter title.
2. **H2 Section (`## 1. Section Title`)**: Major topic divisions, numbered sequentially.
3. **H3 Sub-Section (`### 1.1 Sub-Topic`)**: Granular architectural or conceptual breakdowns.
4. **H4 Granular Element (`#### Element`)**: Code snippet context, scenario steps, or deep-dive notes.

### 3.2 Code Block Formatting & Fences
- **Language Identifiers**: All triple-backtick code fences must declare an explicit language tag (`bash`, `python`, `sql`, `yaml`, `json`, `markdown`).
- **Balanced Fences**: Every opening code fence must have a corresponding closing fence. Files with odd fence counts fail linting.
- **Non-Empty Content**: Empty or dangling code blocks are prohibited.
- **Placeholder Syntax**: Variables intended for user replacement must use standard bash parameter expansion syntax: `${PROJECT_ID}`, `${REGION}`, `${BUCKET_NAME}`.

### 3.3 Markdown Table Formatting
- Tables must have clean pipe alignment, explicit column headers, and a valid separator row (`|:---|:---:|---:|`).
- Long URLs in tables should use descriptive anchor text (e.g., `[Cloud Storage SLA](https://cloud.google.com/storage/sla)`) rather than raw URLs to prevent horizontal scrolling.

### 3.4 Service Naming & Terminology Standardization
Always use official Google Cloud product names:
- Correct: **BigQuery** (not Big Query or bigquery)
- Correct: **Cloud Storage** (not GCS or Google Cloud Storage in formal headings)
- Correct: **Cloud Spanner** (not Spanner DB)
- Correct: **Cloud Bigtable** (not BigTable)
- Correct: **Google Cloud Pub/Sub** or **Pub/Sub** (not pubsub or Cloud PubSub)
- Correct: **Cloud Composer** (not Composer)
- Correct: **Datastream** (not DataStream)
- Correct: **Dataplex Universal Catalog** (not Data Catalog)

---

## 4. Automated Continuous Verification Architecture

The test harness described in `TEST_INFRA.md` acts as the persistent automated verification engine for the repository:

```
+---------------------------------------------------------------------------------+
|                        PROGRESSIVE E2E TEST TIERS                               |
+---------------------------------------------------------------------------------+
| Tier 1: Feature Coverage (run_e2e_tests.py --tier 1)                            |
|         - Asserts 100% census (49 files) in findings table                      |
|         - Verifies official Google Cloud doc citations                          |
|         - Verifies Category A-D performance claims classification               |
|         - Validates 19-objective exam guide mapping (FC, PC, M, TI)             |
|         - Asserts existence of 7 audit files and 3 root governance files        |
+---------------------------------------------------------------------------------+
| Tier 2: Boundary & Corner Cases (run_e2e_tests.py --tier 2)                     |
|         - Validates .gitignore allows docs/ tracking                            |
|         - Crawls entire repository link graph (0 broken relative links)         |
|         - Validates code block fence balance across 316+ blocks                 |
+---------------------------------------------------------------------------------+
| Tier 3: Cross-Feature Traceability (run_e2e_tests.py --tier 3)                  |
|         - Cross-checks EXAM_COVERAGE_MATRIX.md vs exam_coverage_tracker.md      |
|         - Asserts 100% of P0/P1 issues from backlog are recorded in CHANGELOG.md|
|         - Validates SOURCES.md citation metadata and review dates               |
+---------------------------------------------------------------------------------+
| Tier 4: Real-World Scenarios (run_e2e_tests.py --tier 4)                        |
|         - AST parsing of all Python scripts and Airflow DAGs                    |
|         - Anti-hallucination checks on gcloud and bq CLI commands               |
|         - SQL syntax checks (no OPTIONS on SELECT)                              |
+---------------------------------------------------------------------------------+
```

---

## 5. Long-Term Maintenance & Review Lifecycle

To prevent knowledge decay and keep pace with Google Cloud's rapid feature cadence, the repository shall follow a structured maintenance lifecycle:

1. **Bi-Annual Blueprint Synchronization**:
   - Every 6 months, download and diff the official Google Cloud PDE Exam Guide PDF against `docs/audits/EXAM_COVERAGE_MATRIX.md`.
   - Update domain weightings and integrate new sub-bullets into the curriculum.
2. **Quota & Capacity Ceiling Audit**:
   - Quarterly check of `https://cloud.google.com/bigquery/quotas`, `https://cloud.google.com/storage/quotas`, and `https://cloud.google.com/bigtable/quotas`.
   - Update Category B parameters (e.g. partition limits, max object sizes, storage densities).
3. **Service Deprecation & Retirement Tracking**:
   - Track deprecation notices for legacy services (e.g., Cloud Data Fusion sunsetting, Dataproc Serverless migrations, BigQuery flat-rate phase-out).
   - Ensure the curriculum reflects current generally available (GA) tools.
4. **Permanent Audit Trail in `CHANGELOG.md`**:
   - Every technical modification, quota adjustment, or syntax correction must be recorded in root `CHANGELOG.md` citing the corresponding issue identifier (e.g. `ISSUE-P0-01`) and documentation link.
