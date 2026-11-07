"""
Shared base constants and utilities for GCP PDE Handbook E2E Test Suite.
"""

from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

# Workspace Root
TESTS_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = TESTS_DIR.parent

# 100% Repository Census Files (49 Markdown files excluding .git and .agents)
ALL_49_CENSUS_FILES: List[str] = [
    "00_data_engineer_cli_cheat_sheet.md",
    "00_handbook_index.md",
    "00_resource_selection_guide.md",
    "01_phase0_gcp_fundamentals/01_resource_hierarchy_and_tenancy.md",
    "01_phase0_gcp_fundamentals/02_iam_service_accounts_and_auth.md",
    "01_phase0_gcp_fundamentals/03_networking_vpc_firewalls_and_psc.md",
    "01_phase0_gcp_fundamentals/04_pricing_quotas_and_billing.md",
    "02_phase1_glossary_and_taxonomy/00_taxonomy_index.md",
    "02_phase1_glossary_and_taxonomy/01_cloud_and_infrastructure.md",
    "02_phase1_glossary_and_taxonomy/02_storage_and_databases.md",
    "02_phase1_glossary_and_taxonomy/03_ingestion_and_streaming.md",
    "02_phase1_glossary_and_taxonomy/04_processing_and_compute.md",
    "02_phase1_glossary_and_taxonomy/05_governance_and_metadata.md",
    "02_phase1_glossary_and_taxonomy/06_security_and_compliance.md",
    "03_phase2_lifecycle_architecture/01_source_acquisition_and_batch_ingest.md",
    "03_phase2_lifecycle_architecture/02_streaming_ingestion_and_validation.md",
    "03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md",
    "03_phase2_lifecycle_architecture/04_transformations_and_processing.md",
    "03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md",
    "03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md",
    "04_phase3_exam_domains/domain_1_designing_data_processing_systems.md",
    "04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md",
    "04_phase3_exam_domains/domain_3_storing_data.md",
    "04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md",
    "04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md",
    "05_phase4_comparisons_and_decision_trees/01_compute_comparisons.md",
    "05_phase4_comparisons_and_decision_trees/02_storage_database_comparisons.md",
    "05_phase4_comparisons_and_decision_trees/03_ingestion_orchestration_comparisons.md",
    "06_phase5_case_studies/case_01_realtime_ecom_lakehouse.md",
    "06_phase5_case_studies/case_02_cdc_financial_replication.md",
    "06_phase5_case_studies/case_03_enterprise_data_mesh.md",
    "06_phase5_case_studies/case_04_ml_feature_store_and_rag.md",
    "07_phase6_hands_on_labs/lab_01_bigquery_optimization.md",
    "07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md",
    "07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md",
    "07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md",
    "08_phase7_mock_exam_engine/01_scenario_exam_systems_ingestion.md",
    "08_phase7_mock_exam_engine/02_scenario_exam_storage_optimization.md",
    "08_phase7_mock_exam_engine/exam_coverage_tracker.md",
    "docs/audits/PHASE_1_TECHNICAL_AUDIT.md",
    "docs/audits/PHASE_2_1_1_CORRECTION_REPORT.md",
    "docs/audits/PHASE_2_1_2_1_CORRECTION_REPORT.md",
    "docs/audits/PHASE_2_1_2_CORRECTION_REPORT.md",
    "docs/audits/PHASE_2_1_CORRECTION_REPORT.md",
    "docs/audits/PHASE_2_TEACHING_QUALITY_REPORT.md",
    "docs/audits/PHASE_3_EXAM_COVERAGE_GAP_ANALYSIS.md",
    "docs/audits/PHASE_3_REMEDIATION_IMPLEMENTATION_REPORT.md",
    "Intro_To_DE/data_sources_vs_data_sinks.md",
    "README.md",
]

# Official PDE Exam Guide (v4.2) 19 Sub-Objectives
OFFICIAL_EXAM_OBJECTIVES: Dict[str, Dict[str, str]] = {
    "1.1": {
        "domain": "Domain 1",
        "weight": "22%",
        "title": "Designing for security and compliance",
    },
    "1.2": {
        "domain": "Domain 1",
        "weight": "22%",
        "title": "Designing for reliability and fidelity",
    },
    "1.3": {
        "domain": "Domain 1",
        "weight": "22%",
        "title": "Designing for flexibility and portability",
    },
    "1.4": {
        "domain": "Domain 1",
        "weight": "22%",
        "title": "Designing data migrations",
    },
    "2.1": {
        "domain": "Domain 2",
        "weight": "25%",
        "title": "Planning the data pipelines",
    },
    "2.2": {
        "domain": "Domain 2",
        "weight": "25%",
        "title": "Building the pipelines",
    },
    "2.3": {
        "domain": "Domain 2",
        "weight": "25%",
        "title": "Deploying and operationalizing the pipelines",
    },
    "3.1": {
        "domain": "Domain 3",
        "weight": "20%",
        "title": "Selecting storage systems",
    },
    "3.2": {
        "domain": "Domain 3",
        "weight": "20%",
        "title": "Planning for using a data warehouse",
    },
    "3.3": {
        "domain": "Domain 3",
        "weight": "20%",
        "title": "Using a data lake",
    },
    "3.4": {
        "domain": "Domain 3",
        "weight": "20%",
        "title": "Designing for a data platform",
    },
    "4.1": {
        "domain": "Domain 4",
        "weight": "15%",
        "title": "Preparing data for visualization",
    },
    "4.2": {
        "domain": "Domain 4",
        "weight": "15%",
        "title": "Preparing data for AI and ML",
    },
    "4.3": {
        "domain": "Domain 4",
        "weight": "15%",
        "title": "Sharing data",
    },
    "5.1": {
        "domain": "Domain 5",
        "weight": "18%",
        "title": "Optimizing resources",
    },
    "5.2": {
        "domain": "Domain 5",
        "weight": "18%",
        "title": "Designing automation and repeatability",
    },
    "5.3": {
        "domain": "Domain 5",
        "weight": "18%",
        "title": "Organizing workloads based on business requirements",
    },
    "5.4": {
        "domain": "Domain 5",
        "weight": "18%",
        "title": "Monitoring and troubleshooting processes",
    },
    "5.5": {
        "domain": "Domain 5",
        "weight": "18%",
        "title": "Maintaining awareness of failures and mitigating impact",
    },
}

# 7 Stage 1 Audit Deliverables required in docs/audits/
REQUIRED_AUDIT_DELIVERABLES: List[str] = [
    "docs/audits/EXECUTIVE_AUDIT_REPORT.md",
    "docs/audits/FILE_BY_FILE_FINDINGS.md",
    "docs/audits/REMEDIATION_BACKLOG.md",
    "docs/audits/EXAM_COVERAGE_MATRIX.md",
    "docs/audits/ARCHITECTURE_IMPROVEMENTS.md",
    "docs/audits/LAB_EXECUTION_REPORT.md",
    "docs/audits/DOCUMENTATION_IMPROVEMENT_PLAN.md",
]

# 3 Root Governance Files required at repository root
REQUIRED_ROOT_GOVERNANCE_FILES: List[str] = [
    "AUDIT_REPORT.md",
    "CHANGELOG.md",
    "SOURCES.md",
]

# 4 Performance Claim Categories
PERFORMANCE_CLAIM_CATEGORIES = [
    "Category A",  # Officially documented service guarantee / SLA
    "Category B",  # Documented service capability / quota ceiling
    "Category C",  # Workload-dependent engineering estimate
    "Category D",  # Unsupported or misleading claim
]

# 4 Exam Coverage Statuses
EXAM_COVERAGE_STATUSES = [
    "Fully covered",  # or FC
    "Partially covered",  # or PC
    "Missing",  # or M
    "Covered but technically incorrect",  # or TI
]


def get_all_repository_markdown_files(root_dir: Path = WORKSPACE_ROOT) -> List[Path]:
    """
    Returns all Markdown files in the repository excluding internal .agents, .git, and tests directories.
    """
    all_files: List[Path] = []
    for p in root_dir.rglob("*.md"):
        parts = p.relative_to(root_dir).parts
        if any(part.startswith(".") for part in parts):
            continue
        if parts[0] in ("tests", "node_modules", "vendor"):
            continue
        all_files.append(p)
    return sorted(all_files, key=lambda x: str(x).lower())


def read_text_safe(path: Path) -> str:
    """Reads a text file using utf-8 encoding."""
    if not path.is_file():
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def extract_code_blocks(content: str) -> List[Tuple[str, str, int]]:
    """
    Extracts code blocks from markdown content.
    Returns list of tuples: (language, code_text, start_line)
    """
    code_blocks = []
    lines = content.splitlines()
    in_block = False
    current_lang = ""
    current_lines: List[str] = []
    start_line = 0

    for idx, line in enumerate(lines, start=1):
        if line.startswith("```"):
            if not in_block:
                in_block = True
                current_lang = line.strip("`").strip().lower()
                current_lines = []
                start_line = idx
            else:
                in_block = False
                code_blocks.append((current_lang, "\n".join(current_lines), start_line))
                current_lang = ""
                current_lines = []
        elif in_block:
            current_lines.append(line)

    return code_blocks


def extract_markdown_links(content: str) -> List[Tuple[str, str]]:
    """
    Extracts all markdown links [label](target).
    Returns list of tuples: (label, target)
    """
    pattern = re.compile(r'\[([^\]]*)\]\(([^)]+)\)')
    return pattern.findall(content)


def is_relative_internal_link(target: str) -> bool:
    """Checks whether a link target is an internal file link or relative link."""
    target_clean = target.strip()
    if not target_clean:
        return False
    if target_clean.startswith("#"):
        return False  # Same-file anchor link
    if target_clean.startswith("http://") or target_clean.startswith("https://"):
        return False  # External web URL
    if target_clean.startswith("mailto:"):
        return False
    return True


def parse_markdown_table_rows(content: str) -> List[Dict[str, str]]:
    """
    Parses pipe-delimited markdown tables into a list of row dictionaries.
    """
    rows: List[Dict[str, str]] = []
    lines = [line.strip() for line in content.splitlines() if line.strip().startswith("|")]
    if len(lines) < 3:
        return rows

    headers = [col.strip() for col in lines[0].strip("|").split("|")]
    
    # Check if line 1 is a delimiter row
    delim_line = lines[1]
    if not re.match(r'^\|?[\s\-:|]+\|?$', delim_line):
        return rows

    for line in lines[2:]:
        # Skip sub-header delimiters
        if re.match(r'^\|?[\s\-:|]+\|?$', line):
            continue
        cols = [col.strip() for col in line.strip("|").split("|")]
        if len(cols) == len(headers):
            row_dict = {headers[i]: cols[i] for i in range(len(headers))}
            rows.append(row_dict)
    return rows
