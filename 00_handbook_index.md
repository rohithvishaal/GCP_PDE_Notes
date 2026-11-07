# Google Cloud Professional Data Engineer (PDE) Master Handbook
**Target Audience**: Data Engineers & Architects transitioning to Google Cloud with Databricks / Apache distributed computing background.
**Certification**: Google Cloud Certified Professional Data Engineer

---

## Quick Reference Tools
- **[00. Data Engineer CLI Master Cheat Sheet](file:///e:/gcp_pde_revamp/00_data_engineer_cli_cheat_sheet.md)**: Production CLI commands with concise explanations and flag breakdowns across all Google Cloud data services (`gcloud`, `bq`, `cbt`, modern `gcloud storage`).

---

## Handbook Structure & Navigation

- [01. Phase 0: Google Cloud Fundamentals](file:///e:/gcp_pde_revamp/01_phase0_gcp_fundamentals/)
  - [01. Resource Hierarchy & Tenancy](file:///e:/gcp_pde_revamp/01_phase0_gcp_fundamentals/01_resource_hierarchy_and_tenancy.md)
  - [02. IAM, Service Accounts & Identity Architecture](file:///e:/gcp_pde_revamp/01_phase0_gcp_fundamentals/02_iam_service_accounts_and_auth.md)
  - [03. Network Topology, VPC, Firewalls & Private Connectivity](file:///e:/gcp_pde_revamp/01_phase0_gcp_fundamentals/03_networking_vpc_firewalls_and_psc.md)
  - [04. Pricing, Quotas, Cost Controls & Resource Management](file:///e:/gcp_pde_revamp/01_phase0_gcp_fundamentals/04_pricing_quotas_and_billing.md)
- [02. Phase 1: Comprehensive Glossary & Product Taxonomy](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/)
  - [00. Taxonomy & Architectural Taxonomy Map](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/00_taxonomy_index.md)
  - [01. Infrastructure & Core Distributed Systems](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/01_cloud_and_infrastructure.md)
  - [02. Storage & Operational Database Technologies](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/02_storage_and_databases.md)
  - [03. Ingestion, Messaging & Event Streaming](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/03_ingestion_and_streaming.md)
  - [04. Distributed Processing, Compute & Transformation](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/04_processing_and_compute.md)
  - [05. Governance, Cataloging, Quality & Lineage](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/05_governance_and_metadata.md)
  - [06. Security, Encryption & Perimeter Isolation](file:///e:/gcp_pde_revamp/02_phase1_glossary_and_taxonomy/06_security_and_compliance.md)
- [03. Phase 2: End-to-End Enterprise Data Lifecycle Architecture](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/)
  - [01. Source Acquisition & Batch Ingestion](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/01_source_acquisition_and_batch_ingest.md)
  - [02. Streaming Ingestion, Event Watermarking & Validation](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/02_streaming_ingestion_and_validation.md)
  - [03. Storage Modeling & Lakehouse Topology](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md)
  - [04. Distributed Transformation & SQL Processing](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/04_transformations_and_processing.md)
  - [05. Orchestration, Governance & Serving Layer](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md)
  - [06. Disaster Recovery, Resilience & Cost Defense](file:///e:/gcp_pde_revamp/03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md)
- [04. Phase 3: Official Exam Domains Deep Dive](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/)
  - [Domain 1: Designing Data Processing Systems](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/domain_1_designing_data_processing_systems.md)
  - [Domain 2: Ingesting and Processing Data](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md)
  - [Domain 3: Storing Data](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/domain_3_storing_data.md)
  - [Domain 4: Preparing and Using Data for Analysis](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md)
  - [Domain 5: Maintaining and Automating Data Workloads](file:///e:/gcp_pde_revamp/04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md)
- [05. Phase 4: Product Comparisons & Architectural Decision Trees](file:///e:/gcp_pde_revamp/05_phase4_comparisons_and_decision_trees/)
  - [01. Distributed Compute: Dataflow vs Dataproc vs Cloud Data Fusion](file:///e:/gcp_pde_revamp/05_phase4_comparisons_and_decision_trees/01_compute_comparisons.md)
  - [02. Storage & Warehousing: BigQuery vs Bigtable vs Spanner vs Cloud SQL vs AlloyDB](file:///e:/gcp_pde_revamp/05_phase4_comparisons_and_decision_trees/02_storage_database_comparisons.md)
  - [03. Ingestion & Orchestration: Pub/Sub vs Datastream, Composer vs Workflows](file:///e:/gcp_pde_revamp/05_phase4_comparisons_and_decision_trees/03_ingestion_orchestration_comparisons.md)
- [06. Phase 5: Production Architecture Patterns & Case Studies](file:///e:/gcp_pde_revamp/06_phase5_case_studies/)
  - [Case 01: Real-Time E-Commerce Lakehouse](file:///e:/gcp_pde_revamp/06_phase5_case_studies/case_01_realtime_ecom_lakehouse.md)
  - [Case 02: Change Data Capture (CDC) Enterprise Replication](file:///e:/gcp_pde_revamp/06_phase5_case_studies/case_02_cdc_financial_replication.md)
  - [Case 03: Decentralized Data Mesh & Analytics Hub](file:///e:/gcp_pde_revamp/06_phase5_case_studies/case_03_enterprise_data_mesh.md)
  - [Case 04: Production ML Feature Engineering & RAG Pipeline](file:///e:/gcp_pde_revamp/06_phase5_case_studies/case_04_ml_feature_store_and_rag.md)
- [07. Phase 6: Hands-On Production Labs](file:///e:/gcp_pde_revamp/07_phase6_hands_on_labs/)
  - [Lab 01: BigQuery Partitioning, Clustering & Slot Optimization](file:///e:/gcp_pde_revamp/07_phase6_hands_on_labs/lab_01_bigquery_optimization.md)
  - [Lab 02: Streaming Analytics with Pub/Sub & Apache Beam on Dataflow](file:///e:/gcp_pde_revamp/07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md)
  - [Lab 03: CDC Ingestion with Datastream to BigQuery](file:///e:/gcp_pde_revamp/07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md)
  - [Lab 04: End-to-End Orchestration with Cloud Composer DAGs](file:///e:/gcp_pde_revamp/07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md)
- [08. Phase 7: Scenario-Based Exam Bank & Defenses](file:///e:/gcp_pde_revamp/08_phase7_mock_exam_engine/)
  - [01. Scenario Exam 1: Systems & Ingestion Architecture](file:///e:/gcp_pde_revamp/08_phase7_mock_exam_engine/01_scenario_exam_systems_ingestion.md)
  - [02. Scenario Exam 2: Storage, Security & Performance Optimization](file:///e:/gcp_pde_revamp/08_phase7_mock_exam_engine/02_scenario_exam_storage_optimization.md)
  - [03. Exam Coverage Tracker & Objective Mapping](file:///e:/gcp_pde_revamp/08_phase7_mock_exam_engine/exam_coverage_tracker.md)
