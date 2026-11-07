# Google Cloud Professional Data Engineer (PDE) Master Architecture Handbook

[![Certification](https://img.shields.io/badge/Google%20Cloud-Professional%20Data%20Engineer-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/learn/certification/data-engineer)
[![Prerequisite](https://img.shields.io/badge/Prerequisite-Databricks%20%2F%20Spark%20Background-FF3621?logo=databricks&logoColor=white)](#)
[![Format](https://img.shields.io/badge/Docs-VS%20Code%20Ready%20Markdown-007ACC?logo=visual-studio-code&logoColor=white)](#)
[![Exam Coverage](https://img.shields.io/badge/Official%20Syllabus-100%25%20Covered-34A853?logo=google&logoColor=white)](#)

A comprehensive, production-grade learning handbook and architectural reference manual for the **Google Cloud Professional Data Engineer (PDE)** certification.

Authored from first principles for data engineers and architects transitioning from **Databricks, Apache Spark, and Delta Lake**, this repository teaches you how to think, design, and operate like a **Google Cloud Principal Data Architect**.

---

## Quick Reference Tools

| Guide | Description |
|---|---|
| **[Resource Selection Blueprint & Decision Guide](./00_resource_selection_guide.md)** | Step-by-step framework for choosing between GCP storage, compute, ingestion, and orchestration services based on latency, QPS, consistency, and cost curves. |
| **[Data Engineer CLI Master Cheat Sheet](./00_data_engineer_cli_cheat_sheet.md)** | Production CLI commands across `gcloud`, `bq`, `cbt`, and modern `gcloud storage`, with concise explanations and flag breakdowns. |
| **[Master Handbook Index](./00_handbook_index.md)** | Complete table of contents with direct relative navigation links to all 37 modules. |

---

## Curriculum Structure & Navigation

### [Phase 0: Google Cloud Fundamentals](./01_phase0_gcp_fundamentals/)
- [01. Resource Hierarchy & Tenancy](./01_phase0_gcp_fundamentals/01_resource_hierarchy_and_tenancy.md): Organizations, Folders, Projects, Colossus, Jupiter network fabric, and Org Policies.
- [02. IAM, Service Accounts & Identity Architecture](./01_phase0_gcp_fundamentals/02_iam_service_accounts_and_auth.md): Roles, SA impersonation, Workload Identity Federation vs. JSON keys, and Least Privilege.
- [03. Network Topology, VPC, Firewalls & Private Connectivity](./01_phase0_gcp_fundamentals/03_networking_vpc_firewalls_and_psc.md): Global VPCs, Private Google Access (PGA), Shared VPC, PSC, and VPC Service Controls.
- [04. Pricing, Quotas, Cost Controls & Resource Management](./01_phase0_gcp_fundamentals/04_pricing_quotas_and_billing.md): BigQuery Editions, slots vs. on-demand, budget alerts, and FinOps defense.

### [Phase 1: Comprehensive Glossary & Product Taxonomy](./02_phase1_glossary_and_taxonomy/)
- [00. Taxonomy Index & Macro Decision Tree](./02_phase1_glossary_and_taxonomy/00_taxonomy_index.md)
- [01. Infrastructure & Core Distributed Systems](./02_phase1_glossary_and_taxonomy/01_cloud_and_infrastructure.md): Borg, Colossus, Jupiter, Andromeda, and TrueTime.
- [02. Storage & Operational Database Technologies](./02_phase1_glossary_and_taxonomy/02_storage_and_databases.md): 10-point technical profiles for GCS, BigQuery, BigLake, Bigtable, Spanner, Cloud SQL, AlloyDB, Firestore, and Memorystore.
- [03. Ingestion, Messaging & Event Streaming](./02_phase1_glossary_and_taxonomy/03_ingestion_and_streaming.md): Pub/Sub, Pub/Sub Lite, Datastream CDC, STS, and BigQuery DTS.
- [04. Distributed Processing, Compute & Transformation](./02_phase1_glossary_and_taxonomy/04_processing_and_compute.md): Cloud Dataflow (Beam), Cloud Dataproc (Spark/Hadoop), Cloud Data Fusion, and Dataform.
- [05. Governance, Cataloging, Quality & Lineage](./02_phase1_glossary_and_taxonomy/05_governance_and_metadata.md): Dataplex, Dataplex Catalog, Auto Data Quality, Lineage, and Analytics Hub.
- [06. Security, Encryption & Perimeter Isolation](./02_phase1_glossary_and_taxonomy/06_security_and_compliance.md): Cloud KMS (CMEK/CSEK), Sensitive Data Protection (DLP), and VPC-SC.

### [Phase 2: End-to-End Enterprise Data Lifecycle Architecture](./03_phase2_lifecycle_architecture/)
*A complete 6-stage lifecycle implementation for an enterprise omnichannel retail platform processing 50M transactions/day:*
- [01. Source Acquisition & Batch Ingestion](./03_phase2_lifecycle_architecture/01_source_acquisition_and_batch_ingest.md): PostgreSQL/Oracle CDC via Datastream over PSC, Storage Transfer Service, and dual-region GCS Bronze lake.
- [02. Streaming Ingestion, Event Watermarking & Validation](./03_phase2_lifecycle_architecture/02_streaming_ingestion_and_validation.md): Pub/Sub ordering keys, Dataflow Streaming Engine, fixed/sliding/session windows, and BigQuery Storage Write API.
- [03. Storage Modeling & Lakehouse Topology](./03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md): Medallion lakehouse, BigLake external tables, BigQuery Capacitor/Dremel internal architecture, table partitioning/clustering, and Bigtable sub-10ms store.
- [04. Distributed Transformation & SQL Processing](./03_phase2_lifecycle_architecture/04_transformations_and_processing.md): In-warehouse ELT with Dataform (SQLX models, Git, assertions), Dataproc Serverless PySpark batches, and CDC `MERGE` logic.
- [05. Orchestration, Governance & Serving Layer](./03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md): Cloud Composer 3 (Airflow 2.x) production DAGs, Dataplex AutoDQ, BI Engine acceleration, and Analytics Hub.
- [06. Disaster Recovery, Resilience & Cost Defense](./03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md): Dual-Region Turbo Replication (<15 min SLA), BigQuery cross-region dataset replication, Time Travel, and FinOps slot allocation.

### [Phase 3: Official Exam Domains Deep Dive](./04_phase3_exam_domains/)
- [Domain 1: Designing Data Processing Systems](./04_phase3_exam_domains/domain_1_designing_data_processing_systems.md): Security, Org policies, CMEK, DLP tokenization, RPO/RTO calculations, and BigQuery Omni multi-cloud.
- [Domain 2: Ingesting and Processing Data](./04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md): Batch/streaming pipelines, Beam stateful DoFns, triggers, Dataproc Spot VMs with graceful decommissioning, and Dataform.
- [Domain 3: Storing Data](./04_phase3_exam_domains/domain_3_storing_data.md): Storage classes, BigQuery logical vs. physical billing, Bigtable row key salting, Spanner TrueTime commit waits, and AlloyDB HTAP.
- [Domain 4: Preparing and Using Data for Analysis](./04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md): BI Engine, Materialized Views, Dynamic Data Masking, BigQuery ML, Vertex AI Feature Store, and Vector Search RAG pipelines.
- [Domain 5: Maintaining and Automating Data Workloads](./04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md): BigQuery Editions/Reservations, Dataproc cluster tuning, Dataflow system lag & backpressure diagnostics, and `INFORMATION_SCHEMA` queries.

### [Phase 4: Product Comparisons & Architectural Decision Trees](./05_phase4_comparisons_and_decision_trees/)
- [01. Distributed Compute: Dataflow vs. Dataproc vs. Cloud Data Fusion vs. Dataform](./05_phase4_comparisons_and_decision_trees/01_compute_comparisons.md)
- [02. Storage & Databases: BigQuery vs. Bigtable vs. Spanner vs. Cloud SQL vs. AlloyDB vs. Firestore](./05_phase4_comparisons_and_decision_trees/02_storage_database_comparisons.md)
- [03. Ingestion & Orchestration: Pub/Sub vs. Datastream vs. STS; Composer vs. Workflows vs. Scheduler](./05_phase4_comparisons_and_decision_trees/03_ingestion_orchestration_comparisons.md)

### [Phase 5: Production Architecture Case Studies](./06_phase5_case_studies/)
- [Case 01: Real-Time E-Commerce Lakehouse](./06_phase5_case_studies/case_01_realtime_ecom_lakehouse.md): 150k events/sec clickstream, Bigtable profile store, BigLake/GCS, and BigQuery.
- [Case 02: Change Data Capture (CDC) Enterprise Financial Replication](./06_phase5_case_studies/case_02_cdc_financial_replication.md): Oracle/PostgreSQL CDC to BigQuery via Datastream, DLP crypto-tokenization, and CMEK.
- [Case 03: Decentralized Data Mesh & Analytics Hub](./06_phase5_case_studies/case_03_enterprise_data_mesh.md): Multi-department domain data sharing with Dataplex and zero-copy linked datasets.
- [Case 04: Real-Time ML Feature Store & RAG Pipeline](./06_phase5_case_studies/case_04_ml_feature_store_and_rag.md): Fraud detection at 40k QPS (<15ms SLA) using Vertex Feature Store, Spanner, and Vector Search.

### [Phase 6: Hands-On Production Labs](./07_phase6_hands_on_labs/)
- [Lab 01: BigQuery Partitioning, Clustering & Slot Optimization](./07_phase6_hands_on_labs/lab_01_bigquery_optimization.md): 100M+ row dataset benchmark, Materialized Views, and BI Engine reservation.
- [Lab 02: Streaming Analytics with Pub/Sub & Apache Beam on Dataflow](./07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md): Python Apache Beam pipeline with dead-letter queue (DLQ) side outputs, windowing, and Storage Write API.
- [Lab 03: CDC Ingestion with Datastream to BigQuery](./07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md): Cloud SQL PostgreSQL logical decoding replication into BigQuery with zero ETL.
- [Lab 04: End-to-End Orchestration with Cloud Composer DAGs](./07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md): Airflow DAG running GCS sensors, Dataproc Serverless PySpark batches, and BigQuery assertion gates.

### [Phase 7: Scenario-Based Exam Bank & Coverage Tracker](./08_phase7_mock_exam_engine/)
- [01. Scenario Exam 1: Systems & Ingestion Architecture](./08_phase7_mock_exam_engine/01_scenario_exam_systems_ingestion.md): 15 questions targeting Domains 1 & 2 with deep distractor breakdowns.
- [02. Scenario Exam 2: Storage, Security & Performance Optimization](./08_phase7_mock_exam_engine/02_scenario_exam_storage_optimization.md): 15 questions targeting Domains 3, 4 & 5 with deep distractor breakdowns.
- [03. Exam Guide Coverage Tracker](./08_phase7_mock_exam_engine/exam_coverage_tracker.md): 100% syllabus topic mapping matrix and candidate readiness checklist.

---

## Architectural Comparison: Databricks vs. Google Cloud

| Conceptual Need | Databricks / Open Source Stack | Google Cloud Implementation | Architectural Difference |
|---|---|---|---|
| **Data Lake Storage** | DBFS / S3 / ADLS Gen2 with Delta Lake | **Cloud Storage (GCS) + BigLake** | GCS provides uniform global strong consistency. BigLake enables row/column-level security over open formats (Parquet/Iceberg) queried directly by BigQuery without data movement. |
| **Data Warehousing** | Databricks SQL Warehouses (Photon) | **BigQuery (Capacitor + Dremel)** | Fully serverless. Storage and compute are physically decoupled and communicate over Google's 1.6+ Tbps Jupiter network fabric. |
| **Batch & Streaming** | Spark Structured Streaming (Micro-batch) | **Cloud Dataflow (Apache Beam)** | Unified Beam model treats batch as bounded streams. Uses event-time watermarks, triggers, and serverless Dynamic Work Rebalancing (Liquid Sharding). |
| **Existing Spark Jobs** | Databricks Jobs & Compute Clusters | **Cloud Dataproc (GCE or Serverless)** | Ephemeral or Serverless Spark/Hadoop clusters with native GCS connectors and Spot VM autoscaling. |
| **In-Warehouse ELT** | dbt on Databricks / Delta Live Tables (DLT) | **Dataform (SQLX)** | Native Git-integrated BigQuery SQL orchestration with automated dependency resolution and data quality assertions. |
| **Event Ingestion** | Apache Kafka / AWS Kinesis | **Cloud Pub/Sub & Pub/Sub Lite** | Global multi-region serverless event bus with zero partition management, ordering keys, and direct BigQuery zero-ETL subscriptions. |
| **Change Data Capture** | Debezium / Fivetran | **Datastream** | Serverless log-based CDC replicating PostgreSQL, MySQL, and Oracle directly into BigQuery or GCS. |
| **Data Governance** | Unity Catalog | **Dataplex & Dataplex Catalog** | Centralized data mesh governance, automated schema discovery, data lineage, Auto Data Quality (AutoDQ), and column-level policy tags. |
| **Orchestration** | Databricks Workflows / Airflow | **Cloud Composer 3 (Airflow 2.x)** | Fully managed Apache Airflow with cross-cloud Python operators and autoscaling triggerers/schedulers. |

---

## License & Usage

Created for data engineers and cloud architects preparing for the **Google Cloud Professional Data Engineer** certification or designing enterprise data systems on Google Cloud Platform.
