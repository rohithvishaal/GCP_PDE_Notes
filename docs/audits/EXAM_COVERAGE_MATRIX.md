# Google Cloud Professional Data Engineer (PDE) Exam Coverage Matrix

**Document Purpose**: Definitive blueprint mapping and curriculum gap analysis evaluating the entire handbook against the Official Google Cloud Professional Data Engineer Exam Guide (v4.2).  
**Specification Source**: Official Exam Guide PDF (`https://services.google.com/fh/files/misc/professional_data_engineer_exam_guide_english.pdf`)  
**Audit Date**: November 2026  
**Evaluation Scope**: 100% of 19 Sub-Objectives across all 5 Domains.  

---

## 1. Executive Summary & Audit Scorecard

The Google Cloud Professional Data Engineer examination blueprint tests 5 core domains with defined weightings. Each sub-objective has been audited against all 49 files in the repository and classified into four standard audit categories:

1. **Fully Covered (FC)**: The objective and all syllabus sub-bullets are thoroughly addressed with accurate theory, architecture patterns, practical syntax, and decision criteria. Zero technical errors.
2. **Partially Covered (PC)**: High-level concepts are present, but one or more explicit syllabus sub-bullets, architectural trade-offs, or configuration options are omitted or superficial.
3. **Missing (M)**: The specific service, architectural pattern, or syllabus requirement is completely absent across all repository files.
4. **Covered but Technically Incorrect (TI)**: The objective is covered, but contains technical errors, invalid CLI/API syntax, incorrect SLA claims, or outdated configurations.

### 1.1 Blueprint Scorecard Across 19 Sub-Objectives

| Domain | Domain Title | Official Weight | Total Objectives | Fully Covered (FC) | Partially Covered (PC) | Covered but Technically Incorrect (TI) | Missing (M) |
|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Domain 1** | Designing data processing systems | 22% | 4 | 2 (50.0%) | 1 (25.0%) | 1 (25.0%) | 0 (0.0%) |
| **Domain 2** | Ingesting and processing data | 25% | 3 | 0 (0.0%) | 1 (33.3%) | 2 (66.7%) | 0 (0.0%) |
| **Domain 3** | Storing data | 20% | 4 | 1 (25.0%) | 0 (0.0%) | 3 (75.0%) | 0 (0.0%) |
| **Domain 4** | Preparing and using data for analysis | 15% | 3 | 1 (33.3%) | 0 (0.0%) | 2 (66.7%) | 0 (0.0%) |
| **Domain 5** | Maintaining and automating data workloads | 18% | 5 | 3 (60.0%) | 0 (0.0%) | 2 (40.0%) | 0 (0.0%) |
| **TOTALS** | **All 5 Exam Domains** | **100%** | **19** | **7 (36.8%)** | **2 (10.5%)** | **10 (52.6%)** | **0 (0.0%)** |

*Note*: Zero objectives are completely missing at the domain level. However, 52.6% of objectives currently exhibit technical defects or CLI syntax errors in labs and quick references, and 10.5% contain sub-bullet gaps (e.g. LLM query generation, stub references).

---

## 2. Comprehensive Traceability Matrix (19 Sub-Objectives)

### Domain 1: Designing data processing systems (~22% of exam)

#### 1.1 Designing for security and compliance
- **Domain Weight**: 22%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Identity and Access Management (e.g., Cloud IAM and organization policies)
  - Data security (encryption and key management)
  - Privacy (e.g., strategies to handle personally identifiable information)
  - Regional considerations (data sovereignty) for data access and storage
  - Legal and regulatory compliance
  - Designing project, dataset, and table architecture to ensure proper data governance
  - Multi-environment use cases (development vs. production)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_1_designing_data_processing_systems.md` (§ 1.1)
  - `01_phase0_gcp_fundamentals/01_resource_hierarchy_and_tenancy.md`
  - `01_phase0_gcp_fundamentals/02_iam_service_accounts_and_auth.md`
  - `02_phase1_glossary_and_taxonomy/06_security_and_compliance.md`
  - `06_phase5_case_studies/case_03_enterprise_data_mesh.md`
- **Audit Assessment**: Exhaustive and accurate. Thorough coverage of IAM predefined vs custom roles, service account impersonation (`roles/iam.serviceAccountUser`), Workload Identity Federation (WIF) with OIDC, Org Policies (`constraints/gcp.resourceLocations`), CMEK envelope encryption, Cloud EKM, Key Access Justifications (KAJ), Cloud DLP de-identification templates (crypto-hash tokenization, bucketing, masking), and Assured Workloads data boundaries.
- **Official Documentation Citation**:
  - [Cloud IAM Overview](https://cloud.google.com/iam/docs/overview) § "Understanding roles"
  - [Cloud KMS Envelope Encryption](https://cloud.google.com/kms/docs/envelope-encryption) § "How envelope encryption works"
  - [Sensitive Data Protection De-identification](https://cloud.google.com/sensitive-data-protection/docs/deidentification-overview) § "De-identification techniques"

---

#### 1.2 Designing for reliability and fidelity
- **Domain Weight**: 22%
- **Status**: **Partially covered (PC)**
- **Syllabus Sub-Bullets**:
  - Preparing and cleaning data (e.g., Dataform, Dataflow, Cloud Data Fusion, prompting LLMs for query generation)
  - Monitoring and orchestration of data pipelines
  - Disaster recovery and fault tolerance
  - Making decisions related to ACID compliance and availability
  - Data validation
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_1_designing_data_processing_systems.md` (§ 1.2)
  - `03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md`
  - `03_phase2_lifecycle_architecture/02_streaming_ingestion_and_validation.md`
  - `07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md`
- **Audit Assessment**: High-level DR and ACID principles are well explained. However, contains two key gaps:
  1. **Missing Syllabus Sub-Bullet**: Fails to cover the official v4.2 requirement: *"prompting LLMs for query generation"* (Gemini in BigQuery Studio for SQL generation, schema context feeding, and validation assertions).
  2. **Technical Error**: Contains Category D "100% SLA" claim for Turbo Replication in `03_06_dr.md:63`.
- **Required Remediation**: Add Gemini in BigQuery Studio section to `domain_1.md § 1.2` (`ISSUE-P2-09`); correct Turbo Replication SLA claim (`ISSUE-P0-01`).
- **Official Documentation Citation**:
  - [Write Queries with Gemini Assistance in BigQuery](https://cloud.google.com/bigquery/docs/write-queries-with-gemini) § "Generate SQL queries with Gemini assistance"
  - [Google Cloud Disaster Recovery Planning Guide](https://cloud.google.com/architecture/disaster-recovery) § "Disaster recovery scenarios"

---

#### 1.3 Designing for flexibility and portability
- **Domain Weight**: 22%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Mapping current and future business requirements to the architecture
  - Designing for data and application portability (e.g., multi-cloud and data residency requirements)
  - Data staging, cataloging, profiling, and discovery (data governance)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_1_designing_data_processing_systems.md` (§ 1.3)
  - `03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md`
  - `02_phase1_glossary_and_taxonomy/05_governance_and_metadata.md`
  - `06_phase5_case_studies/case_01_realtime_ecom_lakehouse.md`
- **Audit Assessment**: Complete and accurate. Strong coverage of BigQuery Omni querying external datasets in AWS S3 and Azure Blob Storage via cross-cloud connections, BigLake open formats (Apache Iceberg, Delta Lake, Apache Hudi, Parquet), and Dataplex metadata cataloging and profiling.
- **Official Documentation Citation**:
  - [BigQuery Omni Introduction](https://cloud.google.com/bigquery/docs/omni-introduction) § "BigQuery Omni architecture"
  - [BigLake Apache Iceberg Tables](https://cloud.google.com/bigquery/docs/iceberg-tables) § "Iceberg overview"
  - [Dataplex Data Profiling](https://cloud.google.com/dataplex/docs/data-profiling-overview) § "Data profiling scan"

---

#### 1.4 Designing data migrations
- **Domain Weight**: 22%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Analyzing current stakeholder needs, users, processes, and technologies, and creating a plan to get to desired state
  - Planning migration and validation to Google Cloud (e.g., BigQuery Data Transfer Service, Database Migration Service, Transfer Appliance, Google Cloud networking, Datastream)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_1_designing_data_processing_systems.md` (§ 1.4)
  - `03_phase2_lifecycle_architecture/01_source_acquisition_and_batch_ingest.md`
  - `00_resource_selection_guide.md`
  - `07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md`
  - `06_phase5_case_studies/case_02_cdc_financial_replication.md`
- **Audit Assessment**: Comprehensive 6-stage migration lifecycle: discovery, network bandwidth formulas, decision trees comparing Storage Transfer Service (STS) agent pools vs Transfer Appliance, Database Migration Service (DMS), Datastream continuous CDC, and checksum reconciliation (CRC32C and SHA-256).
- **Official Documentation Citation**:
  - [Database Migration Service Documentation](https://cloud.google.com/database-migration/docs) § "Overview of DMS"
  - [Storage Transfer Service Overview](https://cloud.google.com/storage-transfer/docs/overview) § "Agent pools"
  - [Transfer Appliance Documentation](https://cloud.google.com/transfer-appliance/docs/4.0/overview) § "Choosing Transfer Appliance"

---

### Domain 2: Ingesting and processing the data (~25% of exam)

#### 2.1 Planning the data pipelines
- **Domain Weight**: 25%
- **Status**: **Partially covered (PC)**
- **Syllabus Sub-Bullets**:
  - Defining data sources and sinks
  - Defining data transformation and orchestration logic
  - Networking fundamentals
  - Data encryption
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md` (§ 2.1)
  - `01_phase0_gcp_fundamentals/03_networking_vpc_firewalls_and_psc.md`
  - `Intro_To_DE/data_sources_vs_data_sinks.md`
- **Audit Assessment**: Networking fundamentals (Private Google Access, Private Service Connect, firewall rules) and encryption are solid in `domain_2.md`. However, the supporting document `Intro_To_DE/data_sources_vs_data_sinks.md` linked in the tracker is an incomplete 8-line stub lacking definitions of sinks and architecture criteria.
- **Required Remediation**: Expand `Intro_To_DE/data_sources_vs_data_sinks.md` into a formal reference module (`ISSUE-P1-23`).
- **Official Documentation Citation**:
  - [Google Cloud Data Lifecycle Architecture](https://cloud.google.com/architecture/data-lifecycle-cloud-platform) § "Data Ingestion"
  - [Private Google Access Configuration](https://cloud.google.com/vpc/docs/private-google-access) § "Overview"

---

#### 2.2 Building the pipelines
- **Domain Weight**: 25%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Data cleansing
  - Identifying services (Dataflow, Apache Beam, Dataproc, Cloud Data Fusion, BigQuery, Pub/Sub, Spark, Hadoop, Kafka)
  - Transformations: Batch, Streaming (windowing, late arriving data), Processing logic, AI data enrichment
  - Data acquisition and import, integrating with new data sources
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md` (§ 2.2)
  - `03_phase2_lifecycle_architecture/02_streaming_ingestion_and_validation.md`
  - `03_phase2_lifecycle_architecture/04_transformations_and_processing.md`
  - `07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md`
  - `07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md`
- **Audit Assessment**: Conceptually rich, but impaired by critical technical defects in code and mock questions:
  1. `08_phase7/01_scenario_exam_systems_ingestion.md:32` (Q2) specifies invalid Apache Beam syntax `late=AfterWatermark()`.
  2. `03_02_streaming.md:476-478` injects Java Beam methods into Python pipeline snippet.
  3. `lab_02` lacks Dataflow worker service account IAM roles and exhibits an asynchronous drain race condition.
  4. `lab_03` omits Datastream connectivity flag and uses invalid BigQuery SQL `OPTIONS(max_staleness)` on SELECT.
- **Required Remediation**: Fix Beam late trigger syntax (`ISSUE-P1-21`, `ISSUE-P1-01`); remediate Lab 02 worker SA and drain loop (`ISSUE-P1-13`, `ISSUE-P1-14`); fix Lab 03 SQL and profile creation (`ISSUE-P0-07`, `ISSUE-P0-08`).
- **Official Documentation Citation**:
  - [Apache Beam Programming Guide: Triggers](https://beam.apache.org/documentation/programming-guide/#triggers) § "Triggers in the Apache Beam SDK for Python"
  - [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api) § "Exactly-once delivery"
  - [Dataflow Security and Permissions](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions) § "Worker service accounts"

---

#### 2.3 Deploying and operationalizing the pipelines
- **Domain Weight**: 25%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Job automation and orchestration (e.g., Cloud Composer and Workflows)
  - CI/CD (Continuous Integration and Continuous Deployment)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_2_ingesting_and_processing_data.md` (§ 2.3)
  - `03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md`
  - `05_phase4_comparisons_and_decision_trees/03_ingestion_orchestration_comparisons.md`
  - `07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md`
- **Audit Assessment**: Cloud Workflows vs Cloud Composer 3 decision criteria and CI/CD patterns (Beam `TestPipeline`, Flex Templates in Artifact Registry) are thoroughly explained in `domain_2.md`. However, Lab 04 suffers from severe execution blockers:
  1. Omits `gcloud composer environments create` command.
  2. Contains fatal Python syntax error `use_legacy_sql=false` (lowercase).
  3. Uses unrendered Jinja string manipulation at parse time.
  4. Lacks Private Google Access on Dataproc subnetwork.
- **Required Remediation**: Remediate Lab 04 environment creation, DAG Python booleans, Jinja filters, and PGA (`ISSUE-P0-09`, `ISSUE-P0-10`, `ISSUE-P1-17`, `ISSUE-P1-18`).
- **Official Documentation Citation**:
  - [Cloud Composer Overview](https://cloud.google.com/composer/docs/composer-2/composer-overview) § "Airflow architecture"
  - [Dataflow Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/flex-templates) § "Building Flex Templates"
  - [Airflow BigQueryValueCheckOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html) § "Operators"

---

### Domain 3: Storing the data (~20% of exam)

#### 3.1 Selecting storage systems
- **Domain Weight**: 20%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Analyzing data access patterns
  - Choosing managed services (BigQuery, BigLake, AlloyDB, Bigtable, Spanner, Cloud SQL, Cloud Storage, Firestore, Memorystore)
  - Planning for storage costs and performance
  - Lifecycle management of data
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_3_storing_data.md` (§ 3.1)
  - `02_phase1_glossary_and_taxonomy/02_storage_and_databases.md`
  - `05_phase4_comparisons_and_decision_trees/02_storage_database_comparisons.md`
  - `00_resource_selection_guide.md`
- **Audit Assessment**: Comprehensive 9-service comparison matrices and access pattern decision trees. However, contains Category D claims:
  1. Conflates Bigtable sub-10ms operational latency with a contractual "SLA" in `00_resource_selection_guide.md:15, 32, 47, 208` and `02_03:281`.
  2. Storage Decision Table generalizes BigQuery as 99.99% (omitting Standard Edition 99.9%) and Bigtable as 99.999% without documenting the multi-cluster routing requirement.
- **Required Remediation**: Re-label latency targets to SLOs (`ISSUE-P1-11`, `ISSUE-P2-08`); add BigQuery Edition and Bigtable routing qualifiers to decision tables (`ISSUE-P1-05`, `ISSUE-P1-08`).
- **Official Documentation Citation**:
  - [Cloud Bigtable Service Level Agreement](https://cloud.google.com/bigtable/sla) § "SLA Details"
  - [BigQuery Service Level Agreement](https://cloud.google.com/bigquery/sla) § "SLA Details"
  - [Cloud Spanner Overview](https://cloud.google.com/spanner/docs/overview) § "Key features"

---

#### 3.2 Planning for using a data warehouse
- **Domain Weight**: 20%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Designing the data model
  - Deciding degree of data normalization (Star/Snowflake vs nested/repeated)
  - Mapping business requirements
  - Defining architecture to support data access patterns
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_3_storing_data.md` (§ 3.2)
  - `03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md`
  - `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
- **Audit Assessment**: Excellent architectural discussion of Capacitor columnar layout, date/timestamp partitioning, clustering on high-cardinality keys, and logical vs physical storage billing. However:
  1. Lab 01 invents hallucinated `gcloud bi-engine` commands and crashes during Python JSON metric parsing.
  2. Partition quota inconsistency exists between files (4,000 vs 10,000 limit).
- **Required Remediation**: Remediate Lab 01 CLI commands and Python JSON parsing (`ISSUE-P0-05`, `ISSUE-P0-06`); harmonize partition quota limits (`ISSUE-P2-01`).
- **Official Documentation Citation**:
  - [BigQuery Partitioned Tables](https://cloud.google.com/bigquery/docs/partitioned-tables) § "Partitioning specifications"
  - [BigQuery Performance Compute Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-compute) § "Optimize query computation"

---

#### 3.3 Using a data lake
- **Domain Weight**: 20%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Managing the lake (configuring data discovery, access, and cost controls)
  - Processing data
  - Monitoring the data lake
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_3_storing_data.md` (§ 3.3)
  - `03_phase2_lifecycle_architecture/03_storage_modeling_and_lakehouse.md`
  - `06_phase5_case_studies/case_01_realtime_ecom_lakehouse.md`
  - `00_data_engineer_cli_cheat_sheet.md`
- **Audit Assessment**: Cloud Storage data lake architecture, Object Lifecycle Management (OLM), Autoclass transition rules, Bucket Lock compliance, and BigLake federation are thoroughly addressed. However:
  1. CLI cheat sheet (`00_cli:75-80`) uses invalid dual-region syntax `--location=us-central1,us-east4` and non-existent flag `--enable-turbo-replication`.
  2. Decision table omits 99.0% availability SLA for Regional Nearline, Coldline, and Archive.
- **Required Remediation**: Fix CLI cheat sheet storage flags (`ISSUE-P0-11`); add storage class SLA tiers to decision table (`ISSUE-P1-07`).
- **Official Documentation Citation**:
  - [Cloud Storage Storage Classes](https://cloud.google.com/storage/docs/storage-classes) § "Available storage classes"
  - [Cloud Storage Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle) § "Lifecycle actions"
  - [Cloud Storage Service Level Agreement](https://cloud.google.com/storage/sla) § "SLA Details"

---

#### 3.4 Designing for a data platform
- **Domain Weight**: 20%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Building a data platform based on requirements by using Google Cloud tools (Dataplex, Dataplex Catalog, BigQuery, Cloud Storage)
  - Building a federated governance model for distributed data systems
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_3_storing_data.md` (§ 3.4)
  - `02_phase1_glossary_and_taxonomy/05_governance_and_metadata.md`
  - `06_phase5_case_studies/case_03_enterprise_data_mesh.md`
- **Audit Assessment**: Exceptional enterprise Data Mesh implementation: Dataplex Lakes, Zones (Raw, Curated), Assets, Dataplex Catalog entry groups, custom aspects, glossaries, AutoDQ YAML specifications, and automated data lineage across BigQuery and Cloud Storage.
- **Official Documentation Citation**:
  - [Dataplex Overview](https://cloud.google.com/dataplex/docs/introduction) § "Architecture"
  - [Dataplex Auto Data Quality Overview](https://cloud.google.com/dataplex/docs/auto-data-quality-overview) § "Core concepts"
  - [Build a Data Mesh on Google Cloud Architecture Guide](https://cloud.google.com/architecture/build-a-data-mesh) § "Domain-oriented decentralized data ownership"

---

### Domain 4: Preparing and using data for analysis (~15% of exam)

#### 4.1 Preparing data for visualization
- **Domain Weight**: 15%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Connecting to tools
  - Precalculating fields
  - BigQuery features for business intelligence (e.g., BI Engine, materialized views)
  - Troubleshooting poor performing queries
  - Security, data masking, Identity and Access Management (IAM), Cloud DLP
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md` (§ 4.1)
  - `03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md`
  - `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
- **Audit Assessment**: Looker integration, Materialized Views smart rewrite, Query Plan analysis, Policy Tags with Dynamic Data Masking, and Authorized Views are thoroughly covered. However:
  1. Contains hallucinated CLI syntax `gcloud bi-engine capacities create` in `03_05_orchestration.md:396-401`.
  2. CLI cheat sheet uses invalid `bq mk --connection_type=BI_ENGINE` (lines 181-186).
- **Required Remediation**: Replace hallucinated BI Engine commands with official `bq update --bi_reservation_size` syntax or SQL DDL (`ISSUE-P1-04`, `ISSUE-P0-12`).
- **Official Documentation Citation**:
  - [BigQuery BI Engine Overview](https://cloud.google.com/bigquery/docs/bi-engine-overview) § "How BI Engine works"
  - [BigQuery Materialized Views Overview](https://cloud.google.com/bigquery/docs/materialized-views-intro) § "Smart tuning"
  - [BigQuery Column-Level Data Masking](https://cloud.google.com/bigquery/docs/column-data-masking-intro) § "Data masking rules"

---

#### 4.2 Preparing data for AI and ML
- **Domain Weight**: 15%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Preparing data for feature engineering, training and serving ML models (BigQuery ML)
  - Preparing unstructured data for embeddings and retrieval-augmented generation (RAG)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md` (§ 4.2)
  - `02_phase1_glossary_and_taxonomy/04_processing_and_compute.md`
  - `06_phase5_case_studies/case_04_ml_feature_store_and_rag.md`
- **Audit Assessment**: Rigorous coverage of BigQuery ML model architectures (`TRANSFORM` clause, boosted trees, k-means, matrix factorization), BigQuery Object Tables for unstructured images/audio in GCS, BigQuery Vector Search with `ML.GENERATE_EMBEDDING` and ScaNN indexes, and Vertex AI Feature Store. However:
  - `case_04.md:186` mislabels Bigtable read latency as a contractual vendor SLA.
- **Required Remediation**: Re-label latency target to internal SLO in `case_04.md` (`ISSUE-P2-04`).
- **Official Documentation Citation**:
  - [BigQuery ML Introduction](https://cloud.google.com/bigquery/docs/bqml-introduction) § "Supported models"
  - [BigQuery Vector Search](https://cloud.google.com/bigquery/docs/vector-search) § "Vector search overview"
  - [Apache Beam RunInference Transform](https://beam.apache.org/documentation/ml/runinference-overview/) § "Overview"

---

#### 4.3 Sharing data
- **Domain Weight**: 15%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Defining rules to share data
  - Publishing datasets
  - Publishing reports and visualizations
  - BigQuery sharing (Analytics Hub)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_4_preparing_and_using_data_for_analysis.md` (§ 4.3)
  - `03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md`
  - `06_phase5_case_studies/case_03_enterprise_data_mesh.md`
- **Audit Assessment**: Complete and accurate. Detailed architecture for Analytics Hub private/public exchanges, zero-copy linked datasets (zero data egress, subscriber pays compute), BigQuery Data Clean Rooms with aggregation thresholds and differential privacy noise, and Authorized Views/Datasets.
- **Official Documentation Citation**:
  - [Analytics Hub Overview](https://cloud.google.com/bigquery/docs/analytics-hub-introduction) § "Key concepts"
  - [BigQuery Data Clean Rooms](https://cloud.google.com/bigquery/docs/data-clean-rooms) § "Clean rooms overview"
  - [BigQuery Differential Privacy Overview](https://cloud.google.com/bigquery/docs/differential-privacy-overview) § "How differential privacy works"

---

### Domain 5: Maintaining and automating data workloads (~18% of exam)

#### 5.1 Optimizing resources
- **Domain Weight**: 18%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Minimizing costs per required business need for data
  - Ensuring enough resources are available for business-critical data processes
  - Deciding between persistent or job-based data clusters (Dataproc)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md` (§ 5.1)
  - `01_phase0_gcp_fundamentals/04_pricing_quotas_and_billing.md`
  - `03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md`
  - `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
- **Audit Assessment**: Thorough FinOps and compute capacity engineering: BigQuery Editions (Standard vs Enterprise vs Enterprise Plus) baseline vs autoscaling slots, reservation assignment hierarchies, Dataproc deployment models (persistent vs ephemeral vs Dataproc Serverless), and Spot VMs with Graceful Decommissioning.
- **Official Documentation Citation**:
  - [BigQuery Editions Overview](https://cloud.google.com/bigquery/docs/editions-intro) § "BigQuery editions"
  - [BigQuery Reservations Overview](https://cloud.google.com/bigquery/docs/reservations-intro) § "Reservations model"
  - [Dataproc Cluster Flexibility Modes](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/flexible-mode) § "Secondary workers"

---

#### 5.2 Designing automation and repeatability
- **Domain Weight**: 18%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Creating directed acyclic graphs (DAGs) for Cloud Composer
  - Scheduling and orchestrating jobs in a repeatable way
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md` (§ 5.2)
  - `03_phase2_lifecycle_architecture/05_orchestration_governance_and_serving.md`
  - `05_phase4_comparisons_and_decision_trees/03_ingestion_orchestration_comparisons.md`
  - `07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md`
- **Audit Assessment**: Theoretical coverage of idempotent pipeline execution (atomic `MERGE` statements, partition overwrite via `WRITE_TRUNCATE`), catchup/backfill parameterization, and Airflow sensor rescheduling (`mode='reschedule'` vs `poke`) is outstanding. However, Lab 04 DAG contains a fatal Python syntax crash `use_legacy_sql=false` and lacks environment creation commands.
- **Required Remediation**: Fix Lab 04 DAG syntax and environment creation (`ISSUE-P0-09`, `ISSUE-P0-10`).
- **Official Documentation Citation**:
  - [Cloud Composer DAG Writing Best Practices](https://cloud.google.com/composer/docs/composer-2/dag-writing-best-practices) § "Idempotence"
  - [Apache Airflow Sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) § "Sensor modes"
  - [BigQuery DML MERGE Statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement) § "MERGE syntax"

---

#### 5.3 Organizing workloads based on business requirements
- **Domain Weight**: 18%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Capacity management (e.g., BigQuery Editions and reservations)
  - Interactive or batch query jobs
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md` (§ 5.3)
  - `01_phase0_gcp_fundamentals/04_pricing_quotas_and_billing.md`
  - `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
- **Audit Assessment**: Rigorous coverage of BigQuery workload management: multi-project reservation hierarchies, department assignment rules, idle slot sharing across reservations (`ignore_idle_slots=false`), and Interactive vs Batch query execution priority queues.
- **Official Documentation Citation**:
  - [BigQuery Workload Management](https://cloud.google.com/bigquery/docs/reservations-workload-management) § "Assignment rules"
  - [Running Interactive and Batch Queries](https://cloud.google.com/bigquery/docs/running-queries#queries) § "Query priority"

---

#### 5.4 Monitoring and troubleshooting processes
- **Domain Weight**: 18%
- **Status**: **Fully covered (FC)**
- **Syllabus Sub-Bullets**:
  - Observability of data processes (Cloud Monitoring, Cloud Logging, BigQuery admin panel)
  - Monitoring planned usage
  - Troubleshooting error messages, billing issues, and quotas
  - Manage workloads (jobs, queries, compute capacity)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md` (§ 5.4)
  - `03_phase2_lifecycle_architecture/01_source_acquisition_and_batch_ingest.md`
  - `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
  - `07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md`
- **Audit Assessment**: Comprehensive observability guide: Cloud Monitoring metrics (`dataflow.googleapis.com/job/data_watermark_age`, `system_lag`), Cloud Logging log-based metrics, BigQuery `INFORMATION_SCHEMA` queries (slot contention, spill to disk), and Bigtable Key Visualizer heatmaps.
- **Official Documentation Citation**:
  - [Google Cloud Observability Documentation](https://cloud.google.com/monitoring/docs) § "Metrics overview"
  - [BigQuery INFORMATION_SCHEMA Views](https://cloud.google.com/bigquery/docs/information-schema-intro) § "Available views"
  - [Monitoring Dataflow Pipelines](https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring) § "Key Dataflow metrics"

---

#### 5.5 Maintaining awareness of failures and mitigating impact
- **Domain Weight**: 18%
- **Status**: **Covered but technically incorrect (TI)**
- **Syllabus Sub-Bullets**:
  - Designing system for fault tolerance and managing restarts
  - Running jobs in multiple regions or zones
  - Preparing for data corruption and missing data
  - Data replication and failover (e.g., Cloud SQL, Redis clusters)
- **Primary Handbook Files**:
  - `04_phase3_exam_domains/domain_5_maintaining_and_automating_data_workloads.md` (§ 5.5)
  - `03_phase2_lifecycle_architecture/06_dr_resilience_and_cost_defense.md`
  - `02_phase1_glossary_and_taxonomy/02_storage_and_databases.md`
  - `07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md`
- **Audit Assessment**: Solid architectural designs for Dataflow job draining, Pub/Sub Seek/Snapshot replay, BigQuery Time Travel and Table Clones, Cloud SQL cross-region read replicas, and Memorystore for Redis HA failover. However:
  - Fails on the Turbo Replication "100% SLA" claim in `03_06_dr.md:63` and Exam 1 Q13 explanation.
- **Required Remediation**: Correct Turbo Replication SLA claim to $\ge 99.9\%$ monthly volume conformance (`ISSUE-P0-01`, `ISSUE-P1-22`).
- **Official Documentation Citation**:
  - [Cloud Storage Service Level Agreement](https://cloud.google.com/storage/sla) § "SLA Details"
  - [BigQuery Cross-Region Dataset Replication](https://cloud.google.com/bigquery/docs/disaster-recovery-replicate-datasets) § "Replication overview"
  - [Memorystore for Redis High Availability](https://cloud.google.com/memorystore/docs/redis/high-availability) § "Failover architecture"
