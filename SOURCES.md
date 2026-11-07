# Authoritative Google Cloud Documentation Registry (SOURCES)

**Project**: Google Cloud Professional Data Engineer (PDE) Handbook Technical Validation  
**Registry Purpose**: Authoritative registry of primary official documentation sources, API references, architecture framework guides, and Service Level Agreements (SLAs) underpinning all technical validation findings.  
**Review Period**: November 2026  
**Last Verified Date**: 2026-11-07  

---

## 1. Governance & Standards Mandate

In strict accordance with the project integrity principles:
1. **Primary Sources Only**: All technical claims, quotas, CLI syntax, and architectural guidance in the handbook are validated against official Google Cloud documentation (`https://cloud.google.com/`) and Apache project standards (`https://beam.apache.org/`, `https://airflow.apache.org/`).
2. **Zero Secondary Reliance**: No unverified blog posts, third-party cheat sheets, or community forum discussions are accepted as authoritative ground truth.
3. **Explicit Citation Anchor**: Citations must reference specific documentation pages and section headings.

---

## 2. Authoritative Documentation Reference Registry

### 2.1 Storage, Databases & Disaster Recovery

| Service / Topic | Official Documentation Canonical URL | Key Section / Topic Evaluated | Last Verified Date |
|:---|:---|:---|:---:|
| **Cloud Storage SLA** | https://cloud.google.com/storage/sla | SLA Details, Monthly Replication Volume & Time Conformance | 2026-11-07 |
| **Cloud Storage Turbo Replication** | https://cloud.google.com/storage/docs/disaster-recovery | Configure Turbo Replication, Dual-Region Buckets | 2026-11-07 |
| **Cloud Storage Classes & Lifecycle** | https://cloud.google.com/storage/docs/storage-classes | Available storage classes, minimum storage durations | 2026-11-07 |
| **Cloud Storage Autoclass** | https://cloud.google.com/storage/docs/autoclass | How Autoclass works, default transition rules | 2026-11-07 |
| **Cloud Storage Quotas & Limits** | https://cloud.google.com/storage/quotas | Maximum object size (5 TiB), request rate limits | 2026-11-07 |
| **Cloud Spanner Overview & SLAs** | https://cloud.google.com/spanner/sla | Regional (99.99%) and Multi-Region (99.999%) Availability SLA | 2026-11-07 |
| **Cloud Spanner Schema Design** | https://cloud.google.com/spanner/docs/schema-and-data-model | Interleaved tables, TrueTime external consistency | 2026-11-07 |
| **Cloud Bigtable SLA** | https://cloud.google.com/bigtable/sla | Availability SLA Details, Multi-Cluster Routing App Profile Requirement | 2026-11-07 |
| **Cloud Bigtable Quotas & Performance** | https://cloud.google.com/bigtable/quotas | Storage density (5 TB SSD / 16 TB HDD), 100 MB max row size | 2026-11-07 |
| **Cloud Bigtable Schema Best Practices** | https://cloud.google.com/bigtable/docs/schema-design | Row key design, salting, hotspotting avoidance | 2026-11-07 |
| **Cloud SQL SLA** | https://cloud.google.com/sql/sla | Enterprise Edition HA (99.95%) vs Enterprise Plus HA (99.99%) | 2026-11-07 |
| **Cloud SQL Private IP Configuration** | https://cloud.google.com/sql/docs/postgres/configure-private-ip | Private Services Access (PSA) requirements | 2026-11-07 |
| **AlloyDB for PostgreSQL Architecture** | https://cloud.google.com/alloydb/docs/overview | Storage auto-scaling up to 128 TiB per cluster | 2026-11-07 |

---

### 2.2 BigQuery Data Warehousing & Analytics

| Service / Topic | Official Documentation Canonical URL | Key Section / Topic Evaluated | Last Verified Date |
|:---|:---|:---|:---:|
| **BigQuery Service Level Agreement** | https://cloud.google.com/bigquery/sla | Standard Edition (99.9%) vs Enterprise / Enterprise Plus (99.99%) | 2026-11-07 |
| **BigQuery Partitioned Tables Quotas** | https://cloud.google.com/bigquery/quotas#partitioned_tables | 10,000 max partitions per table, 4,000 modified per operation | 2026-11-07 |
| **BigQuery Editions & Reservations** | https://cloud.google.com/bigquery/docs/editions-intro | Standard, Enterprise, Enterprise Plus editions & slot autoscaling | 2026-11-07 |
| **BigQuery Workload Management** | https://cloud.google.com/bigquery/docs/reservations-workload-management | `--target_job_concurrency`, multi-tenant reservation hierarchies | 2026-11-07 |
| **BigQuery BI Engine Capacity** | https://cloud.google.com/bigquery/docs/bi-engine-reserve | `bq update --bi_reservation_size` syntax, DDL `ALTER BI_CAPACITY` | 2026-11-07 |
| **BigQuery Storage Overview & Billing** | https://cloud.google.com/bigquery/docs/storage_overview | Capacitor columnar format, logical vs physical storage billing | 2026-11-07 |
| **BigQuery Change Data Capture (CDC)** | https://cloud.google.com/bigquery/docs/change-data-capture | `max_staleness` option on tables, dynamic session overrides | 2026-11-07 |
| **BigQuery Storage Write API** | https://cloud.google.com/bigquery/docs/write-api | Exactly-once stream append, monotonic offsets | 2026-11-07 |
| **BigQuery Studio Gemini Integration** | https://cloud.google.com/bigquery/docs/write-queries-with-gemini | Prompting LLMs for query generation and SQL validation | 2026-11-07 |
| **BigQuery Vector Search** | https://cloud.google.com/bigquery/docs/vector-search | `VECTOR_SEARCH` syntax, ScaNN indexing | 2026-11-07 |
| **BigQuery Analytics Hub & Clean Rooms** | https://cloud.google.com/bigquery/docs/analytics-hub-introduction | Zero-copy linked datasets, Data Clean Rooms differential privacy | 2026-11-07 |

---

### 2.3 Streaming, Ingestion & Data Pipelines

| Service / Topic | Official Documentation Canonical URL | Key Section / Topic Evaluated | Last Verified Date |
|:---|:---|:---|:---:|
| **Google Cloud Pub/Sub SLA & Quotas** | https://cloud.google.com/pubsub/sla | 99.95% Availability SLA, 10 MB maximum message size | 2026-11-07 |
| **Pub/Sub Message Ordering & Dead Letter** | https://cloud.google.com/pubsub/docs/handling-failures | Ordering keys, dead-letter topics (`maxDeliveryAttempts`) | 2026-11-07 |
| **Apache Beam Programming Guide** | https://beam.apache.org/documentation/programming-guide/ | Windowing, triggers, Python SDK `AfterProcessingTime` / `AfterCount` | 2026-11-07 |
| **Dataflow Worker Security & Permissions** | https://cloud.google.com/dataflow/docs/concepts/security-and-permissions | User-managed worker service accounts (`--service_account_email`) | 2026-11-07 |
| **Dataflow Job Lifecycle & Draining** | https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline | `drain` command semantics, terminal state polling | 2026-11-07 |
| **Dataflow Flex Templates Execution** | https://cloud.google.com/dataflow/docs/guides/templates/run-flex-templates | `--template-file-gcs-location` flag syntax | 2026-11-07 |
| **Datastream Connection Profiles** | https://cloud.google.com/datastream/docs/reference/rest/v1/projects.locations.connectionProfiles | Mandatory connectivity flags (`--static-ip-connectivity`) | 2026-11-07 |
| **Datastream Stream Configuration** | https://cloud.google.com/datastream/docs/create-a-stream | JSON configuration parameters for source/destination | 2026-11-07 |
| **Datastream BigQuery Destination IAM** | https://cloud.google.com/datastream/docs/destination-bigquery | Required permissions for Datastream service agent | 2026-11-07 |

---

### 2.4 Orchestration, Governance & Security

| Service / Topic | Official Documentation Canonical URL | Key Section / Topic Evaluated | Last Verified Date |
|:---|:---|:---|:---:|
| **Cloud Composer Environment Provisioning** | https://cloud.google.com/composer/docs/composer-2/create-environments | CLI environment creation, `roles/composer.ServiceAgentV2Ext` | 2026-11-07 |
| **Cloud Composer DAG Writing Practices** | https://cloud.google.com/composer/docs/composer-2/dag-writing-best-practices | Idempotency, deferrable operators, Jinja templating rules | 2026-11-07 |
| **Dataproc Serverless PySpark Batch** | https://cloud.google.com/dataproc-serverless/docs/guides/submit-spark-batch | PySpark submission syntax (`--py-files`), dependency distribution | 2026-11-07 |
| **Dataproc Serverless Network Setup** | https://cloud.google.com/dataproc-serverless/docs/concepts/network | Private Google Access (PGA) subnetwork prerequisites | 2026-11-07 |
| **Dataplex Universal Catalog Overview** | https://cloud.google.com/dataplex/docs/introduction | Centralized metadata management, Aspect types, custom entry groups | 2026-11-07 |
| **Dataplex Auto Data Quality CLI** | https://cloud.google.com/dataplex/docs/use-auto-data-quality | Data quality scans, `--data-source-resource` flag syntax | 2026-11-07 |
| **BigQuery Customer-Managed Encryption Keys** | https://cloud.google.com/bigquery/docs/customer-managed-encryption | CMEK service agent email format (`serviceAccount:bq-${PROJECT_NUMBER}`) | 2026-11-07 |
| **Sensitive Data Protection Inspection** | https://cloud.google.com/sensitive-data-protection/docs/inspecting-storage | Inspect jobs, job triggers, de-identification templates | 2026-11-07 |
| **Private Google Access VIP Routing** | https://cloud.google.com/vpc/docs/private-google-access | Domain options (`private.googleapis.com` / `restricted.googleapis.com`) | 2026-11-07 |

---

## 3. Official Certification Specification Reference

| Specification Document | Authoritative Source URL | Version & Details | Review Date |
|:---|:---|:---|:---:|
| **Google Cloud Professional Data Engineer Exam Guide** | https://services.google.com/fh/files/misc/professional_data_engineer_exam_guide_english.pdf | Version 4.2 (English), 5 Domains, 19 Sub-Objectives | 2026-11-07 |
| **Google Cloud Architecture Framework** | https://cloud.google.com/architecture/framework | Pillars: System Design, Reliability, Security, Cost Optimization, Operational Excellence | 2026-11-07 |
| **Google Cloud Service Level Agreements Directory** | https://cloud.google.com/terms/sla | Master index of all legally binding Google Cloud SLAs | 2026-11-07 |
