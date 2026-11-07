# Google Cloud Data Engineer CLI Master Cheat Sheet
**Primary Toolsets**: `gcloud`, `bq`, `cbt`, and modern `gcloud storage`  
**Target Role**: Google Cloud Principal Data Architect / Professional Data Engineer

This reference document compiles production CLI commands used daily by Google Cloud Data Engineers. Every command includes a concise operational explanation and a structured breakdown of every flag used.

---

## Table of Contents
1. [Core Platform & IAM (`gcloud`)](#1-core-platform--iam-gcloud)
2. [Cloud Storage (`gcloud storage`)](#2-cloud-storage-gcloud-storage)
3. [BigQuery CLI (`bq`)](#3-bigquery-cli-bq)
4. [Cloud Pub/Sub (`gcloud pubsub`)](#4-cloud-pubsub-gcloud-pubsub)
5. [Cloud Dataflow (`gcloud dataflow`)](#5-cloud-dataflow-gcloud-dataflow)
6. [Cloud Dataproc (`gcloud dataproc`)](#6-cloud-dataproc-gcloud-dataproc)
7. [Cloud Bigtable (`gcloud bigtable` & `cbt`)](#7-cloud-bigtable-gcloud-bigtable--cbt)
8. [Cloud Spanner (`gcloud spanner`)](#8-cloud-spanner-gcloud-spanner)
9. [Cloud SQL & AlloyDB (`gcloud sql` & `gcloud alloydb`)](#9-cloud-sql--alloydb-gcloud-sql--gcloud-alloydb)
10. [Datastream CDC (`gcloud datastream`)](#10-datastream-cdc-gcloud-datastream)
11. [Cloud Composer (`gcloud composer`)](#11-cloud-composer-gcloud-composer)
12. [Dataplex & Data Governance (`gcloud dataplex`)](#12-dataplex--data-governance-gcloud-dataplex)
13. [Security & Encryption (`gcloud kms` & `gcloud dlp`)](#13-security--encryption-gcloud-kms--gcloud-dlp)
14. [Networking & Private Connectivity (`gcloud compute`)](#14-networking--private-connectivity-gcloud-compute)

---

## 1. Core Platform & IAM (`gcloud`)

### 1.1 Switch Project & Account
```bash
gcloud config set project my-pde-analytics-prod
gcloud config set account data-lead@company.com
```
- **Explanation**: Points your active CLI context to the target project and authenticated Google identity.

### 1.2 Create Service Account with Zero Keys
```bash
gcloud iam service-accounts create sa-dataflow-pipeline \
    --display-name="Dataflow Execution Identity" \
    --description="Used by Dataflow workers to read Pub/Sub and write to BigQuery"
```
- **Explanation**: Creates a service account identity without generating risky downloadable JSON keys.
- **Flags**:
  - `--display-name`: Friendly UI name visible in Cloud Console.
  - `--description`: Explains workload ownership and security purpose.

### 1.3 Assign Scoped Predefined Role to Service Account
```bash
gcloud projects add-iam-policy-binding my-pde-analytics-prod \
    --member="serviceAccount:sa-dataflow-pipeline@my-pde-analytics-prod.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor" \
    --condition='expression=resource.name.startsWith("projects/my-pde-analytics-prod/datasets/curated_"),title=RestrictToCurated'
```
- **Explanation**: Grants table write access strictly to datasets prefixed with `curated_` using IAM Conditions.
- **Flags**:
  - `--member`: The identity being granted access (`user:`, `serviceAccount:`, or `group:`).
  - `--role`: Specific predefined or custom role identifier.
  - `--condition`: CEL (Common Expression Language) rule restricting permission scope.

### 1.4 Impersonate Service Account (Production Best Practice)
```bash
gcloud compute instances list \
    --impersonate-service-account="sa-pipeline-deployer@my-pde-analytics-prod.iam.gserviceaccount.com"
```
- **Explanation**: Runs the command using the permissions of a service account without needing its private key file.
- **Flags**:
  - `--impersonate-service-account`: Requests short-lived OAuth 2.0 access tokens dynamically via IAM credentials API.

---

## 2. Cloud Storage (`gcloud storage`)

### 2.1 Create Dual-Region Bucket with Turbo Replication
```bash
gcloud storage buckets create gs://corp-lake-landing-us \
    --location=us-central1,us-east4 \
    --default-storage-class=STANDARD \
    --uniform-bucket-level-access \
    --enable-turbo-replication
```
- **Explanation**: Provisions an active-active dual-region landing bucket with a guaranteed 15-minute cross-region RPO.
- **Flags**:
  - `--location`: Pair of regions forming the dual-region boundary.
  - `--default-storage-class`: Default class for new objects (`STANDARD`, `NEARLINE`, `COLDLINE`, `ARCHIVE`).
  - `--uniform-bucket-level-access`: Disables legacy object-level ACLs; enforces centralized Cloud IAM only (PDE requirement).
  - `--enable-turbo-replication`: Guarantees 100% of objects replicate to the paired region within 15 minutes.

### 2.2 Configure Object Lifecycle Management
```bash
gcloud storage buckets update gs://corp-lake-landing-us \
    --lifecycle-file=lifecycle_rules.json
```
- **Explanation**: Applies declarative lifecycle rules (e.g., transition to Nearline after 30 days, delete after 365 days).
- **Flags**:
  - `--lifecycle-file`: Path to JSON file defining conditions (`age`, `createdBefore`, `matchesStorageClass`) and actions (`SetStorageClass`, `Delete`).

### 2.3 Enable Autoclass (Automated Cost Optimization)
```bash
gcloud storage buckets update gs://corp-lake-landing-us \
    --enable-autoclass
```
- **Explanation**: Automatically transitions objects between Standard, Nearline, Coldline, and Archive based on access patterns without retrieval fees or lifecycle rule maintenance.
- **Flags**:
  - `--enable-autoclass`: Activates Google's automatic object access-tiering algorithm.

### 2.4 Synchronize On-Premises or Cloud Directories
```bash
gcloud storage rsync /data/landing_export/ gs://corp-lake-landing-us/transactions/ \
    --recursive \
    --delete-unmatched-destination-objects \
    --checksums-only
```
- **Explanation**: Mirrors a local directory into Cloud Storage.
- **Flags**:
  - `--recursive` (`-r`): Traverses all subdirectories.
  - `--delete-unmatched-destination-objects` (`-d`): Deletes files in GCS that no longer exist in the source.
  - `--checksums-only` (`-c`): Compares hashes (CRC32c/MD5) instead of file modification timestamps.

---

## 3. BigQuery CLI (`bq`)

### 3.1 Create Partitioned and Clustered Table
```bash
bq mk \
    --table \
    --schema="order_id:STRING,customer_id:INT64,order_amount:NUMERIC,order_ts:TIMESTAMP" \
    --time_partitioning_field=order_ts \
    --time_partitioning_type=DAY \
    --time_partitioning_expiration=7776000 \
    --clustering_fields="customer_id,order_id" \
    --require_partition_filter=true \
    --description="Curated retail transactions" \
    analytics_prod.fact_orders
```
- **Explanation**: Creates a production analytical table optimized for minimal scanned bytes and fast customer lookups.
- **Flags**:
  - `--table`: Specifies table object creation.
  - `--schema`: Inline schema or path to a JSON schema file.
  - `--time_partitioning_field`: The TIMESTAMP/DATE column used to divide physical storage.
  - `--time_partitioning_type`: Granularity of partition splits (`DAY`, `HOUR`, `MONTH`, `YEAR`).
  - `--time_partitioning_expiration`: Seconds (7,776,000 = 90 days) after which older partitions automatically delete.
  - `--clustering_fields`: Up to 4 comma-separated columns sorted inside each partition block.
  - `--require_partition_filter=true`: Prevents accidental full table scans by rejecting queries missing a `WHERE order_ts` clause.

### 3.2 Dry-Run Cost Estimation Before Execution
```bash
bq query \
    --use_legacy_sql=false \
    --dry_run \
    "SELECT customer_id, SUM(order_amount) FROM analytics_prod.fact_orders WHERE order_ts >= '2026-11-01' GROUP BY 1"
```
- **Explanation**: Validates query syntax and outputs the exact number of bytes the query will scan without executing or incurring billing charges.
- **Flags**:
  - `--use_legacy_sql=false`: Enforces standard ANSI SQL.
  - `--dry_run`: Calculates execution graph and bytes read without dispatching query workers.

### 3.3 Enforce Cost Ceiling on Query Execution
```bash
bq query \
    --use_legacy_sql=false \
    --maximum_bytes_billed=10737418240 \
    "SELECT * FROM analytics_prod.fact_orders"
```
- **Explanation**: Safeguards billing by failing queries that exceed a set scan limit.
- **Flags**:
  - `--maximum_bytes_billed`: Maximum bytes allowed (10 GB in bytes). If query plans exceed this, execution aborts instantly.

### 3.4 Point-in-Time Recovery (BigQuery Time Travel)
```bash
bq cp \
    analytics_prod.fact_orders@-3600000 \
    analytics_prod.fact_orders_recovered_1hr_ago
```
- **Explanation**: Restores a table to its exact state 1 hour ago without needing backups.
- **Flags**:
  - `@-3600000`: Offset snapshot decorator in milliseconds (-3,600,000 ms = -1 hour; supports up to 7 days).

### 3.5 Create BI Engine In-Memory Reservation
```bash
bq mk \
    --connection_type=BI_ENGINE \
    --location=us-central1 \
    --bi_reservation_size=10737418240 \
    --project_id=my-pde-analytics-prod
```
- **Explanation**: Allocates 10 GB of sub-millisecond memory cache in `us-central1` for dashboard queries.
- **Flags**:
  - `--bi_reservation_size`: RAM allocation in bytes (10 GB).

---

## 4. Cloud Pub/Sub (`gcloud pubsub`)

### 4.1 Create Topic with CMEK and Extended Retention
```bash
gcloud pubsub topics create clickstream-events \
    --message-retention-duration=7d \
    --topic-encryption-key=projects/my-pde-analytics-prod/locations/us-central1/keyRings/data-ring/cryptoKeys/pubsub-key
```
- **Explanation**: Creates a high-throughput event buffer retaining messages for 7 days, encrypted with customer-managed keys.
- **Flags**:
  - `--message-retention-duration`: Retains published unacknowledged or acknowledged messages for replay (`7d` max).
  - `--topic-encryption-key`: Cloud KMS key resource ID for customer-managed encryption.

### 4.2 Create Pull Subscription with Ordering Keys & Dead-Letter Queue
```bash
gcloud pubsub subscriptions create sub-order-processing-sequential \
    --topic=order-events \
    --ack-deadline=60 \
    --enable-message-ordering \
    --dead-letter-topic=order-poison-pill-dlq \
    --max-delivery-attempts=5 \
    --retain-acked-messages \
    --message-retention-duration=7d
```
- **Explanation**: Ingestion endpoint providing per-entity serialized delivery, best-effort poison-pill routing, and 7-day replay capability for sequential microservices (e.g. Cloud Run / GCE).  
  *Architectural Warning*: Do **not** enable `--enable-message-ordering` on subscriptions read by Cloud Dataflow streaming pipelines. Dataflow processes messages concurrently and manages stream ordering natively via Apache Beam event-time watermarks; enabling ordering keys on a Dataflow subscription degrades pipeline throughput and risks worker starvation.
- **Flags**:
  - `--ack-deadline`: Seconds subscriber has to acknowledge before message is redelivered (10 to 600 seconds).
  - `--enable-message-ordering`: Guarantees messages sharing an `orderingKey` are delivered in strict order (subject to ~1 MB/s publisher throughput ceiling per key in a region; do NOT use for Dataflow).
  - `--dead-letter-topic`: Topic where poison pills are forwarded on a best-effort basis when processing fails repeatedly.
  - `--max-delivery-attempts`: Nacks or timeouts before evicting message to DLQ (5 attempts).
  - `--retain-acked-messages`: Keeps acknowledged messages on disk so subscribers can seek backwards in time.

### 4.3 Seek Subscription Backward in Time (Stream Replay)
```bash
gcloud pubsub subscriptions seek sub-order-processing-sequential \
    --time="2026-11-07T00:00:00Z"
```
- **Explanation**: Rewinds message delivery pointers to re-process stream data following a pipeline bug fix.
- **Flags**:
  - `--time`: UTC ISO-8601 timestamp to which message ack pointers are reset.

### 4.4 Create Direct Zero-ETL BigQuery Subscription
```bash
gcloud pubsub subscriptions create sub-orders-direct-bq \
    --topic=order-events \
    --bigquery-table=my-pde-analytics-prod:analytics_raw.raw_orders \
    --use-topic-schema \
    --write-metadata
```
- **Explanation**: Streams events directly into BigQuery without deploying a Dataflow or Cloud Run pipeline.
- **Flags**:
  - `--bigquery-table`: Target BigQuery table destination (`project:dataset.table`).
  - `--use-topic-schema`: Validates message payload against Pub/Sub Avro/Protobuf schema before writing.
  - `--write-metadata`: Writes `subscription_name`, `message_id`, `publish_time`, and `attributes` as table columns.

---

## 5. Cloud Dataflow (`gcloud dataflow`)

### 5.1 Run Production Flex Template with Streaming Engine
```bash
gcloud dataflow flex-template run realtime-ecom-pipeline-prod \
    --template-file-gcs-path=gs://corp-dataflow-templates/templates/retail_stream.json \
    --region=us-central1 \
    --service-account-email=sa-dataflow-pipeline@my-pde-analytics-prod.iam.gserviceaccount.com \
    --network=analytics-vpc \
    --subnetwork=regions/us-central1/subnetworks/data-private-subnet \
    --disable-public-ips \
    --enable-streaming-engine \
    --max-workers=30 \
    --num-workers=4 \
    --worker-machine-type=n2-standard-4 \
    --parameters=\
inputSubscription=projects/my-pde-analytics-prod/subscriptions/sub-clickstream-dataflow,\
outputTable=my-pde-analytics-prod:analytics_prod.fact_clickstream,\
deadLetterBucket=gs://corp-lake-landing-us/dlq/
```
- **Explanation**: Deploys a containerized Apache Beam streaming pipeline with private networking, autoscaling, and off-VM state management.
- **Flags**:
  - `--template-file-gcs-path`: GCS pointer to container specification file.
  - `--region`: Regional control plane executing the pipeline.
  - `--service-account-email`: Worker VM execution identity.
  - `--disable-public-ips`: Prevents worker VMs from having public Internet IPs (requires Private Google Access).
  - `--enable-streaming-engine`: Offloads pipeline window state and shuffle operations from worker VMs to Google's specialized backend infrastructure.
  - `--max-workers`: Ceiling on autoscaling worker instances.
  - `--num-workers`: Initial baseline worker count.
  - `--worker-machine-type`: Compute Engine VM profile per worker.
  - `--parameters`: Comma-separated runtime pipeline parameters (`ValueProvider`).

### 5.2 Gracefully Drain Active Streaming Job
```bash
gcloud dataflow jobs drain 2026-11-07_01_00_00-1234567890 \
    --region=us-central1
```
- **Explanation**: Safely shuts down a streaming job without data loss by finishing in-flight windows and acknowledging uncommitted records.
- **Flags**:
  - `drain` vs `cancel`: `drain` finishes in-flight bundles and closes windows; `cancel` halts execution immediately, risking lost in-memory state.

---

## 6. Cloud Dataproc (`gcloud dataproc`)

### 6.1 Create Cost-Optimized Ephemeral Spark Cluster
```bash
gcloud dataproc clusters create ephemeral-spark-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --single-node=false \
    --master-machine-type=n2-standard-4 \
    --master-boot-disk-size=100GB \
    --num-workers=2 \
    --worker-machine-type=n2-standard-4 \
    --num-secondary-workers=8 \
    --secondary-worker-type=spot \
    --image-version=2.2-debian12 \
    --enable-component-gateway \
    --no-address \
    --subnet=data-private-subnet \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh \
    --metadata=spark-bigquery-connector-version=0.36.0 \
    --max-idle=30m \
    --graceful-decommission-timeout=1h
```
- **Explanation**: Creates a cost-effective Spark cluster running 80% Spot VMs, private networking, auto-termination after 30 minutes of inactivity, and safe decommissioning.
- **Flags**:
  - `--num-secondary-workers`: Additional worker nodes dedicated to processing without HDFS master responsibilities.
  - `--secondary-worker-type=spot`: Uses preemptible/Spot VMs (up to 70–90% cost reduction).
  - `--enable-component-gateway`: Provides secure HTTPS access to Spark UI, YARN Resource Manager, and Jupyter without opening public firewall ports.
  - `--no-address`: Eliminates external public IP addresses (internal VPC routing only).
  - `--max-idle=30m`: Automatically deletes the cluster if no jobs are submitted within 30 minutes.
  - `--graceful-decommission-timeout=1h`: Waits for workers facing Spot preemption to finish running tasks and evacuate shuffle data before terminating.

### 6.2 Submit Dataproc Serverless PySpark Batch (No Cluster Management)
```bash
gcloud dataproc batches submit pyspark gs://corp-spark-jobs/scripts/aggregate_sales.py \
    --region=us-central1 \
    --service-account=sa-dataproc@my-pde-analytics-prod.iam.gserviceaccount.com \
    --subnet=data-private-subnet \
    --deps-pkg=gs://corp-spark-jobs/dependencies/helpers.zip \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.36.0.jar \
    --version=2.2 \
    -- --input_date=2026-11-07
```
- **Explanation**: Executes a distributed PySpark script entirely serverless. Google provisions, scales, and deletes Spark resources automatically.
- **Flags**:
  - `batches submit pyspark`: Targets the Dataproc Serverless engine.
  - `--deps-pkg`: Custom zipped Python packages distributed to executor runtimes.
  - `--jars`: Java dependencies (e.g., BigQuery connector) placed on driver and executor classpaths.
  - `--`: Separator passing remaining arguments directly to the Python script's `argparse`.

---

## 7. Cloud Bigtable (`gcloud bigtable` & `cbt`)

### 7.1 Create Production Multi-Cluster Bigtable Instance
```bash
gcloud bigtable instances create retail-profile-store \
    --display-name="Realtime Retail Customer Store" \
    --cluster-config=id=retail-cluster-c1,zone=us-central1-b,nodes=3,autoscaling-min=3,autoscaling-max=20,autoscaling-cpu-target=65 \
    --cluster-config=id=retail-cluster-c2,zone=us-central1-c,nodes=3,autoscaling-min=3,autoscaling-max=20,autoscaling-cpu-target=65
```
- **Explanation**: Provisions an active-active, multi-zone Bigtable instance with automated CPU-based node autoscaling (targets sub-10ms latency for warm SSD clusters under 65% CPU load; note that Bigtable SLAs guarantee availability, not a specific latency number).
- **Flags**:
  - `--cluster-config`: Sets cluster ID, zone, baseline node count, and scaling thresholds.
  - `autoscaling-cpu-target=65`: Scales out nodes when average CPU load exceeds 65%.

### 7.2 Create Table and Column Families (`cbt` CLI)
```bash
cbt -instance=retail-profile-store createtable user_sessions
cbt -instance=retail-profile-store createfamily user_sessions cf_meta
cbt -instance=retail-profile-store createfamily user_sessions cf_events
```
- **Explanation**: Initializes the wide-column table and defines column families (storage locality groups).

### 7.3 Configure Compound Garbage Collection Policies (`cbt`)
```bash
cbt -instance=retail-profile-store setgcpolicy user_sessions cf_events \
    maxage=30d or maxversions=3
```
- **Explanation**: Enforces automated data lifecycle purging directly on tablet storage to bound disk costs and latency.
- **Flags**:
  - `maxage=30d or maxversions=3`: Automatically purges cells older than 30 days OR keeps only the latest 3 versions per cell.

### 7.4 Read Row with Key Pattern (`cbt`)
```bash
cbt -instance=retail-profile-store read user_sessions \
    prefix="user_4981#" \
    columns="cf_events:last_page_view"
```
- **Explanation**: Performs a fast point/range scan on lexicographically sorted rowkeys.
- **Flags**:
  - `prefix`: Restricts scan to rows beginning with the specified string.
  - `columns`: Prunes retrieval to specified column family and qualifier.

---

## 8. Cloud Spanner (`gcloud spanner`)

### 8.1 Create Regional or Multi-Region Spanner Instance
```bash
gcloud spanner instances create core-ledger-spanner \
    --config=regional-us-central1 \
    --description="Enterprise Core Transactional Ledger" \
    --processing-units=500
```
- **Explanation**: Allocates a globally consistent, horizontally scalable relational database instance with TrueTime consistency.
- **Flags**:
  - `--config`: Topology definition (`regional-us-central1` for 99.99% SLA, or `nam-eur-asia1` multi-region for 99.999% SLA).
  - `--processing-units`: Compute capacity units (1,000 processing units = 1 node; fractional allocations support 100 to 900 PUs).

### 8.2 Execute Schema DDL Update
```bash
gcloud spanner databases ddl update core_banking_db \
    --instance=core-ledger-spanner \
    --ddl='CREATE TABLE Accounts (AccountId STRING(36) NOT NULL, Balance NUMERIC) PRIMARY KEY(AccountId)'
```
- **Explanation**: Executes online schema migration without taking the database offline or locking tables.

---

## 9. Cloud SQL & AlloyDB (`gcloud sql` & `gcloud alloydb`)

### 9.1 Create Production Cloud SQL PostgreSQL with Logical Replication
```bash
gcloud sql instances create pg-source-orders \
    --database-version=POSTGRES_15 \
    --tier=db-custom-4-16384 \
    --region=us-central1 \
    --availability-type=REGIONAL \
    --storage-type=SSD \
    --storage-size=100GB \
    --storage-auto-increase \
    --network=projects/my-pde-analytics-prod/global/networks/analytics-vpc \
    --no-assign-ip \
    --database-flags=cloudsql.logical_decoding=on
```
- **Explanation**: Creates a high-availability PostgreSQL database instance restricted to private VPC access, configured for CDC tools like Datastream.
- **Flags**:
  - `--availability-type=REGIONAL`: Enables active-standby automatic failover across zones.
  - `--no-assign-ip`: Prevents assignment of a public IPv4 address.
  - `--network`: Binds the database to your private VPC via Private Service Access (PSA).
  - `--database-flags=cloudsql.logical_decoding=on`: Enables WAL replication slots for log-based CDC.

### 9.2 Create High-Performance AlloyDB Cluster
```bash
gcloud alloydb clusters create alloydb-analytics-cluster \
    --region=us-central1 \
    --network=analytics-vpc \
    --password="SecurePassword123!"
```
- **Explanation**: Deploys an AlloyDB cluster with disaggregated compute and storage, providing up to 4x transactional performance and 100x analytical query acceleration compared to standard PostgreSQL.

---

## 10. Datastream CDC (`gcloud datastream`)

### 10.1 Create PostgreSQL Source Connection Profile
```bash
gcloud datastream connection-profiles create cp-postgres-orders \
    --location=us-central1 \
    --type=postgresql \
    --postgresql-hostname=10.10.1.5 \
    --postgresql-port=5432 \
    --postgresql-username=datastream_cdc \
    --postgresql-password="CDCSecretPassword" \
    --postgresql-database=production_db
```
- **Explanation**: Configures connectivity and credentials for Datastream to tail WAL logs from the source database.

### 10.2 Create Real-Time CDC Replication Stream to BigQuery
```bash
gcloud datastream streams create stream-orders-to-bigquery \
    --location=us-central1 \
    --source=cp-postgres-orders \
    --destination=cp-bigquery-lake \
    --postgresql-source-config=replication_slot=datastream_slot,publication=datastream_pub \
    --bigquery-destination-config=data_freshness=0s
```
- **Explanation**: Establishes continuous, zero-ETL replication from PostgreSQL tables directly into BigQuery.
- **Flags**:
  - `--postgresql-source-config`: Points to the source database's logical replication slot and publication.
  - `--bigquery-destination-config=data_freshness=0s`: Streams updates directly to BigQuery storage without batch-staging delays.

---

## 11. Cloud Composer (`gcloud composer`)

### 11.1 Create Cloud Composer 3 Airflow Environment
```bash
gcloud composer environments create prod-orchestrator-composer \
    --location=us-central1 \
    --image-version=composer-3-airflow-2.9.1 \
    --service-account=sa-composer@my-pde-analytics-prod.iam.gserviceaccount.com \
    --environment-size=medium
```
- **Explanation**: Deploys a managed Apache Airflow 2 orchestration environment running on auto-managed GKE infrastructure.
- **Flags**:
  - `--image-version`: Pinpoints specific Composer and Airflow software versions.
  - `--environment-size`: Preconfigured workload profile (`small`, `medium`, `large`) governing scheduler, triggerer, and web server capacity.

### 11.2 Trigger Airflow DAG Execution via CLI
```bash
gcloud composer environments run prod-orchestrator-composer \
    --location=us-central1 \
    dags trigger -- retail_nightly_elt_pipeline
```
- **Explanation**: Triggers an Airflow DAG run directly from terminal or CI/CD pipelines.

---

## 12. Dataplex & Data Governance (`gcloud dataplex`)

### 12.1 Create Dataplex Lake & Curated Zone
```bash
gcloud dataplex lakes create customer-lake \
    --location=us-central1 \
    --display-name="Customer 360 Lake"

gcloud dataplex zones create curated-zone \
    --location=us-central1 \
    --lake=customer-lake \
    --type=CURATED \
    --resource-location-type=SINGLE_REGION
```
- **Explanation**: Builds the logical hierarchy for a enterprise Data Mesh architecture.
- **Flags**:
  - `--type=CURATED` vs `RAW`: Curated zones store cleaned, formatted data (Parquet, BigQuery tables); Raw zones hold incoming source formats.

### 12.2 Attach BigQuery Dataset as Dataplex Asset
```bash
gcloud dataplex assets create asset-orders-mart \
    --location=us-central1 \
    --lake=customer-lake \
    --zone=curated-zone \
    --resource-type=BIGQUERY_DATASET \
    --resource-name=projects/my-pde-analytics-prod/datasets/analytics_prod \
    --discovery-enabled
```
- **Explanation**: Registers physical storage into Dataplex for centralized metadata cataloging, discovery, and automated schema parsing.
- **Flags**:
  - `--discovery-enabled`: Automatically scans datasets, discovers schemas, and publishes metadata to Dataplex Catalog.

---

## 13. Security & Encryption (`gcloud kms` & `gcloud dlp`)

### 13.1 Create Customer-Managed Key (CMEK) with Auto-Rotation
```bash
gcloud kms keyrings create data-ring \
    --location=us-central1

gcloud kms keys create lake-encryption-key \
    --location=us-central1 \
    --keyring=data-ring \
    --purpose=encryption \
    --rotation-period=90d \
    --next-rotation-time="2027-01-01T00:00:00Z"
```
- **Explanation**: Generates a cryptographic key for CMEK encryption with automatic 90-day rotation.
- **Flags**:
  - `--purpose=encryption`: Defines symmetric encryption/decryption key usage.
  - `--rotation-period=90d`: Rotates key versions automatically every 90 days.

### 13.2 Grant BigQuery Service Agent Permission to Use CMEK Key
```bash
gcloud kms keys add-iam-policy-binding lake-encryption-key \
    --location=us-central1 \
    --keyring=data-ring \
    --member="serviceAccount:bq-my-pde-analytics-prod@bigquery-encryption.iam.gserviceaccount.com" \
    --role="roles/cloudkms.cryptoKeyEncrypterDecrypter"
```
- **Explanation**: Allows the BigQuery managed service agent to encrypt and decrypt storage using your CMEK key.

### 13.3 Inspect Cloud Storage for PII Using Sensitive Data Protection (DLP)
```bash
gcloud dlp jobs create \
    --inspect-job \
    --storage-bucket=corp-lake-landing-us \
    --info-types=EMAIL_ADDRESS,US_SOCIAL_SECURITY_NUMBER,CREDIT_CARD_NUMBER \
    --min-likelihood=LIKELY
```
- **Explanation**: Runs an asynchronous scan across Cloud Storage objects to identify sensitive PII.
- **Flags**:
  - `--info-types`: Predefined detectors to match in file payloads.
  - `--min-likelihood`: Match confidence threshold (`VERY_UNLIKELY`, `UNLIKELY`, `POSSIBLE`, `LIKELY`, `VERY_LIKELY`).

---

## 14. Networking & Private Connectivity (`gcloud compute`)

### 14.1 Enable Private Google Access on Subnetwork
```bash
gcloud compute networks subnets update data-private-subnet \
    --region=us-central1 \
    --enable-private-google-access
```
- **Explanation**: Allows VMs, Dataproc nodes, and Dataflow workers without external IP addresses to access Google APIs (BigQuery, GCS, Pub/Sub) via internal routing.
- **Flags**:
  - `--enable-private-google-access`: Routes internal VM traffic targeting default Google API domains (`*.googleapis.com`) to Google's internal networkVIPs (`199.36.153.8/30`).

### 14.2 Create Firewall Rule for Internal Pipeline Communication
```bash
gcloud compute firewall-rules create allow-internal-data-mesh \
    --network=analytics-vpc \
    --action=ALLOW \
    --direction=INGRESS \
    --rules=tcp:0-65535,udp:0-65535,icmp \
    --source-ranges=10.10.0.0/16
```
- **Explanation**: Permits worker-to-worker internal communication required by Spark shuffle services and Dataflow engines across the VPC.
- **Flags**:
  - `--direction=INGRESS`: Evaluates inbound network packets.
  - `--source-ranges`: CIDR block restricted to internal subnets.
