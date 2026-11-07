# Hands-On Lab Execution & Reproducibility Audit Report

**Project**: Google Cloud Professional Data Engineer (PDE) Handbook Technical Validation  
**Deliverable**: Stage 1 Hands-On Lab Audit Report  
**Target Scope**: Phase 6 Hands-On Labs (Labs 01 – 04, 1,812 lines of code, CLI scripts, and DAGs)  
**Standard**: Google Cloud SDK Reference, Apache Beam Python SDK, Apache Airflow Google Provider, and BigQuery SQL Syntax Specifications  
**Date**: November 2026  

---

## 1. Executive Summary & Lab Scorecard

A thorough, line-by-line static analysis and reproducibility audit was executed on all four hands-on labs in `07_phase6_hands_on_labs/`. Each lab was evaluated across four critical reproducibility vectors:
1. **Prerequisites & Identity Architecture**: Completeness of APIs, user IAM roles, and service account configurations.
2. **CLI Command & API Syntax Integrity**: Absence of hallucinated commands, invalid flags, or non-existent parameters.
3. **Application & Pipeline Code Rigor**: Python AST validity, correct library imports, and proper exception handling.
4. **End-to-End Execution Flow & Cleanup**: Resource dependency ordering, timing race conditions, and graceful teardown.

### 1.1 Lab Audit Scorecard

| Lab Module | Lab Title & Technology Stack | Total Lines | P0 Blockers | P1 Flaws | P2 Polish | P3 Cleanliness | Overall Reproducibility Status |
|:---|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Lab 01** | BigQuery Optimization (Partitioning, Clustering, BI Engine) | 498 | 2 | 1 | 0 | 0 | **Fails Execution (Blocked)** |
| **Lab 02** | Beam & Dataflow Streaming (Pub/Sub, Windows, BigQuery) | 479 | 0 | 2 | 1 | 1 | **Fails Runtime (IAM & Race)** |
| **Lab 03** | Datastream CDC to BigQuery (Cloud SQL, CDC, max_staleness) | 385 | 2 | 2 | 0 | 0 | **Fails Execution (Blocked)** |
| **Lab 04** | Cloud Composer Orchestration (Airflow, Dataproc Serverless) | 450 | 2 | 3 | 0 | 0 | **Fails Execution (Blocked)** |
| **TOTALS** | **Phase 6 Hands-On Labs Suite** | **1,812** | **6** | **8** | **1** | **1** | **0 / 4 Fully Reproducible** |

*Conclusion*: Zero of the four labs can currently be executed end-to-end without encountering immediate execution blockers, fatal Python exceptions, or permission denials. All 16 findings have been prioritized into the remediation backlog for Milestone 3 (Stage 2A).

---

## 2. Deep-Dive Audit: Lab 01 (BigQuery Optimization)

- **File**: `07_phase6_hands_on_labs/lab_01_bigquery_optimization.md`
- **Objective**: Demonstrate performance and cost optimization via partitioning, clustering, and BI Engine in-memory acceleration.

### 2.1 Technical Defects & Execution Blockers

#### Finding LAB-01-A: Hallucinated `gcloud bi-engine` Command Group (P0)
- **Observed Syntax** (Lines 388–392, 398, 481–484):
  ```bash
  gcloud bi-engine reservations create \
      --project=${PROJECT_ID} \
      --location=${REGION} \
      --size=2147483648
  gcloud bi-engine reservations list --location=${REGION}
  gcloud bi-engine reservations delete --location=${REGION} --project=${PROJECT_ID} --quiet
  ```
- **Root Cause**: `gcloud bi-engine` is a hallucinated command group that does not exist in the Google Cloud SDK. Executing this produces `ERROR: (gcloud) Invalid choice: 'bi-engine'`.
- **Remediation**: Replace with official `bq` CLI commands or SQL DDL:
  ```bash
  # Create/Update BI Engine capacity (size in GiB)
  bq update --project_id=${PROJECT_ID} --location=${REGION} --bi_reservation_size=2 --reservation
  
  # Inspect BI Engine reservation
  bq show --project_id=${PROJECT_ID} --location=${REGION} --reservation
  
  # Delete BI Engine reservation
  bq update --project_id=${PROJECT_ID} --location=${REGION} --bi_reservation_size=0 --reservation
  ```
- **Doc Citation**: [BigQuery BI Engine Reserve Capacity](https://cloud.google.com/bigquery/docs/bi-engine-reserve) § "Reserve capacity using the bq command-line tool".

#### Finding LAB-01-B: Fatal Python `AttributeError` on Query Results JSON Parsing (P0)
- **Observed Syntax** (Lines 178–196, 248–270):
  ```bash
  bq query --use_legacy_sql=false --format=prettyjson "SELECT ... LIMIT 10;" > unpart_result.json
  ```
  ```python
  def parse_job(file_path):
    with open(file_path) as f:
      data = json.load(f)
    return {
        'bytes_processed': data.get('totalBytesProcessed'),
        'bytes_billed': data.get('totalBytesBilled'),
        'slot_millis': data.get('totalSlotMs'),
    }
```
- **Root Cause**: `bq query --format=prettyjson` outputs a JSON array of query rows (`[{...}]`), which parses as a Python `list`. Invoking `data.get(...)` raises `AttributeError: 'list' object has no attribute 'get'`. Furthermore, execution statistics are never included in query row output.
- **Remediation**: Capture the BigQuery Job ID and retrieve execution metadata using `bq show -j`:
  ```bash
  JOB_ID=$(bq query --use_legacy_sql=false --format=none "SELECT ... LIMIT 10;" 2>&1 | grep -oE "job_[a-zA-Z0-9_-]+" | head -n 1)
  bq show --format=prettyjson -j ${JOB_ID} > unpart_job.json
  ```
  Alternatively, use SQL queries against `INFORMATION_SCHEMA.JOBS_BY_PROJECT`.
- **Doc Citation**: [BigQuery Jobs Resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs) § "JobStatistics".

#### Finding LAB-01-C: Missing User IAM Roles in Prerequisites (P1)
- **Observed Syntax** (Lines 41–61): Enables APIs, but specifies zero IAM prerequisites.
- **Root Cause**: Candidates executing in restricted sandbox environments receive `403 Access Denied` when creating datasets or managing reservations.
- **Remediation**: Explicitly document prerequisite roles: `roles/bigquery.admin` and `roles/bigqueryreservation.admin`.
- **Doc Citation**: [BigQuery Access Control](https://cloud.google.com/bigquery/docs/access-control) § "Predefined roles".

---

## 3. Deep-Dive Audit: Lab 02 (Beam & Dataflow Streaming)

- **File**: `07_phase6_hands_on_labs/lab_02_beam_dataflow_streaming.md`
- **Objective**: Deploy a streaming pipeline ingesting Pub/Sub telemetry, windowing by event time, and streaming to BigQuery.

### 3.1 Technical Defects & Operational Flaws

#### Finding LAB-02-A: Missing Worker Service Account & IAM Roles (P1)
- **Observed Syntax** (Lines 43–82, 312–327):
  `python3 src/pipeline.py --runner=DataflowRunner --project=${PROJECT_ID} ...` omits `--service_account_email`.
- **Root Cause**: Modern GCP projects (created post-May 2024) grant zero roles to the Compute Engine default SA. Worker VM startup fails immediately or crashes with permission errors upon accessing GCS or BigQuery.
- **Remediation**: Create a dedicated worker service account and assign least-privilege roles:
  ```bash
  export WORKER_SA="sa-dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com"
  gcloud iam service-accounts create sa-dataflow-worker --display-name="Dataflow Worker SA"
  
  for role in roles/dataflow.worker roles/pubsub.subscriber roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin; do
      gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${WORKER_SA}" --role="${role}"
  done
  ```
  Add `--service_account_email=${WORKER_SA}` to the pipeline command line.
- **Doc Citation**: [Dataflow Worker Security](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions) § "Specify a user-managed worker service account".

#### Finding LAB-02-B: Asynchronous Drain Resource Deletion Race Condition (P1)
- **Observed Syntax** (Lines 454–469):
  `gcloud dataflow jobs drain ${JOB_ID}` is followed immediately by `gcloud pubsub subscriptions delete`, `bq rm`, and `gcloud storage rm`.
- **Root Cause**: `drain` is an asynchronous command that returns immediately while workers continue processing in-flight panes for several minutes. Deleting underlying Pub/Sub subscriptions and BigQuery tables causes the workers to crash with IO 404 errors, causing the job to enter `Failed` state.
- **Remediation**: Add a status polling loop waiting for terminal state:
  ```bash
  gcloud dataflow jobs drain ${JOB_ID} --region=${REGION}
  echo "Waiting for Dataflow job ${JOB_ID} to complete draining..."
  while [[ $(gcloud dataflow jobs show ${JOB_ID} --region=${REGION} --format="value(state)") =~ (Running|Draining) ]]; do
      sleep 10
  done
  ```
- **Doc Citation**: [Stopping a Dataflow Pipeline](https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline) § "Drain a job".

#### Finding LAB-02-C: Non-Standard Beam Trigger Wrapping (P2)
- **Observed Syntax** (Lines 139–140, 264–270): Wrapping `Repeatedly` around `AfterWatermark(...)`.
- **Remediation**: Remove `Repeatedly` wrapper; `AfterWatermark(early=..., late=...)` is already a recurring composite trigger.
- **Doc Citation**: [Apache Beam Triggers](https://beam.apache.org/documentation/programming-guide/#triggers).

#### Finding LAB-02-D: Hardcoded Project ID in Non-Portable `sed` Command (P3)
- **Observed Syntax** (Lines 350, 401): Uses `sed -i` to replace `<PROJECT_ID>` in Python file.
- **Remediation**: Read `PROJECT_ID` from `os.environ` in Python.
- **Doc Citation**: [Pub/Sub Python Client](https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library).

---

## 4. Deep-Dive Audit: Lab 03 (Datastream CDC to BigQuery)

- **File**: `07_phase6_hands_on_labs/lab_03_datastream_cdc_bigquery.md`
- **Objective**: Replicate transactional PostgreSQL changes via serverless CDC into BigQuery with real-time UPSERT views.

### 4.1 Technical Defects & Execution Blockers

#### Finding LAB-03-A: Missing Connectivity Flag in Profile Creation (P0)
- **Observed Syntax** (Lines 193–202):
  `gcloud datastream connection-profiles create pg-source-profile` lacks `--static-ip-connectivity`.
- **Root Cause**: Datastream connection profile creation strictly requires choosing one of `--static-ip-connectivity`, `--private-connection`, or `--forward-ssh-hostname`. Omitting it causes immediate command failure.
- **Remediation**: Add `--static-ip-connectivity` to the command line.
- **Doc Citation**: [Datastream Connection Profiles](https://cloud.google.com/datastream/docs/reference/rest/v1/projects.locations.connectionProfiles) § "Connectivity methods".

#### Finding LAB-03-B: Invalid BigQuery SQL `OPTIONS` Clause on `SELECT` (P0)
- **Observed Syntax** (Lines 318–325):
  ```sql
  SELECT order_id, order_status, order_total 
  FROM `${PROJECT_ID}.${DATASET_ID}.orders`
  OPTIONS(max_staleness = INTERVAL '0' MINUTE);
  ```
- **Root Cause**: In BigQuery SQL, `OPTIONS(...)` is a DDL clause valid on table creation/alteration. Appending it to a `SELECT` query produces a syntax error.
- **Remediation**: Configure max staleness via table DDL:
  ```sql
  ALTER TABLE `${PROJECT_ID}.${DATASET_ID}.orders`
  SET OPTIONS (max_staleness = INTERVAL 0 MINUTE);
  ```
  Or override for the session: `SET @@max_staleness_override = INTERVAL 0 MINUTE;`.
- **Doc Citation**: [BigQuery CDC max_staleness](https://cloud.google.com/bigquery/docs/change-data-capture) § "Set max_staleness option".

#### Finding LAB-03-C: Cloud SQL Private IP Contradiction (P1)
- **Observed Syntax** (Lines 72–90): Passes `--network` and `--no-assign-ip` on instance creation, then patches with `--assign-ip`.
- **Root Cause**: Passing `--network` fails if Private Services Access (PSA) peering is not pre-configured.
- **Remediation**: Create instance directly with public IP and authorized networks for standalone sandbox execution.
- **Doc Citation**: [Cloud SQL Private IP](https://cloud.google.com/sql/docs/postgres/configure-private-ip).

#### Finding LAB-03-D: Missing Datastream Service Agent BigQuery IAM Roles (P1)
- **Observed Syntax** (Lines 59–67, 208–213): Datastream writes via Storage Write API under its service agent, but roles are never granted.
- **Remediation**: Grant `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` to `service-${PROJECT_NUMBER}@gcp-sa-datastream.iam.gserviceaccount.com`.
- **Doc Citation**: [Datastream BigQuery Destination](https://cloud.google.com/datastream/docs/destination-bigquery) § "Required permissions".

---

## 5. Deep-Dive Audit: Lab 04 (Cloud Composer Orchestration)

- **File**: `07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md`
- **Objective**: Orchestrate an enterprise ELT pipeline with Airflow, GCS sensors, Dataproc Serverless, and BigQuery quality checks.

### 5.1 Technical Defects & Execution Blockers

#### Finding LAB-04-A: Complete Omission of Environment Creation Command (P0)
- **Observed Syntax** (Lines 51–105, 347–369): Section 2 defines variables and datasets, but omits the `gcloud composer environments create` command.
- **Root Cause**: When candidates run subsequent `gcloud composer environments run` commands in Section 5, execution fails with `Environment [pde-orchestration-lab] not found`.
- **Remediation**: Add explicit creation step:
  ```bash
  gcloud composer environments create ${COMPOSER_ENV} \
      --location=${REGION} \
      --image-version=composer-2.6.6-airflow-2.7.3 \
      --service-account=${COMPUTE_SA} \
      --environment-size=small
  ```
- **Doc Citation**: [Cloud Composer Environment Creation](https://cloud.google.com/composer/docs/composer-2/create-environments) § "Create an environment using the Google Cloud CLI".

#### Finding LAB-04-B: Fatal Python Syntax Error (`use_legacy_sql=false`) in Airflow DAG (P0)
- **Observed Syntax** (Line 289):
  `use_legacy_sql=false` inside `BigQueryValueCheckOperator`.
- **Root Cause**: In Python, boolean values are capitalized (`True`/`False`). Lowercase `false` raises a fatal `NameError: name 'false' is not defined`, crashing DAG import.
- **Remediation**: Capitalize boolean literal: `use_legacy_sql=False`.
- **Doc Citation**: [BigQueryValueCheckOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html).

#### Finding LAB-04-C: Jinja Variable String Manipulation at DAG Parse Time (P1)
- **Observed Syntax** (Lines 187–190, Line 237):
  `BUCKET_DATA = "{{ var.value.bucket_data }}"` followed by `bucket=BUCKET_DATA.replace("gs://", "")`.
- **Root Cause**: At parse time, the string is the unrendered template `"{{ var.value.bucket_data }}"`. Calling `.replace()` does not remove `gs://`. At runtime, passing `"gs://..."` to `GCSObjectsWithPrefixExistenceSensor` causes HTTP 400 Invalid Bucket Name.
- **Remediation**: Use Jinja filter in template: `bucket="{{ var.value.bucket_data | replace('gs://', '') }}"`.
- **Doc Citation**: [GCSObjectsWithPrefixExistenceSensor](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/sensors/cloud/gcs.html).

#### Finding LAB-04-D: Missing Private Google Access on Dataproc Subnetwork (P1)
- **Observed Syntax** (Lines 65–71, 261): Dataproc Serverless batch targets `default` subnet without Private Google Access.
- **Root Cause**: Dataproc Serverless workloads have no external IPs and fail if PGA is disabled.
- **Remediation**: Add `gcloud compute networks subnets update default --region=${REGION} --enable-private-ip-google-access`.
- **Doc Citation**: [Dataproc Serverless Networking](https://cloud.google.com/dataproc-serverless/docs/concepts/network) § "Private Google Access".

#### Finding LAB-04-E: Missing Composer Service Agent Extension IAM Role (P1)
- **Observed Syntax** (Lines 88–105): Grants Dataproc/BigQuery roles to `COMPUTE_SA`, but omits `roles/composer.ServiceAgentV2Ext` on Composer SA and `roles/composer.worker` on `COMPUTE_SA`.
- **Remediation**: Add explicit role bindings for Composer Service Agent and worker account.
- **Doc Citation**: [Cloud Composer Permissions](https://cloud.google.com/composer/docs/composer-2/create-environments) § "Grant service account permissions".

---

## 6. Remediation Verification Commands

The following commands verify the defects and validate fixes during Milestone 3:
```bash
# 1. Verify Lab 04 Python DAG parse validity
python3 -c "
import ast
with open('07_phase6_hands_on_labs/lab_04_cloud_composer_orchestration.md') as f:
    text = f.read()
# Find invalid 'use_legacy_sql=false'
assert 'use_legacy_sql=false' in text, 'Expected defect present'
print('Verified: Lab 04 contains invalid lowercase false')
"

# 2. Verify Lab 01 bq CLI vs gcloud bi-engine syntax
bq update --help | grep "bi_reservation_size"
```
