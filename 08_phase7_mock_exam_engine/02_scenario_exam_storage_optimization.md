# Scenario Exam 02: Storage, Analysis, ML & Cost Optimization

This exam evaluates real-world troubleshooting, high-performance data modeling, machine learning deployment, and cloud economics aligned with **Domain 3 (Storing Data)**, **Domain 4 (Preparing and Using Data for Analysis)**, and **Domain 5 (Maintaining and Automating Data Workloads)** of the Google Cloud Professional Data Engineer certification.

---

### Question 1 (Domain 3: Bigtable Row Key Design & Hotspotting)
**Scenario**: A telecommunications enterprise is designing a time-series store in Cloud Bigtable to ingest 50 million network diagnostic ping metrics per minute across 10,000 cell towers. The monitoring team frequently queries the most recent 10 minutes of pings for a specific cell tower ID. A junior engineer proposes the row key format: `TIMESTAMP#CELL_TOWER_ID` (e.g., `2026-02-14T12:00:00#TOWER_9918`). When testing at scale, write throughput degrades significantly and CPU utilization on a single node hits 100% while other nodes remain idle. How should you redesign the row key to eliminate the bottleneck and maintain query efficiency?

- **A.** Change the row key to `UUID#TIMESTAMP` to ensure uniform random distribution across all Bigtable tablet servers.
- **B.** Change the row key to `CELL_TOWER_ID#REVERSE_TIMESTAMP` (e.g., `TOWER_9918#(Long.MAX_VALUE - timestamp)`).
- **C.** Change the row key to `MD5(CELL_TOWER_ID)#TIMESTAMP` and query using regular expression filters.
- **D.** Increase the number of nodes in the Bigtable cluster from 4 to 40 without modifying the row key schema.

**Correct Answer**: **B**

#### Deep Architectural Explanation
Cloud Bigtable stores data in lexicographically sorted string order across distributed tablets:
1. **The Anti-Pattern (Sequential Timestamp First)**: Starting a row key with a timestamp (`TIMESTAMP#CELL_TOWER_ID`) means every current incoming write across all 10,000 towers has an identical or monotonically increasing prefix. All write traffic routes to the single tablet server hosting that timestamp range, completely defeating horizontal distribution (hotspotting).
2. **The Optimal Pattern (`CELL_TOWER_ID#REVERSE_TIMESTAMP`)**:
   - Starting with `CELL_TOWER_ID` distributes writes uniformly across all tablet servers in the cluster, as different tower IDs map to different alphabetical partitions.
   - Using a **reverse timestamp** (`Long.MAX_VALUE - timestamp`) ensures that the latest ping events for any given tower appear first lexicographically. Queries fetching the most recent 10 minutes of metrics can execute a highly efficient, contiguous forward row scan over a narrow slice of keys rather than scanning backwards.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Leading with a random UUID distributes writes evenly, but makes range scans for a specific cell tower impossible. Retrieving a tower's last 10 minutes of data would require a catastrophic full table scan across millions of rows.
- **Option C is incorrect**: Hashing the tower ID (`MD5(CELL_TOWER_ID)`) prevents sequential tower prefix scanning, and ascending timestamps still put the newest records at the end of the tower block, which is less efficient for fetching the most recent events.
- **Option D is incorrect**: Adding nodes does not fix hotspotting caused by a monotonically increasing key prefix. All writes will still slam into the single tablet holding the latest timestamp boundary.

**Official Reference**: [Cloud Bigtable Row Key Design Best Practices](https://cloud.google.com/bigtable/docs/schema-design#row-keys)

---

### Question 2 (Domain 3: BigQuery Partitioning & Clustering Architecture)
**Scenario**: An e-commerce platform maintains a 50 TB BigQuery table containing 5 years of user order history (`orders`). The table is queried hundreds of times a day by analysts and operational dashboards. 80% of queries filter on a date range (typically the last 7 to 30 days) and frequently filter or group by `store_id`, `customer_region`, and `order_status`. Queries currently scan the entire 50 TB dataset, costing over \$300 per execution under on-demand pricing. How should you restructure this table to minimize bytes scanned and slot consumption?

- **A.** Shard the table into daily separate tables named `orders_YYYYMMDD` and query them using wildcard unions.
- **B.** Partition the table by `order_date` (daily) and cluster by `store_id`, `customer_region`, and `order_status`.
- **C.** Cluster the table by `order_date`, `store_id`, and `customer_region` without configuring partitioning.
- **D.** Create a Materialized View over the entire table without partitioning or clustering filters.

**Correct Answer**: **B**

#### Deep Architectural Explanation
The optimal BigQuery storage topology pairs **partitioning** with **clustering**:
1. **Partitioning by `order_date`**: Segregates storage into distinct physical segments by calendar day. When queries filter on `WHERE order_date BETWEEN ...`, BigQuery's metadata layer prunes non-matching partitions entirely before scanning. Scanning 30 days out of 5 years immediately reduces scanned data volume by ~98%.
2. **Clustering by `store_id`, `customer_region`, `order_status`**: Within each date partition, BigQuery collocates and sorts rows based on the cluster columns. When queries filter by specific store IDs or regions, BigQuery skips non-matching 1 GB columnar blocks (block pruning).
3. Together, this combination drops scanned bytes from 50 TB down to tens of gigabytes per query, reducing cost by over 99%.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Date-sharded tables (`orders_YYYYMMDD`) are a legacy anti-pattern. They create massive metadata overhead, degrade query optimization, limit tables to 10,000 partitions max, and complicate schema evolution compared to native partitioned tables.
- **Option C is incorrect**: Clustering without partitioning does not guarantee hard partition pruning boundaries and cannot leverage partition-level management features like partition expiration or partition filter requirements.
- **Option D is incorrect**: A materialized view without underlying partitioning on the base table will scan excessive base storage during incremental refreshes and does not optimize ad-hoc analytical queries filtering on arbitrary column combinations.

**Official Reference**: [BigQuery Partitioning and Clustering](https://cloud.google.com/bigquery/docs/partitioned-tables)

---

### Question 3 (Domain 4: In-Warehouse Machine Learning with BigQuery ML)
**Scenario**: A digital media publisher wants to build and deploy a machine learning model to predict customer churn based on subscription usage metrics stored in BigQuery. The data science team wants to train a Gradient Boosted Tree model and perform batch inference directly inside the data warehouse to avoid exporting sensitive customer features outside the security boundary. Which BigQuery ML statement correctly trains this model?

- **A.** `CREATE OR REPLACE MODEL \`ml_dataset.churn_model\` OPTIONS(model_type='BOOSTED_TREE_CLASSIFIER', input_label_cols=['churned']) AS SELECT * FROM \`ml_dataset.customer_features\`;`
- **B.** `EXPORT MODEL \`ml_dataset.churn_model\` OPTIONS(model_type='RANDOM_FOREST') TO 'gs://my-bucket/model/' AS SELECT * FROM \`ml_dataset.customer_features\`;`
- **C.** `CREATE OR REPLACE MODEL \`ml_dataset.churn_model\` OPTIONS(model_type='LINEAR_REG', optimize_strategy='AUTO') AS SELECT * FROM \`ml_dataset.customer_features\`;`
- **D.** `TRAIN MODEL \`ml_dataset.churn_model\` USING TENSORFLOW WITH DATA SELECT * FROM \`ml_dataset.customer_features\`;`

**Correct Answer**: **A**

#### Deep Architectural Explanation
BigQuery ML (BQML) democratizes machine learning by allowing SQL analysts to train and evaluate models directly within BigQuery storage using standard SQL DDL:
1. The syntax `CREATE OR REPLACE MODEL <dataset.model_name> OPTIONS(model_type='BOOSTED_TREE_CLASSIFIER', input_label_cols=['label_column']) AS SELECT ...` trains an XGBoost-based ensemble classifier.
2. Training and subsequent batch inference (`ML.PREDICT()`) execute entirely within BigQuery's managed compute slots, eliminating the need to provision Vertex AI training clusters, manage Docker images, or export feature sets across network boundaries.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: `EXPORT MODEL` is used to dump an already-trained model artifact to Cloud Storage, not to train a new model.
- **Option C is incorrect**: `LINEAR_REG` trains a continuous regression model (predicting numeric values), which is incorrect for a binary classification problem like customer churn (`churned` = true/false).
- **Option D is incorrect**: `TRAIN MODEL ... USING TENSORFLOW` is invalid BQML syntax.

**Official Reference**: [BigQuery ML Boosted Tree Classifier Documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-boosted-tree)

---

### Question 4 (Domain 5: BigQuery Edition & Capacity Management)
**Scenario**: An enterprise enterprise data warehouse experiences severe query concurrency bottlenecks during month-end financial reporting. Hundreds of analysts execute complex dashboard queries simultaneously, causing queries to queue and latency to spike from seconds to tens of minutes. The organization operates under BigQuery on-demand pricing and has hit the regional concurrency and slot limits. The Chief Technology Officer mandates that month-end query latency must remain consistent, but monthly cloud spend must be predictable without paying for peak capacity 24/7. Which capacity strategy should you deploy?

- **A.** Switch to BigQuery Enterprise Edition, create an Enterprise reservation with a baseline of 500 slots, enable Autoscaling with a max of 2,000 slots, and assign the production reporting project to this reservation.
- **B.** Request a permanent quota increase to 50,000 on-demand slots and maintain on-demand billing.
- **C.** Provision a BigQuery Enterprise Plus multi-year annual commitment of 5,000 flat slots dedicated solely to reporting.
- **D.** Create 10 duplicate BigQuery datasets across different regions and route analyst queries randomly using a round-robin DNS load balancer.

**Correct Answer**: **A**

#### Deep Architectural Explanation
BigQuery Editions (Standard, Enterprise, Enterprise Plus) decouple capacity from unpredictable on-demand pricing while providing intelligent elasticity:
1. **Reservations & Autoscaling**: In BigQuery Enterprise Edition, you can set a modest **baseline capacity** (e.g., 500 slots) to handle regular day-to-day operations at a steady, predictable cost.
2. **Dynamic Slot Autoscaling**: When month-end surges occur, BigQuery automatically scales out additional slots in increments of 100 slots up to the configured ceiling (2,000 slots). Once the query surge abates, the autoscaled slots immediately scale down, avoiding idle billing.
3. **Reservation Assignments**: Assigning the reporting project or folder to the reservation prevents bursty reporting queries from starving ETL pipelines or other mission-critical workloads.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: On-demand pricing does not guarantee dedicated slot isolation, and on-demand queries can still suffer from queueing during multi-tenant regional congestion. Furthermore, high concurrency under on-demand can lead to uncontrollable, unpredictable billing spikes.
- **Option C is incorrect**: Purchasing a permanent 5,000 slot annual commitment forces the company to pay for 5,000 slots 24/7/365, wasting massive budget when month-end reporting only runs for a few days each month.
- **Option D is incorrect**: Replicating datasets across regions introduces cross-region egress costs, replication lag, and architectural fragmentation, and cannot be load-balanced via DNS because BigQuery is a managed API service.

**Official Reference**: [BigQuery Reservations & Slot Autoscaling](https://cloud.google.com/bigquery/docs/reservations-intro)

---

### Question 5 (Domain 3: Cloud Spanner Interleaved Tables & Joins)
**Scenario**: You are designing a high-concurrency relational data model in Cloud Spanner for a banking application with two tables: `Customers` (1 million rows) and `Accounts` (5 million rows). Each customer has an average of 5 accounts. The application frequently executes queries joining `Customers` and `Accounts` on `customer_id` with sub-10 millisecond latency SLAs. When stress-testing under heavy load, queries that join the two tables exhibit high latency and excessive network communication across Spanner splits. How should you design the schema?

- **A.** Define `Accounts` as an **interleaved table** within `Customers` using the `INTERLEAVE IN PARENT Customers` clause.
- **B.** Denormalize all customer and account data into a single wide table with JSON attributes.
- **C.** Create a secondary non-interleaved index on `Accounts(customer_id) STORING (account_balance)`.
- **D.** Migrate the entire workload to Cloud Bigtable.

**Correct Answer**: **A**

#### Deep Architectural Explanation
In Cloud Spanner's distributed storage architecture:
1. Non-interleaved tables distribute rows across split boundaries based on their primary keys. Joining two independent tables often requires Spanner to execute distributed cross-split network calls across multiple server nodes.
2. **Table Interleaving**: By declaring `INTERLEAVE IN PARENT Customers ON DELETE CASCADE`, Spanner physically collocates child rows (`Accounts`) with the parent row (`Customers`) on the same physical storage split and server.
3. Performing a join between parent and child on the primary key executes locally in memory on the same node without network RPCs, achieving the sub-10ms latency requirement under massive concurrency.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: Denormalizing into raw JSON documents breaks relational integrity, eliminates type safety, complicates partial updates, and impairs analytical querying.
- **Option C is incorrect**: A secondary index helps index lookups, but does not solve the physical collocation requirement across splits for regular parent-child queries.
- **Option D is incorrect**: Bigtable lacks ACID multi-row transactions, foreign key constraints, and relational SQL join capabilities needed for core transactional banking accounts.

**Official Reference**: [Cloud Spanner Interleaved Tables](https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables)

---

### Question 6 (Domain 4: Data Governance & Lineage with Dataplex)
**Scenario**: A healthcare enterprise operates a hybrid lakehouse where raw telemetry lands in Cloud Storage buckets, structured tables live in BigQuery, and transformation jobs execute across Dataproc and Dataflow. Regulators require complete auditability of data lineage: for any column in an executive dashboard, the organization must be able to trace every transformation step back to the source landing file. The organization wants an automated, centralized solution that does not require building custom metadata scrapers. What should you implement?

- **A.** Enable **Dataplex Data Lineage** across the Google Cloud Organization, and view end-to-end lineage graphs directly in Dataplex Catalog.
- **B.** Instruct developers to manually update an Excel spreadsheet after every pipeline commit and upload it to GCS.
- **C.** Deploy Apache Atlas on a persistent Dataproc cluster and write custom Airflow Python hooks to parse SQL strings.
- **D.** Configure BigQuery audit logs to stream to Cloud Logging and write custom regex queries to parse table names.

**Correct Answer**: **A**

#### Deep Architectural Explanation
Google Cloud **Dataplex Data Lineage** provides automated, native tracking of data origins, transformations, and destinations:
1. **Zero-Code Instrumentation**: When enabled, services like BigQuery, Cloud Dataflow, and Dataproc automatically capture execution events and register lineage metadata without requiring custom code or manual hooks.
2. **Visual & API Governance**: Lineage graphs are natively visualized in Dataplex Catalog and exposed via the Data Lineage API, allowing data stewards to trace provenance at both the dataset and table/column level for regulatory compliance.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: Manual spreadsheet documentation is error-prone, impossible to maintain at scale, and fails regulatory compliance audits.
- **Option C is incorrect**: Running a self-managed Apache Atlas cluster adds massive maintenance, VM operational costs, and manual parser integration when Google Cloud provides automated serverless data lineage natively.
- **Option D is incorrect**: Parsing raw audit logs with regex is fragile, fails to capture multi-stage dataflow transformations or complex subqueries, and does not provide an interactive visual lineage graph.

**Official Reference**: [Dataplex Data Lineage Overview](https://cloud.google.com/dataplex/docs/data-lineage)

---

### Question 7 (Domain 3: BigQuery Column-Level Security & Data Masking)
**Scenario**: An analytics platform contains a BigQuery table `users` with columns: `user_id`, `full_name`, `email_address`, `credit_card_hash`, and `state_code`. Business analysts require access to query `user_id` and `state_code` for market analysis, but corporate compliance forbids them from viewing `full_name` and `email_address`. However, customer service representatives must be able to view masked emails (e.g., `e****@example.com`), while compliance officers must view full plaintext. How should you implement this access control policy without creating duplicate tables?

- **A.** Create three separate authorized views over the base table and grant different IAM roles to each view.
- **B.** Define a Dataplex Policy Tag taxonomy with column-level security and dynamic data masking rules, attach tags to the table columns, and grant fine-grained reader and masked reader roles.
- **C.** Export the table daily into three separate GCS buckets with encrypted CSVs.
- **D.** Use SQL functions to manually hash values in a scheduled Airflow pipeline.

**Correct Answer**: **B**

#### Deep Architectural Explanation
BigQuery integrates with **Policy Tags** and **Dynamic Data Masking** in Dataplex / Data Catalog:
1. **Single Source of Truth**: Eliminates the overhead of maintaining multiple authorized views, pipelines, or duplicate tables.
2. **Granular Access Control**:
   - Compliance Officers receive the `roles/datacatalog.categoryFineGrainedReader` role on the Policy Tag, allowing full plaintext visibility.
   - Customer Service Reps receive the `roles/bigquery.maskedReader` role, which automatically masks the column using preconfigured masking routines (e.g., email masking `e****@example.com` or hash masking) at query time.
   - Business Analysts without either role receive an immediate authorization error if they attempt to query the sensitive columns, but can query non-sensitive columns without interruption.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Authorized views require maintaining separate DDLs, duplicate permission sets, and break if the underlying schema changes, creating an administrative nightmare across dozens of tables.
- **Option C is incorrect**: Exporting multiple CSV files creates unmanaged data sprawl, risks security breaches, and incurs unnecessary compute and storage billing.
- **Option D is incorrect**: Hardcoding hashing in ETL pipelines permanently destroys the raw data for authorized personas who genuinely need plaintext access.

**Official Reference**: [BigQuery Column-Level Security and Data Masking](https://cloud.google.com/bigquery/docs/column-data-masking-intro)

---

### Question 8 (Domain 5: Cost Defense & Query Budgets)
**Scenario**: Multiple external BI teams and ad-hoc analysts run queries against a central BigQuery data warehouse billed under on-demand pricing. Last month, a contractor ran an accidental unpartitioned Cartesian cross-join query that scanned 80 TB of data, costing \$500 for a single job. Management requires automated guardrails preventing any individual query from scanning more than 1 TB of data, without revoking query permissions from the users. How can you enforce this limit?

- **A.** Implement the `--maximum_bytes_billed` control on client connections or configure custom query quotas per user using BigQuery Quotas.
- **B.** Grant all users `roles/viewer` instead of `roles/bigquery.user`.
- **C.** Delete all unpartitioned tables in the project.
- **D.** Set up a Cloud Billing alert that triggers a Cloud Function to disable the BigQuery API when spend hits \$1,000.

**Correct Answer**: **A**

#### Deep Architectural Explanation
Google Cloud BigQuery provides two distinct mechanisms to enforce query cost limits:
1. **`maximum_bytes_billed`**: Clients and BI tools can specify this parameter on query jobs. If BigQuery's query planner estimates that the bytes to be billed exceed this threshold (e.g., 1 TB = `1099511627776` bytes), the query fails immediately **before running**, incurring zero cost.
2. **BigQuery Custom Quotas**: Project administrators can configure **custom query quotas per user per day** (e.g., 2 TB / user / day) directly in the Google Cloud Console. When a user exceeds their daily scan quota, subsequent queries are blocked until the next quota window resets.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: `roles/viewer` revokes the ability to run queries altogether, preventing analysts from doing their jobs.
- **Option C is incorrect**: Deleting tables destroys business data and does not prevent users from running expensive queries against partitioned tables where filters are omitted.
- **Option D is incorrect**: Disabling the BigQuery API brings down the entire company's data operations and breaks mission-critical production pipelines. Furthermore, billing alerts have a processing latency of several hours, meaning the expensive query would have already completed and been billed long before the alert triggered.

**Official Reference**: [BigQuery Custom Quotas and Cost Controls](https://cloud.google.com/bigquery/docs/custom-quotas)

---

### Question 9 (Domain 3: Bigtable Garbage Collection & Compaction)
**Scenario**: You are maintaining a Cloud Bigtable instance storing user session tokens. Each session token expires after 72 hours. Your table contains a single column family named `sessions`. Despite regular customer churn, disk utilization has grown continuously to 8 TB, and Bigtable storage costs are increasing steadily. You check your code and confirm that old tokens are never explicitly deleted by application code. How should you automate data cleanup to reclaim disk space?

- **A.** Write a nightly Dataflow batch job that scans the table and executes `MutateRows` delete operations for all keys older than 72 hours.
- **B.** Configure a **Garbage Collection (GC) Rule** on the `sessions` column family with `max_age` set to 3 days (72 hours).
- **C.** Delete the Bigtable table every Sunday and recreate it from scratch.
- **D.** Enable Bigtable autoscaling with target CPU set to 50%.

**Correct Answer**: **B**

#### Deep Architectural Explanation
Cloud Bigtable provides native, automated **Garbage Collection Policies** defined at the column family level:
1. **Rule Types**: You can configure rules based on **maximum age** (`max_age`, e.g., 3 days), **maximum number of versions** (`max_versions`), or logical combinations (`AND`/`OR`).
2. **Zero-Overhead Reclamation**: Once configured, expired cells are automatically excluded from read queries immediately. Bigtable's background compaction processes then physically purge the expired data from underlying Colossus SSTables during routine disk compactions, reclaiming storage without consuming client compute or network bandwidth.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Running a manual Dataflow scan to delete rows generates massive unnecessary compute costs, consumes Bigtable read/write throughput, and creates high write amplification.
- **Option C is incorrect**: Dropping the table drops active, unexpired sessions for legitimate users currently logged into the platform.
- **Option D is incorrect**: Bigtable autoscaling adjusts the number of compute worker nodes based on CPU and storage utilization; it does not delete or garbage-collect application data.

**Official Reference**: [Cloud Bigtable Garbage Collection Policies](https://cloud.google.com/bigtable/docs/garbage-collection)

---

### Question 10 (Domain 4: Data Clean Rooms & Privacy-Preserving Analytics)
**Scenario**: A national retail bank and an airline partner want to collaborate on customer acquisition analytics. The airline wants to join its frequent flyer loyalty member list with the bank's customer list to identify overlapping high-net-worth customers for co-branded credit card offers. However, data privacy regulations (GDPR and CCPA) strictly prohibit either party from sharing raw Personally Identifiable Information (PII) or viewing individual customer records belonging to the other party. How should you architect this data exchange on Google Cloud?

- **A.** Have the airline export its customer table to a GCS bucket, grant read access to the bank's service account, and trust the bank's analysts not to look at PII.
- **B.** Configure a **BigQuery Data Clean Room** via Analytics Hub, share subscriber listings with differential privacy / aggregation constraints, and execute privacy-preserving join queries.
- **C.** Write a Cloud Run service that accepts customer emails, generates SHA-256 hashes, and uploads the hashed CSVs to a public web server.
- **D.** Establish a cross-project VPC peering network between both organizations' Google Cloud projects.

**Correct Answer**: **B**

#### Deep Architectural Explanation
**BigQuery Data Clean Rooms** (built on Google Cloud Analytics Hub) enable secure, multi-party data collaboration without revealing underlying raw PII:
1. **Data Isolation**: Each party contributes data to an isolated clean room environment managed within BigQuery.
2. **Analysis Rules & Query Restrictions**: The data owner enforces **Aggregation-only constraints** and **Differential Privacy** thresholds (e.g., minimum group thresholds where queries only return aggregated cohort counts if results include at least 50 distinct individuals).
3. Neither party can execute `SELECT *` or view individual raw records, enabling regulatory compliance under GDPR and CCPA while unlocking joint analytics.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Directly sharing raw files in GCS gives the bank full access to PII, committing a severe regulatory violation.
- **Option C is incorrect**: Simple unsalted SHA-256 hashes are easily reversible via dictionary and rainbow table attacks, and publishing them to a public server introduces catastrophic compliance exposure.
- **Option D is incorrect**: VPC peering establishes private network connectivity for VMs; it has nothing to do with data sharing governance, privacy policies, or analytical query restrictions.

**Official Reference**: [BigQuery Data Clean Rooms Overview](https://cloud.google.com/bigquery/docs/data-clean-rooms)

---

### Question 11 (Domain 5: Orchestration Idempotence & Backfilling)
**Scenario**: You are maintaining an Apache Airflow DAG in Cloud Composer that calculates daily regional sales totals and inserts the results into a production BigQuery reporting table `daily_sales_by_region`. Due to an upstream network outage, the DAG failed to run for 5 consecutive days last week. You need to backfill the missing 5 days of data. When you trigger the backfill, you notice that each execution appends duplicate rows to the destination table, corrupting historical reporting metrics. How should you redesign the DAG to ensure idempotent execution?

- **A.** Add an Airflow task that executes `DELETE FROM daily_sales_by_region WHERE true;` before every task run.
- **B.** Structure the BigQuery task using `BigQueryInsertJobOperator` executing an atomic `MERGE` statement keyed on `(report_date, region_id)`, or write output using partition overwrite mode (`WRITE_TRUNCATE` targeting partition `$YYYYMMDD`).
- **C.** Disable catchup on the Airflow DAG by setting `catchup=False`.
- **D.** Increase the task retries to 10 with a 1-hour delay.

**Correct Answer**: **B**

#### Deep Architectural Explanation
In production data engineering, pipelines must be **idempotent**: executing a pipeline multiple times for the same logical execution date (`{{ ds }}`) must always produce the identical resulting state without duplicate rows.
1. **Atomic MERGE**: Using a `MERGE INTO ... USING ... ON target.date = source.date AND target.region = source.region WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...` ensures duplicate runs update existing records in place.
2. **Partition Overwrite (`WRITE_TRUNCATE`)**: If the table is partitioned by `report_date`, writing task output with `write_disposition='WRITE_TRUNCATE'` targeting the specific partition (`table$20260214`) atomically replaces only that day's data without affecting neighboring partitions or duplicating records.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Truncating the entire table (`DELETE FROM ... WHERE true`) wipes out all historical data across all time whenever a single task or backfill executes.
- **Option C is incorrect**: Setting `catchup=False` disables automated backfills altogether; it does not make the pipeline idempotent and fails to process the missing 5 days.
- **Option D is incorrect**: Increasing retries has nothing to do with idempotence and will simply retry failing tasks without preventing duplication.

**Official Reference**: [Idempotent Data Pipelines with BigQuery](https://cloud.google.com/blog/products/data-analytics/how-to-write-idempotent-data-pipelines-in-bigquery)

---

### Question 12 (Domain 3: Cloud Storage Consistency & Strong Ordering)
**Scenario**: A migration architect is moving a legacy Hadoop cluster running HDFS to Google Cloud using Cloud Dataproc and Google Cloud Storage (via the Cloud Storage GCS Connector). On the legacy HDFS cluster, downstream jobs relied heavily on immediate directory listing and file atomic renaming (`mv dir_a dir_b`) to coordinate job completion. The client expresses concern that eventual consistency in cloud object stores could cause downstream jobs to read partial files or miss completed partition directories. How should you address their architectural concern regarding Google Cloud Storage?

- **A.** Deploy an Apache ZooKeeper cluster on Compute Engine to track object state transitions.
- **B.** Explain that Google Cloud Storage natively provides **strong global consistency** for all read-after-write, read-after-update, read-after-delete, and bucket/object listing operations.
- **C.** Configure Cloud Storage Object Versioning and write a custom Java polling plugin to verify object checksums.
- **D.** Avoid Cloud Storage entirely and provision Filestore Enterprise clusters to mount NFS volumes across all Dataproc nodes.

**Correct Answer**: **B**

#### Deep Architectural Explanation
Unlike legacy object storage systems (such as older AWS S3 versions which historically provided eventual consistency for metadata and listings):
1. **Strong Global Consistency**: Google Cloud Storage has offered **100% strongly consistent operations globally** across all regions and storage classes.
2. Once a write, update, or delete operation receives an HTTP 200 OK response, any subsequent read or list operation immediately reflects that mutation anywhere in the world.
3. When using the GCS Connector with Apache Hadoop and Spark, jobs can read newly written files immediately without consistency delays, eliminating the need for complex metadata sync databases like S3Guard.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Deploying ZooKeeper to coordinate object storage metadata is unnecessary and introduces massive infrastructure complexity.
- **Option C is incorrect**: Object versioning preserves deleted or overwritten object history; it does not change consistency semantics.
- **Option D is incorrect**: Filestore Enterprise is an expensive managed NFS solution designed for POSIX file-locking applications. Using NFS as primary storage for big data analytics defeats the cost and scalability advantages of decoupled GCS object storage.

**Official Reference**: [Cloud Storage Consistency Model](https://cloud.google.com/storage/docs/consistency)

---

### Question 13 (Domain 4: Looker & Semantic Modeling with BI Engine)
**Scenario**: An executive analytics dashboard built on Looker experiences sluggish load times (7 to 12 seconds per tile) when querying a central BigQuery reporting dataset. The underlying BigQuery table contains 200 million rows, partitioned by day and clustered by department. The executive team demands dashboard refresh latencies under 1 second. Adding more BigQuery slots has not resolved the dashboard latency. What architectural solution should you implement?

- **A.** Provision a **BigQuery BI Engine** reservation in the dataset's region and ensure queries target the accelerated clustered tables.
- **B.** Export the data every 10 minutes to a MySQL instance and repoint Looker to MySQL.
- **C.** Rewrite all LookML explores to use raw SQL derived tables with `persist_for: "0 seconds"`.
- **D.** Instruct all executives to view cached PDF reports emailed once per week.

**Correct Answer**: **A**

#### Deep Architectural Explanation
**BigQuery BI Engine** is an in-memory analysis service deeply integrated into BigQuery:
1. **Sub-Second Latencies**: BI Engine caches frequently queried columnar data in high-speed vectorized memory, reducing query response times to sub-second or tens of milliseconds.
2. **Seamless Looker Integration**: Looker connects natively to BigQuery; when BI Engine is enabled, BigQuery's optimizer automatically routes queries to BI Engine memory without requiring any changes to LookML code or SQL queries.
3. **Smart Fallback**: If a query exceeds reserved BI Engine capacity or contains unsupported functions, it transparently falls back to standard BigQuery slots without throwing errors.

#### Why Each Incorrect Option Fails
- **Option B is incorrect**: Exporting 200 million rows to MySQL breaks the lakehouse architecture, limits scalability, creates replication delays, and reintroduces relational database bottlenecks.
- **Option C is incorrect**: Setting `persist_for: "0 seconds"` completely disables Looker caching, forcing every single user click to re-execute full queries against BigQuery, dramatically increasing query latency and cost.
- **Option D is incorrect**: Delivering static PDFs fails to satisfy the requirement for interactive real-time dashboard analytics.

**Official Reference**: [BigQuery BI Engine Overview](https://cloud.google.com/bigquery/docs/bi-engine-overview)

---

### Question 14 (Domain 5: Pipeline Observability & Alerting Strategy)
**Scenario**: You are the Lead Data Reliability Engineer managing 40 mission-critical streaming pipelines running on Cloud Dataflow. A critical business metric is **Data Freshness** (ensuring events from Pub/Sub reach BigQuery within 60 seconds). You need to configure automated alerts in Cloud Monitoring to notify the on-call engineer whenever a pipeline experiences processing delays or worker resource starvation. Which Cloud Monitoring metric should you alert on?

- **A.** `dataflow.googleapis.com/job/user_counter`
- **B.** `dataflow.googleapis.com/job/data_watermark_age` (or `system_lag`) exceeding 60 seconds.
- **C.** `compute.googleapis.com/instance/disk/write_bytes_count`
- **D.** `pubsub.googleapis.com/topic/send_request_count`

**Correct Answer**: **B**

#### Deep Architectural Explanation
In streaming systems, latency cannot be accurately measured by standard CPU utilization or disk I/O:
1. **Data Watermark Age (`data_watermark_age`)**: Measures the difference in seconds between the current real-world timestamp and the pipeline's event-time watermark. A high watermark age indicates that input events are lagging behind wall-clock time.
2. **System Lag (`system_lag`)**: Reflects the current maximum duration that an unprocessed message has spent sitting inside the pipeline pipeline buffers.
3. Setting an alert condition on `data_watermark_age > 60s` or `system_lag > 60s` reliably alerts engineers to processing backlogs, upstream ingestion stalls, or worker starvation before SLAs are breached.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: User counters are custom arbitrary metrics defined inside Beam code; they do not natively track streaming system latency or watermark progression.
- **Option C is incorrect**: Disk write bytes only reflect local VM I/O volume and provide no insight into whether streaming events are meeting latency SLAs.
- **Option D is incorrect**: Pub/Sub send request count measures publishing volume to the topic, not processing latency inside the downstream Dataflow job.

**Official Reference**: [Monitoring Dataflow Pipelines & Metrics](https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring)

---

### Question 15 (Domain 4: Dataflow ML RunInference Architecture)
**Scenario**: You are architecting a real-time event streaming pipeline using Apache Beam on Dataflow. The pipeline consumes user transaction events from Pub/Sub and must evaluate each transaction against an existing TensorFlow fraud detection model. The model requires 3 GB of memory. Traffic reaches 50,000 transactions per second. An engineer suggests calling a Vertex AI Online Prediction HTTP endpoint from inside a Beam `ParDo` transform for every transaction. Why will this suggestion fail under production load, and what is the recommended architecture?

- **A.** Vertex AI cannot host TensorFlow models; you should rewrite the model in SQL and use BQML.
- **B.** Calling an external HTTP prediction endpoint per record creates severe network RPC latency, exhausts client connections, and risks HTTP 429 quota throttling; instead, use the Apache Beam `RunInference` transform to load and execute model inference locally within worker memory.
- **C.** Dataflow cannot run Python machine learning libraries; you must migrate the pipeline to Apache Flink on Compute Engine.
- **D.** The Beam `ParDo` transform only accepts scalar integers; use `GroupByKey` before calling the endpoint.

**Correct Answer**: **B**

#### Deep Architectural Explanation
1. **The Anti-Pattern (External HTTP Calls in Streaming Hot Paths)**: Making an HTTP REST call for each record at 50,000 requests/sec introduces huge network latency (10-50ms per call), saturates socket pools, overwhelms the Vertex AI endpoint quota, and causes pipeline watermarks to stall.
2. **The Production Pattern (`RunInference`)**:
   - The Apache Beam `RunInference` transform optimizes machine learning workflows by loading the model directly into worker memory once per worker process (or sharing model weights across threads).
   - It performs in-memory batched tensor evaluation locally on the worker VM (optionally leveraging attached GPUs/TPUs), achieving tens of thousands of predictions per second with sub-millisecond per-record inference overhead and zero network RPCs.

#### Why Each Incorrect Option Fails
- **Option A is incorrect**: Vertex AI natively supports TensorFlow SavedModel artifacts; however, remote endpoint invocation is an anti-pattern for 50k req/sec streaming pipelines.
- **Option C is incorrect**: Apache Beam and Dataflow offer first-class native support for Python ML frameworks (PyTorch, TensorFlow, Scikit-learn, ONNX) via `RunInference`.
- **Option D is incorrect**: `ParDo` accepts arbitrary Python data structures, objects, dictionaries, and tensors.

**Official Reference**: [Apache Beam RunInference Transform Guide](https://beam.apache.org/documentation/ml/runinference-overview/)
