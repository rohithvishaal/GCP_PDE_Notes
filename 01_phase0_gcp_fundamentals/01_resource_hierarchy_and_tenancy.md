# Resource Hierarchy, Tenancy & Regional Architecture

## 1. Architectural Overview & The GCP Tenancy Model

Google Cloud structures all infrastructure, compute, and data assets within a strict, multi-tiered directed acyclic tree known as the **Resource Hierarchy**. Unlike platforms where compute clusters or metastores sit at the tenancy root, Google Cloud anchors identity, billing, network boundaries, and administrative governance to this hierarchy.

For a Principal Data Architect or an engineer transitioning from Databricks and AWS environments, understanding this hierarchy is critical. Misunderstanding the boundary of a **Project** or how inheritance flows from an **Organization** or **Folder** will lead to broken security boundaries, quota exhaustion across pipelines, or uncontained blast radiuses during pipeline failures.

```mermaid
graph TD
    Org["Organization: company.com (Root Node)"]
    
    FolderData["Folder: Enterprise Data Platform"]
    FolderEng["Folder: Core Engineering"]
    
    FolderProd["Folder: Production Data"]
    FolderNonProd["Folder: Non-Production Data"]
    
    ProjIngest["Project: prj-prod-ingest-01<br/>(Pub/Sub, Datastream, Cloud Storage)"]
    ProjCompute["Project: prj-prod-compute-01<br/>(Dataflow, Dataproc Serverless)"]
    ProjAnalytics["Project: prj-prod-analytics-01<br/>(BigQuery, Dataplex, Analytics Hub)"]
    
    DatasetBQ["BigQuery Dataset: raw_telemetry"]
    GCSBucket["Cloud Storage Bucket: landing-zone-us-central1"]
    PubSubTopic["Pub/Sub Topic: iot-events"]
    
    Org --> FolderData
    Org --> FolderEng
    FolderData --> FolderProd
    FolderData --> FolderNonProd
    FolderProd --> ProjIngest
    FolderProd --> ProjCompute
    FolderProd --> ProjAnalytics
    ProjIngest --> GCSBucket
    ProjIngest --> PubSubTopic
    ProjAnalytics --> DatasetBQ
```

---

## 2. Core Hierarchy Entities

### 2.1 The Organization Node
- **Root of the Hierarchy**: Tied 1:1 to a Google Workspace or Cloud Identity domain (e.g., `acme-corp.com`).
- **Control Plane Anchor**: Central point for Organization Policies, top-level IAM role grants, and default Cloud Logging sinks.
- **Superadmin Persona**: The `roles/resourcemanager.organizationAdmin` manages lifecycle and delegation, but should never manage data pipelines directly.

### 2.2 Folders
- **Logical Grouping**: Organizes projects by line of business (LOB), environment (`prod`, `staging`, `dev`), or compliance boundary (`pci-dss`, `hipaa`).
- **Nesting**: Can be nested up to **10 levels deep**.
- **Policy Aggregation**: Applying an IAM role or Org Policy at the folder level automatically propagates downward to all child folders and projects through policy inheritance.

### 2.3 The Project: The Sovereign Boundary
In Google Cloud, the **Project** is the foundational atom of tenancy. It is not merely a tag or a namespace; it establishes the explicit boundary for:
1. **IAM Boundary**: IAM bindings assigned to users, groups, or Service Accounts apply across all resources within the project unless constrained at lower resource levels.
2. **Billing Boundary**: Every project is linked to exactly one Cloud Billing Account. All SKU usage (CPU, RAM, storage, network egress, queries) inside that project rolls up to that billing account.
3. **Quota Boundary**: API rate limits, regional compute quota (e.g., Dataproc vCPUs), BigQuery reservation allocations, and IP address allocations are metered per-project.
4. **Network Boundary**: A Virtual Private Cloud (VPC) network belongs to a project. While Shared VPC allows cross-project network sharing, the network lifecycle lives inside a host project.
5. **API Activation**: Google Cloud APIs (e.g., `bigquery.googleapis.com`, `dataflow.googleapis.com`) are enabled or disabled per project.

> [!IMPORTANT]
> **Enterprise Data Platform Pattern**: Never deploy end-to-end data pipelines (ingestion + compute + warehousing + egress) in a single monolithic project. Separate ingestion, processing, storage/warehouse, and orchestration into distinct projects to prevent compute jobs from starving analytics quotas or exposing raw data through permissive project-level IAM.

### 2.4 Resources
The leaf nodes of the hierarchy:
- BigQuery datasets, tables, routines, and models.
- Cloud Storage buckets and objects.
- Cloud Pub/Sub topics, subscriptions, and schemas.
- Dataproc clusters and batch workloads.
- Dataflow streaming and batch jobs.

---

## 3. Structural Comparison: Google Cloud vs. Databricks

Engineers coming from Databricks often conflate Unity Catalog metastores or workspace clusters with GCP projects. The following mapping clarifies architectural boundaries:

| Dimension | Databricks (AWS/Azure) | Google Cloud Platform | Architectural Implications |
| :--- | :--- | :--- | :--- |
| **Tenancy Root** | Databricks Account Console | Cloud Identity / GCP Organization | GCP governs IAM, billing, and networking natively above the project level. |
| **Namespace Boundary** | Workspace | GCP Project | Databricks workspaces share metastores via Unity Catalog; GCP projects own their own APIs, quotas, and service boundaries. |
| **Catalog / Governance** | Unity Catalog Metastore | Dataplex / Dataplex Catalog | Metastore is a single catalog entity; Dataplex provides a logical mesh across distributed projects and buckets. |
| **Relational Hierarchy** | `catalog.schema.table` | `project_id.dataset_id.table_id` | BigQuery tables natively require 3-part qualification (`project.dataset.table`). |
| **Compute Tenancy** | Spark Cluster (Driver + Workers) on EC2/Compute Engine | Dataflow (Managed Beam) / Dataproc (Spark/Hadoop) / BigQuery Slots | Databricks runs VMs in customer VPCs with Databricks control plane; Dataflow and BigQuery execute queries in Google-managed multi-tenant compute planes. |
| **Storage Separation** | External Volumes / S3 Buckets / ADLS Gen2 | Cloud Storage (GCS) Buckets / BigQuery Managed Storage | Both decouple compute from storage, but BigQuery storage can be queried directly without starting or managing clusters. |

---

## 4. Physical Geography: Regions, Zones, Dual-Regions, and Multi-Regions

Data residency, pipeline latency, and disaster recovery (DR) depend on selecting the correct physical deployment target.

```mermaid
graph LR
    subgraph MultiRegion["Multi-Region: 'US'"]
        subgraph Region1["Region: us-central1 (Iowa)"]
            Zone1A["Zone: us-central1-a"]
            Zone1B["Zone: us-central1-b"]
            Zone1C["Zone: us-central1-c"]
        end
        subgraph Region2["Region: us-east4 (N. Virginia)"]
            Zone2A["Zone: us-east4-a"]
            Zone2B["Zone: us-east4-b"]
        end
    end
```

### 4.1 Zone
- A single failure domain within a region, typically backed by one or more physical data centers.
- Connected to other zones in the same region via high-bandwidth, sub-millisecond latency networks.
- Used for: GCE instances, Dataproc worker nodes.

### 4.2 Region
- A distinct geographic location with **three or more zones**.
- Examples: `us-central1` (Iowa), `europe-west1` (Belgium), `asia-east1` (Taiwan).
- Inter-zone latency inside a region is < 1ms. Cross-region latency varies from 10ms to 200ms+.
- Storage in a regional bucket or single-region BigQuery dataset provides 99.99% availability and guarantees strict data sovereignty (data never leaves the country/region boundary).

### 4.3 Dual-Region
- Specific to Cloud Storage: Spans two specific regions (e.g., `nam4` for `us-central1` and `us-east4`).
- Provides active-active replication with an RPO (Recovery Point Objective) of 0 for object data via Turbo Replication (15-minute SLA for 100% of replicated data).
- Ideal for critical staging lakes feeding analytics pipelines that require cross-region disaster recovery without paying for full multi-region spread.

### 4.4 Multi-Region
- Large geographic areas containing two or more independent regions (e.g., `US`, `EU`, `ASIA`).
- **BigQuery Multi-Region (`US`)**: Queries run across dynamically allocated slots in data centers throughout the US. Datasets stored in `US` cannot be joined with datasets in single regions (e.g., `us-central1`) without cross-region data transfer or data copy.
- Cloud Storage Multi-Region provides 99.95% availability SLA and geo-redundancy against regional catastrophic events.

### 4.5 The Underlying Fabric: Jupiter Network
Google Cloud compute and storage do not travel over commodity internet backbones. They traverse **Jupiter**, Google's custom software-defined datacenter network fabric:
- **Bisection Bandwidth**: Over 1 Petabit/sec of total bisection bandwidth.
- **Compute-Storage Decoupling**: BigQuery worker nodes (Borg containers) read from BigQuery storage (Colossus) across Jupiter with bandwidth comparable to local NVMe disks, eliminating the traditional Hadoop co-location requirement.

---

## 5. Resource Manager & Organizational Policies

While IAM governs *who* (identities) can do *what* on *which* resources, **Organization Policies** define hard configuration guardrails across the entire resource hierarchy regardless of IAM permissions.

### 5.1 Inheritance Rules
1. **Hierarchical Evaluation**: Policies evaluate top-down (Org -> Folders -> Project).
2. **Deny Overrides Allow**: If a constraint is enforced at the organization level, a project administrator cannot disable it unless explicitly permitted by inheritance rules.
3. **Inheritance Modes**:
   - `inheritFromParent`: Merges values from parent nodes.
   - `reset`: Clears inherited values and restarts policy at this node.

### 5.2 Critical Data Engineering Org Constraints

The PDE exam frequently tests organizational constraints used to secure data platforms and prevent accidental data exfiltration or misconfiguration:

| Constraint Name | Type | Data Engineering Impact & Purpose |
| :--- | :--- | :--- |
| `constraints/compute.vmExternalIpAccess` | List (Deny All) | Prevents Dataproc and Compute Engine worker nodes from getting public IPs. Forces traffic through Cloud NAT or Private Google Access. |
| `constraints/storage.uniformBucketLevelAccess` | Boolean (Enforced) | Disables legacy Object ACLs across all Cloud Storage buckets; enforces uniform IAM control at the bucket level. |
| `constraints/storage.publicAccessPrevention` | Boolean (Enforced) | Blocks public read access (`allUsers`, `allAuthenticatedUsers`) on GCS data lake buckets, preventing data exposure. |
| `constraints/gcp.resourceLocations` | List (Allowed) | Restricts resource creation (BigQuery datasets, GCS buckets) to compliant regions (e.g., `in:eu-locations` for GDPR). |
| `constraints/iam.disableServiceAccountKeyCreation` | Boolean (Enforced) | Blocks users from downloading downloadable JSON private keys for Service Accounts, forcing Workload Identity Federation and SA impersonation. |

### 5.3 Concrete Implementation: Declaring Organizational Policies

Setting an organization policy using `gcloud`:

```bash
# Enforce Uniform Bucket-Level Access across all projects in a folder
gcloud resource-manager org-policies enable-enforce \
    storage.uniformBucketLevelAccess \
    --folder=1049283746501

# Restrict BigQuery and GCS creation to us-central1 and us-east4 only
cat <<EOF > location_constraint.yaml
constraint: constraints/gcp.resourceLocations
listPolicy:
  allowedValues:
    - in:us-central1-locations
    - in:us-east4-locations
EOF

gcloud resource-manager org-policies set-policy \
    location_constraint.yaml \
    --organization=987654321098
```

---

## 6. Enterprise Multi-Project Topologies for Data Platforms

A production-grade Google Cloud data architecture isolates concerns across functional projects:

```mermaid
graph TD
    subgraph HostProject["Shared VPC Host Project (prj-net-core)"]
        VPCNetwork["VPC: vpc-data-prod<br/>10.100.0.0/16"]
        SubnetData["Subnet: sb-dataproc-prod<br/>10.100.10.0/24"]
        CloudNAT["Cloud NAT Gateway"]
    end

    subgraph IngestProject["Ingestion Project (prj-data-ingest)"]
        DatastreamJob["Datastream (CDC)"]
        PubSub["Pub/Sub Topics"]
    end

    subgraph ProcessProject["Processing Project (prj-data-compute)"]
        DataprocCluster["Dataproc Ephemeral Clusters"]
        DataflowWorker["Dataflow Streaming Pipeline"]
    end

    subgraph LakeProject["Storage & Warehouse Project (prj-data-warehouse)"]
        RawGCS["GCS Lakehouse (Delta/Iceberg/Parquet)"]
        BigQueryDW["BigQuery Core DWH (Part/Clust)"]
    end

    subgraph ConsumptionProject["Consumption / BI Project (prj-data-bi)"]
        LookerBI["Looker / Metabase / Jupyter"]
        AdHocSlots["BigQuery Reservation Assignment"]
    end

    SubnetData -.-> DataprocCluster
    SubnetData -.-> DataflowWorker
    DatastreamJob --> RawGCS
    PubSub --> DataflowWorker
    DataflowWorker --> BigQueryDW
    DataprocCluster --> RawGCS
    DataprocCluster --> BigQueryDW
    ConsumptionProject -->|Queries via BigQuery Job User| BigQueryDW
```

### Key Isolation Principles:
1. **prj-data-ingest**: Contains external ingress endpoints, Datastream CDC instances, Pub/Sub topics. No analytical queries run here.
2. **prj-data-compute**: Runs Dataflow and Dataproc. Uses **Service Project** attachment to the Shared VPC. Dataproc workers pull code and write processed data to the warehouse project.
3. **prj-data-warehouse**: Hosts raw GCS buckets and BigQuery datasets. Compute permissions are strictly segregated from storage permissions.
4. **prj-data-bi**: Analysts submit queries from this project. They pay for their own query execution slots/costs while reading shared tables from `prj-data-warehouse`.

---

## 7. Exam Relevance & Anti-Patterns (PDE Exam Guide)

### Anti-Patterns to Spot in Exam Scenarios:
- **Anti-Pattern 1**: Running Dataproc or Dataflow in the same project where raw PII datasets reside, while giving data engineers `Editor` or `Owner` roles on that project.
  - *Correction*: Isolate compute into a compute project where engineers have job execution privileges; grant granular `roles/bigquery.dataViewer` or Dataplex reader roles on specific datasets in the warehouse project.
- **Anti-Pattern 2**: Creating BigQuery datasets in `US` multi-region while raw staging GCS buckets are in `us-central1`, and expecting serverless BigQuery external tables to query GCS without network egress charges or cross-region query errors.
  - *Correction*: External tables and underlying GCS buckets **must reside in the exact same region or multi-region**.
- **Anti-Pattern 3**: Attempting to resolve cross-project quota starvation by creating new datasets inside the same project.
  - *Correction*: Quotas (slots, API rate limits) are bounded by the project. Workloads must be isolated into dedicated projects or bound to distinct BigQuery Reservations.
