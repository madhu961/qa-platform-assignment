# Quality / Platform Engineer Take Home Assignment

## Overview

Built an end-to-end PySpark data quality and platform pipeline on AWS using:

- Amazon S3 for raw + curated storage
- EC2 for Spark execution
- AWS Glue Catalog as metastore
- Apache Iceberg for ACID data lake tables
- PySpark for transformation + entity resolution
- Python for orchestration / quality checks
- MLflow for model tracking

The system ingests two noisy corporate datasets, performs entity resolution and harmonization, stores the unified registry in Iceberg, and trains a simple ML model directly from the curated table.

---

## Architecture

Raw CSV Sources (S3)
↓
PySpark Ingestion
↓
Quality Checks / Circuit Breakers
↓
Entity Resolution
  - exact match
  - fuzzy match
  - unmatched preservation
↓
Canonical Harmonization
↓
Iceberg MERGE UPSERT
↓
Unified Corporate Registry
↓
Feature Engineering
↓
ML Training

---

## Technologies Used

- Python 3.12
- PySpark 3.5.1
- Apache Iceberg 1.5.2
- AWS S3
- AWS Glue Catalog
- EC2 Ubuntu
- MLflow
- Pytest

---

## Assignment Requirements Mapping

### 1. Data Ingestion
Reads two CSV files from S3.

### 2. Data Quality
Implemented:
- row count validation
- null key checks
- negative revenue checks
- contract validation

### 3. Entity Resolution
Two-pass approach:

1. Deterministic exact normalized-name match  
2. Fuzzy blocked matching using RapidFuzz

### 4. Harmonization
Unified schema with:
- canonical company name
- suppliers/customers
- revenue/profit
- source lineage flags

### 5. Storage
Apache Iceberg table using Glue Catalog with transactional MERGE INTO upserts.

### 6. Reconciliation
Reports:
- input rows
- final rows
- duplicates resolved
- source-only rows
- match rate

### 7. ML
Trains binary classifier using curated Iceberg outputs.

---

## Sample Run Metrics

Input Rows:
- Source1: 1200
- Source2: 1200

Output Registry:
- Final Entities: 1611
- Duplicates Consolidated: 789
- Match Rate: ~75%

Match Types:
- exact_name
- fuzzy_name
- source1_only
- source2_only

---

## Query Iceberg Table

From Spark:
  spark.read.format("iceberg").load(
    "glue_catalog.qa_assignment.corporate_registry"
  ).show()

---

## Key Design Decisions

### 1. Why Iceberg

Selected Apache Iceberg because it provides:

- ACID transactions
- schema evolution
- MERGE support
- time travel
- scalable metadata management

Better fit than plain parquet for operational registry workloads.

### 2. Why Staging Table Before MERGE

Instead of merging directly from a Spark temp view, we used:

  DataFrame → Iceberg Staging Table → MERGE

Iceberg requires deterministic execution plans. Spark temp views can retain non-deterministic lineage which may fail with non-deterministic expressions during merge.

### 3. Why Two-Pass Entity Resolution

Pass 1: Exact Matching

- Normalize names
- Match on cleaned names
- High precision
- Misses noisy matches

Pass 2: Fuzzy Matching

- Block by prefix (first 6 chars)
- Use string similarity (RapidFuzz)
- Improves recall
- Handles formatting variation

Used Combined Strategy ensuring high precision from exact matching while recovering missed links via fuzzy matching, giving a balanced precision–recall tradeoff suitable for production-scale entity resolution.:

### 4. Why Blocking in Fuzzy Matching

Instead of full cross join (1200 x 1200 = 1.44M comparisons), we used:

  Block on prefix to reduce comparisons drastically

This is scalable for larger datasets.

### 5. Entity Resolution Logic

- Step 1: Preprocessing
  Normalize:
  company names → clean_name (remove suffixes: Inc, Ltd, Corp) 

- Step 2: Exact Matching
  clean_name_s1 == clean_name_s2

- Step 3: Fuzzy Matching
  blocked candidates → compute similarity → threshold filter

- Step 4: Classification
  Each record is categorized:
  Type             Meaning
  exact_name       perfect match
  fuzzy_name       approximate match
  source1_only     no match
  source2_only     no match

### 6.Harmonization

We generate a canonical schema:

- corporate_id
- canonical_name
- canonical_address
- activity_places
- top_suppliers
- main_customers
- revenue
- profit
- match_confidence
- source1_present
- source2_present
- last_updated_ts
- batch_id
- match_type

### 7. Iceberg Merge Logic

- Flow:
  Harmonized DF → Staging Table → MERGE INTO Final Table

- Merge condition:
  ON t.corporate_id = s.corporate_id

- Actions:
  update existing rows
  insert new rows

### 8. Data Quality Checks

Implemented checks:

Source 1
- row count threshold
- null company name validation

Source 2
- negative revenue detection
- schema validation

Failures raise exceptions → pipeline stops.

### 9. Observability

Structured logs emitted for:
  {"step": "entity_resolution", "rows": 1611}
  {"step": "reconciliation", "match_rate": 75.6}
  {"step": "model_metrics", "auc": 0.84}

### 10. ML Pipeline

Features used:
- revenue
- number of suppliers
- number of customers
- number of activity locations

Target: profit > threshold

Model: Logistic Regression

Output:

- AUC score
- model logged in MLflow

---

## CI/CD Pipeline

GitHub Actions can run:
  pytest tests

Future extension:
- automated pipeline deployment
- infra provisioning via Terraform

---

## Production Improvements

If extended further:

- Airflow orchestration
- Great Expectations contracts
- Grafana dashboards
- dbt marts
- EventBridge scheduling
- Model drift monitoring
- Terraform IaC

---

## How to Run

### 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt```


### 2. Pipeline

```bash
source venv/bin/activate
python -m src.main_pipeline```

### 3. Tests

```bash
source venv/bin/activate
pytest tests -q```

---

## AWS Setup

### 1. S3

- Create bucket: 
  qa-platform-assignment

- Folders:
  input/source1/
  input/source2/
  iceberg/warehouse/


### 2. EC2

- Ubuntu instance
- attach IAM role:
  S3 access
  Glue access

### 3. Glue

- Create database:
  qa_assignment

---
