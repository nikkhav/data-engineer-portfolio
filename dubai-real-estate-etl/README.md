# Dubai Real Estate Data Warehouse

## Overview

This project is an end-to-end Data Warehouse for Dubai real estate transactions, using a modern data platform stack with Airflow, dbt, PostgreSQL, and Metabase.

The pipeline automatically ingests raw data from the Dubai Land Department API, cleans and transforms it using Data Vault 2.0 patterns, and exposes analytics-ready dimensional models for BI dashboards and ML experiments.

---

## Architecture

### Technologies
- **PostgreSQL** — main DWH with 4 layers:
  - **stg** — raw data (JSONB)
  - **dds** — Data Vault (hubs, links, satellites)
  - **cdm** — Star Schema (fact + dimensions)
  - **bi** — curated business views

- **Airflow** — orchestrates ingestion and runs dbt
- **dbt** — models transformations (STG → DDS → CDM → BI)
- **Metabase** — analytics and dashboards
- **Docker Compose** — infrastructure orchestration

---

## Data Flow

### 1. **Ingestion (STG – Airflow)**
- Fetch CSV from Dubai Land Department open API  
- Store raw records in `stg.raw_deals`
- Add metadata: `load_ts`, `ingestion_id`, `load_source`

### 2. **Transformation (dbt)**

#### **STG Layer**
- Flatten JSONB
- Type casting
- Light cleanup  
All STG models = **views**

#### **DDS Layer (Data Vault)**
- `hub_transaction`
- `hub_property`
- `link_transaction_property`
- `sat_transaction_details`
- `sat_property_details`

Rules used:
- surrogate keys → `dbt_utils.generate_surrogate_key`
- hashdiff → change detection
- incremental models with unique_key

#### **CDM Layer (Star Schema)**
- `fact_deals`
- `dim_property`
- `dim_project`
- `dim_date`

All CDM models = **tables**

#### **BI Layer**
Curated analytics models:
- `project_overview`
- `area_property_price`

---

## Getting Started

### 1. Build Airflow image
```bash
docker compose build airflow
```

### 2. Start full environment
```bash
docker compose up -d
```

### 3. Access Web UIs
| Service | URL |
|--------|-----|
| Airflow | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| Postgres | localhost:15432 |

You can use any username/password for Airflow during initial setup.

### 4. Run dbt transformations manually (optional)
```bash
docker exec -it airflow bash -c "cd /opt/airflow/dbt && dbt build --target dev"
```
