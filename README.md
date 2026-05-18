<p align="center">
  <h1 align="center">BellosData Lakehouse — S3 Delta Lake Platform</h1>
  <p align="center">
    <strong>Serverless data lakehouse on AWS — public APIs ingested by Airflow, stored as Delta Lake on S3, modelled via YAML-driven dimensional builders, catalogued by Unity Catalog, and queried with DuckDB.</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow">
    <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=databricks&logoColor=white" alt="Delta Lake">
    <img src="https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3">
    <img src="https://img.shields.io/badge/Unity_Catalog-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Unity Catalog">
    <img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB">
    <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white" alt="Terraform">
    <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/AWS_Lightsail-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="Lightsail">
  </p>
</p>

---

## Architecture Overview

A **production-grade data lakehouse** built entirely on AWS serverless/low-cost services. The platform ingests data from **6 public APIs**, stores it as **Delta Lake** tables on S3, transforms it through **YAML-driven dimensional builders**, and serves it via **Unity Catalog** and **DuckDB**.

```mermaid
flowchart LR
    subgraph SOURCES["🌐 Public APIs"]
        A1["Open-Meteo\nWeather"]
        A2["Open-Meteo\nWind"]
        A3["eBird / NW Birding\nBird Sightings"]
        A4["OurAirports\nAviation Data"]
        A5["postcodes.io\nUK Postcodes"]
        A6["Companies House\nUK Companies"]
    end

    subgraph PLATFORM["🖥️ Lightsail (Docker Compose)"]
        AF["Apache Airflow 3.2.1\n18 DAGs\nCeleryExecutor"]
        UC["Unity Catalog\nDelta Lake Metadata"]
        DDB["DuckDB\nSQL Console"]
    end

    subgraph BRONZE["📦 S3 Bronze"]
        B["playdarch-bronze-raw\nRDL (Delta Lake)\nPartitioned by ingest_date"]
    end

    subgraph SILVER["🥈 S3 Silver"]
        S["playdarch-silver-curated\nODL (Delta Lake)\nDimensions · Facts · Maps"]
    end

    A1 & A2 & A3 & A4 & A5 & A6 -->|"HTTP GET"| AF
    AF -->|"RDL Write\n(Delta append)"| B
    AF -->|"ODL Builders\n(YAML-driven)"| S
    B -->|"Source data"| S
    S --> UC
    S --> DDB

    style SOURCES fill:#4285F4,color:#fff
    style PLATFORM fill:#FF9900,color:#fff
    style BRONZE fill:#CD7F32,color:#fff
    style SILVER fill:#C0C0C0,color:#000
```

### How It Works

1. **18 Airflow DAGs** run on a schedule — each ingests data from a public API via HTTP
2. **RDL ingestion DAGs** write raw JSON records to **S3 Bronze** as Delta Lake tables, partitioned by `ingest_date`
3. **ODL builder DAGs** read YAML configuration files (`dimensions.yml`, `facts.yml`, `mappings.yml`) and construct typed dimensional tables on **S3 Silver**
4. **Unity Catalog** provides metadata governance across all Delta tables
5. **DuckDB** web SQL console enables ad-hoc querying of both Bronze and Silver layers

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------| 
| **Infrastructure** | Terraform | Lightsail, ECR, ECS, Lambda, API Gateway — all as code |
| **Compute** | AWS Lightsail | Always-on `medium_3_0` (4 GB RAM, 2 vCPU, $24/mo) |
| **Orchestration** | Apache Airflow 3.2.1 | CeleryExecutor with Redis + PostgreSQL, 18 DAGs |
| **Containerisation** | Docker Compose | Airflow cluster + Unity Catalog + DuckDB service |
| **Storage** | Amazon S3 | Two-tier Delta Lake: Bronze (`playdarch-bronze-raw`) + Silver (`playdarch-silver-curated`) |
| **Table Format** | Delta Lake | ACID transactions, time travel, schema enforcement |
| **Catalog** | Unity Catalog (OSS) | Data governance, table metadata, access control |
| **Query Engine** | DuckDB | In-process SQL analytics + web console |
| **Language** | Python 3.11 | All DAGs, builders, and utilities |

---

## Project Structure

```
bellosdata-lakehouse/
│
├── airflow/
│   ├── dags/                              # 18 orchestration DAGs
│   │   ├── bellosdata_common.py           # Shared: rdl_write, odl_write, http_get, manifests
│   │   ├── aws_session.py                 # AWS credential management
│   │   │
│   │   ├── rdl_weather_ingestion.py       # Open-Meteo → NW England hourly weather
│   │   ├── rdl_wind_ingestion.py          # Open-Meteo → Multi-height wind data
│   │   ├── nw_bird_ingestion.py           # eBird + NW Birding → Bird sightings
│   │   ├── rdl_airports_ingestion.py      # OurAirports → Global airport data
│   │   ├── rdl_postcodes_ingestion.py     # postcodes.io → UK postcode lookup
│   │   ├── rdl_companies_enhanced.py      # Companies House → NW companies
│   │   ├── rdl_landscapes_ingestion.py    # UK landscape/habitat data
│   │   ├── private_jet_ingestion.py       # OpenSky → NW private jet movements
│   │   │
│   │   ├── odl_dim_builder.py             # YAML-driven dimension builder
│   │   ├── odl_fact_builder.py            # YAML-driven fact table builder
│   │   ├── odl_mapping_builder.py         # YAML-driven mapping table builder
│   │   │
│   │   ├── companies_house_upload_dag.py  # Companies House S3 upload
│   │   ├── drive_sync_dag.py              # Google Drive sync
│   │   ├── budget_monitor_dag.py          # AWS cost monitoring
│   │   ├── invoice_engine.py              # Invoice generation
│   │   └── receipt_archive_dag.py         # Receipt archival to S3
│   │
│   ├── config/                            # YAML-driven data model registry
│   │   ├── dimensions.yml                 # 7 dimensions (flat + hierarchy)
│   │   ├── facts.yml                      # 4 fact tables with measures
│   │   ├── mappings.yml                   # 6 mapping/bridge tables
│   │   ├── copy_jobs.yml                  # S3 copy job definitions
│   │   └── export_jobs.yml                # Data export configs
│   │
│   ├── plugins/                           # Airflow custom plugins
│   ├── docker-compose.yaml                # Full Airflow cluster (CeleryExecutor)
│   ├── docker-compose.cloud.yaml          # Cloud-optimised compose
│   ├── duckdb_service.py                  # DuckDB web SQL console service
│   └── duckdb_console.py                  # DuckDB CLI helper
│
├── terraform/
│   └── main.tf                            # Lightsail + ECR + ECS + Lambda API + API Gateway
│
├── unity-catalog/
│   ├── compose.yaml                       # Unity Catalog local compose
│   ├── compose.cloud.yaml                 # Unity Catalog cloud compose
│   └── etc/conf/                          # Unity Catalog server config
│
├── lambda/
│   └── ledger_api.py                      # Serverless cloud API (always-on Lambda)
│
├── .github/
│   └── workflows/
│       └── pr_title_checker.yml           # Enforces DATA-X branch naming
│
├── deploy-cloud.sh                        # Cloud deployment script (SCP + SSH)
├── deploy-platform.ps1                    # Windows deployment automation
├── start-platform.ps1                     # Local platform startup
│
├── .gitignore
└── README.md
```

---

## Data Domains

The lakehouse ingests data across **6 domains**, all from free public APIs:

| Domain | Source API | Schedule | Key Data |
|--------|-----------|----------|----------|
| **Weather** | Open-Meteo | Daily 08:00 UTC | Hourly observations for 8 NW England grid points — temp, precipitation, wind, UV |
| **Wind** | Open-Meteo | Daily 08:30 UTC | Multi-height wind (10m/80m/120m) for energy & aviation analysis |
| **NW Birds** | eBird + NW Birding | Daily 09:00 UTC | Bird sightings, species taxonomy, conservation status |
| **Airports** | OurAirports CSV | Weekly | Global airport database — runways, coordinates, ICAO/IATA codes |
| **Postcodes** | postcodes.io | Weekly | Full UK postcode register — geography, IMD, LSOA, constituency |
| **Companies** | Companies House API | Daily | NW England company register — filings, SIC codes, insolvency |
| **Private Jets** | OpenSky Network | Hourly | ADS-B aircraft positions over NW England airspace |
| **Landscapes** | Multiple open sources | Monthly | UK habitat & landscape classification — SSSIs, NNRs, AONBs |

---

## Medallion Architecture

The lakehouse follows a **Bronze → Silver** medallion pattern with YAML-driven transformation:

```mermaid
flowchart TB
    subgraph BRONZE["Bronze — S3 Raw Data Layer (RDL)"]
        direction LR
        R1["rdl/weather"]
        R2["rdl/wind"]
        R3["rdl/nw-birds"]
        R4["rdl/airports"]
        R5["rdl/postcodes"]
        R6["rdl/companies"]
        R7["rdl/private-jets"]
        R8["rdl/landscapes"]
    end

    subgraph SILVER["Silver — S3 Curated Layer (ODL)"]
        direction LR
        subgraph DIMS["Dimensions (7)"]
            D1["dim_date"]
            D2["dim_weather_station"]
            D3["dim_species"]
            D4["dim_company"]
            D5["dim_location ⊞"]
            D6["dim_airport ⊞"]
            D7["dim_habitat ⊞"]
        end
        subgraph FACTS["Facts (4)"]
            F1["fact_weather_observation"]
            F2["fact_wind_measurement"]
            F3["fact_flight_movement"]
            F4["fact_company_filing"]
        end
        subgraph MAPS["Mappings (6)"]
            M1["map_postcode_to_location"]
            M2["map_company_to_postcode"]
            M3["map_airport_to_location"]
            M4["map_species_to_habitat"]
            M5["map_owner_to_company"]
            M6["map_station_to_location"]
        end
    end

    BRONZE -->|"YAML-driven\nODL Builders"| SILVER

    style BRONZE fill:#CD7F32,color:#fff
    style SILVER fill:#C0C0C0,color:#000
```

> **⊞** = Hierarchy dimension (multi-level parent-child: e.g. `dim_location` = Country → Region → County → District → Ward → Postcode)

| Layer | S3 Bucket | Format | Purpose |
|-------|-----------|--------|---------|
| **Bronze (RDL)** | `playdarch-bronze-raw` | Delta Lake (append) | Raw JSON records, partitioned by `ingest_date`. Source of truth. |
| **Silver (ODL)** | `playdarch-silver-curated` | Delta Lake (overwrite) | Typed star schema — dimensions, facts, mappings. Surrogate keys. |

---

## YAML-Driven Data Modelling

Instead of hand-coding SQL transformations, the lakehouse uses **3 YAML registries** that are read by Python builder DAGs:

| Registry | File | Drives | Contents |
|----------|------|--------|----------|
| **Dimensions** | `config/dimensions.yml` | `odl_dim_builder.py` | 7 dimensions — flat (SCD1/2) and hierarchy (multi-level parent-child) |
| **Facts** | `config/facts.yml` | `odl_fact_builder.py` | 4 fact tables — grain, dimension keys, measures with aggregation types |
| **Mappings** | `config/mappings.yml` | `odl_mapping_builder.py` | 6 bridge tables — one-to-one, many-to-one, many-to-many with fuzzy matching |

### Example: Adding a new dimension

```yaml
# dimensions.yml — just add a new entry:
- name: dim_my_new_entity
  type: flat
  source: rdl/my_source
  scd_type: 2
  grain: "One row per entity"
  business_key: [entity_id]
  attributes:
    - name: entity_sk
      dtype: int32
      description: "Surrogate key"
    - name: entity_id
      dtype: string
```

The `odl_dim_builder` DAG will automatically detect the new entry and create the corresponding Delta table on Silver S3.

---

## Airflow DAGs

**18 DAGs** running on Airflow 3.2.1 with CeleryExecutor:

| DAG | Schedule | Category | Purpose |
|-----|----------|----------|---------|
| `rdl_weather_ingestion` | `0 8 * * *` | RDL | Open-Meteo → 8 NW grid points hourly weather |
| `rdl_wind_ingestion` | `0 8 30 * * *` | RDL | Open-Meteo → Multi-height wind data |
| `nw_bird_ingestion` | `0 9 * * *` | RDL | eBird + NW Birding → Bird sightings |
| `rdl_airports_ingestion` | `0 6 * * 1` | RDL | OurAirports CSV → Global airport data |
| `rdl_postcodes_ingestion` | `0 7 * * 1` | RDL | postcodes.io → Full UK postcode register |
| `rdl_companies_enhanced` | `0 10 * * *` | RDL | Companies House API → NW company register |
| `rdl_landscapes_ingestion` | `0 6 1 * *` | RDL | UK landscapes + habitats |
| `private_jet_ingestion` | `0 * * * *` | RDL | OpenSky → NW airspace ADS-B positions |
| `odl_dim_builder` | Asset-triggered | ODL | YAML-driven dimension table builder |
| `odl_fact_builder` | Asset-triggered | ODL | YAML-driven fact table builder |
| `odl_mapping_builder` | Asset-triggered | ODL | YAML-driven mapping table builder |
| `companies_house_upload` | Manual | Utility | Bulk Companies House CSV upload |
| `drive_sync_dag` | `0 2 * * *` | Utility | Google Drive file sync |
| `budget_monitor_dag` | `0 9 * * 1` | Ops | AWS Cost Explorer monitoring |
| `invoice_engine` | Manual | Business | Invoice generation pipeline |
| `receipt_archive_dag` | `0 3 * * *` | Ops | Receipt archival to S3 |

> **Asset-triggered** DAGs use Airflow's Asset-based scheduling — they automatically run when upstream RDL DAGs publish new data.

---

## Infrastructure

### Lightsail + Docker Compose

The entire platform runs on a single **AWS Lightsail** instance via Docker Compose:

```mermaid
flowchart TB
    subgraph LIGHTSAIL["AWS Lightsail — bellosdata-platform"]
        subgraph DOCKER["Docker Compose"]
            PG["PostgreSQL 16\n(Airflow metadata)"]
            RD["Redis 7.2\n(Celery broker)"]
            API["Airflow API Server\n:8081"]
            SCH["Airflow Scheduler"]
            WRK["Airflow Worker"]
            TRG["Airflow Triggerer"]
            DAG["DAG Processor"]
            UC["Unity Catalog\n:8070 API / :3000 UI"]
        end
    end

    PG --- API
    RD --- WRK
    API --- SCH
    SCH --- WRK
    SCH --- DAG
    SCH --- TRG

    style LIGHTSAIL fill:#FF9900,color:#fff
    style DOCKER fill:#2496ED,color:#fff
```

| Component | Details |
|-----------|---------|
| **Instance** | Lightsail `medium_3_0` — 4 GB RAM, 2 vCPU, 80 GB SSD |
| **OS** | Amazon Linux 2023 |
| **Static IP** | Elastic IP attached for persistent access |
| **Ports** | 8081 (Airflow UI), 8070 (Unity Catalog API), 3000 (Unity Catalog UI) |
| **Docker services** | 8 containers: PostgreSQL, Redis, API Server, Scheduler, Worker, Triggerer, DAG Processor, Unity Catalog |

### Terraform Resources

| Resource | Description |
|----------|-------------|
| `aws_lightsail_instance` | Data platform instance with Docker bootstrap |
| `aws_lightsail_static_ip` | Persistent IP for Airflow + Unity Catalog access |
| `aws_ecr_repository` | Container registry for pipeline images |
| `aws_ecs_cluster` + `task_definition` | Fargate-ready for future container workloads |
| `aws_apigatewayv2_api` | HTTP API Gateway for serverless Lambda API |
| `aws_lambda_function` | `ledger-cloud-api` — always-on serverless endpoint |

---

## Cost Estimate

| Service | Monthly | Notes |
|---------|---------|-------|
| Lightsail | $24.00 | Always-on 4 GB instance |
| S3 (Bronze + Silver) | ~$0.05 | < 500 MB Delta Lake tables |
| Lambda (API) | $0.00 | Free Tier |
| API Gateway | $0.00 | Free Tier |
| ECR | $0.00 | Free Tier storage |
| **Total** | **~$24/month** | |

---

## Quick Start

```bash
# Clone
git clone https://github.com/TimiOlayinka/bellosdata-lakehouse.git
cd bellosdata-lakehouse

# Create .env (AWS credentials for S3 access)
cat > airflow/.env <<EOF
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-west-2
EOF

# Start the platform locally
cd airflow
docker compose up -d

# Access services
# Airflow UI:        http://localhost:8081
# Unity Catalog UI:  http://localhost:3000
# Unity Catalog API: http://localhost:8070

# Deploy to cloud (Lightsail)
cd .. && pwsh deploy-platform.ps1
```

---

**Built by [Timi Olayinka](https://github.com/TimiOlayinka)** — Data Engineering & AI Automation
