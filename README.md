# OmniRoute - Real-Time Fleet Monitoring & Driver Safety Platform

## Project Overview

OmniRoute is a real-time fleet monitoring and operational analytics platform built using Apache Spark, Kafka, Airflow, and AWS S3.

The platform processes vehicle telemetry streams, tracks historical vehicle-driver assignments using SCD Type 2 architecture, performs fuel efficiency auditing, detects driver safety violations, applies dynamic penalty logic, and generates operational reporting snapshots.

This project was developed as part of a Data Engineering Bootcamp capstone submission.

---

# Architecture Overview
```text
Telemetry Producer
        ↓
Kafka Topic
        ↓
Spark Structured Streaming
        ↓
Realtime Vehicle Events (Gold Layer)
        ↓
SCD2 Historical Asset Tracking
        ↓
Batch Gold Pipelines
        ↓
Driver Safety + Fuel Efficiency + Fleet Reporting
        ↓
Airflow Scheduled Orchestration
```
# Tech Stack

| Component | Technology |
|---|---|
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Language | Python / PySpark |
| Orchestration | Apache Airflow |
| Storage | AWS S3 |
| Infrastructure | AWS EC2 |
| Data Modeling | SCD Type 2 |
| File Format | Parquet |

---

# Key Features Implemented

## 1. Real-Time Vehicle Telemetry Streaming

- Kafka-based telemetry ingestion
- Spark Structured Streaming consumer
- Real-time event enrichment
- S3 Gold layer writes

### Implemented Logic
- Speed violation detection
- Geofence violation detection
- Timestamp standardization
- Streaming S3 persistence

---

## 2. SCD Type 2 Asset Tracking

Historical vehicle-driver assignment tracking using SCD2 design.

### Features
- Temporal joins
- Historical record preservation
- Active/inactive assignment management
- Incremental snapshot design

---

## 3. Fuel Efficiency Auditing

Daily vehicle fuel efficiency monitoring pipeline.

### Features
- Distance calculation using window functions
- KM/L efficiency computation
- Baseline threshold comparison
- Weekend exclusion
- Maintenance-day exclusion
- Fuel anomaly detection

### Business Logic
Vehicles operating below 88% of expected fuel efficiency are flagged as anomalies.

---

## 4. Driver Safety & Penalty System

Driver strike-based penalty and suspension engine.

### Features
- Speed violation tracking
- Zone violation tracking
- Strike accumulation
- Dynamic rate adjustment
- Driver suspension logic
- Payroll-safe payout calculations
- Monthly historical snapshots

### Penalty Logic

Adjusted Rate Formula:

adjusted_rate = base_rate × (1 - (0.05 × strike_count))

Drivers with 10 or more strikes are marked as:
- SUSPENDED

---

## 5. Monthly Driver Cooldown Workflow

Automated monthly rehabilitation process orchestrated using Airflow.

### Features
- Runs on 1st day of every month
- Resets eligible driver strikes
- Restores adjusted rates
- Preserves suspended drivers
- Snapshot-based warehouse design
- Partitioned historical tracking

---

## 6. Active Fleet Snapshot Reporting

Operational reporting pipeline for monitoring active fleet distribution.

### Features
- Daily active fleet counts
- Model-wise aggregation
- Snapshot partitioning
- Historical operational reporting

---

# Data Lake Architecture

## Bronze Layer
Raw streaming and ingestion datasets.

## Silver Layer
Cleaned and transformed operational datasets.

## Gold Layer
Business-ready analytical datasets and reporting tables.

---

# Gold Layer Tables

| Dataset | Purpose |
|---|---|
| realtime_vehicle_events | Enriched streaming telemetry |
| asset_history_scd2 | Historical driver-vehicle tracking |
| fuel_efficiency_fact | Fuel audit reporting |
| driver_safety_status | Driver penalty snapshots |
| active_fleet_snapshot | Fleet reporting snapshots |

---

# Airflow DAGs

| DAG | Purpose |
|---|---|
| omniroute_batch_pipeline | Daily batch processing |
| monthly_driver_cooldown | Monthly strike reset workflow |

---

# Project Structure

```bash
omniroute-project/
│
├── airflow_dags/
├── sample_data/
├── spark_jobs/
│   ├── streaming/
│   └── batch/
├── logs/
└── README.md
```

---

# S3 Partitioning Strategy

Partitioned snapshot architecture implemented for:

- Driver safety snapshots
- Monthly cooldown snapshots
- Fleet reporting snapshots

Example:

```bash
gold/driver_safety_status/month=2026-05/
gold/driver_safety_status/month=2026-06/
```

---

# Engineering Concepts Demonstrated

- Real-time streaming pipelines
- Spark Structured Streaming
- SCD Type 2 dimensional modeling
- Temporal joins
- Snapshot fact tables
- Airflow orchestration
- Incremental warehouse design
- Partitioned data lakes
- Operational analytics
- Batch + streaming hybrid architecture

---

# Project Screenshots & Outputs

## Architecture Diagram

Detailed platform architecture:

![Architecture Diagram](screenshots/architecture_diagram.png)

---

## Spark Pipeline Outputs

Detailed Spark batch and streaming output screenshots are documented here:

[View Spark Pipeline Outputs](spark_output.md)

---

## Airflow DAG Screenshots

Airflow orchestration screenshots and DAG execution flows:

[View Airflow DAG Screenshots](Airflow_Screens.md)

---

## S3 Data Lake Screenshots

S3 Bronze/Silver/Gold layer screenshots and partitioned snapshot architecture:

[View S3 Data Lake Screenshots](s3_screens.md)

# Future Improvements

- Delta Lake / Apache Iceberg integration
- MERGE/UPSERT support
- Real-time alerting pipelines
- CI/CD integration
- Dashboard visualizations
- Containerized deployment
- Kubernetes orchestration

---

# How to Run

## Start Spark Job

```bash
spark-submit driver_safety_status_gold.py
```

## Start Airflow

```bash
AIRFLOW__WEBSERVER__WORKERS=1 airflow standalone
```

---



# Author

Disha Jain,
Vishal Gupta,
Amit Kumar,
