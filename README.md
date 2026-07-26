# 🚀 Enterprise Data Quality Monitoring Platform

A production-ready **Enterprise Data Quality Monitoring Platform** built using **Apache Airflow**, **Apache Spark**, **Great Expectations**, **PostgreSQL**, **Docker**, and **Nginx** to automate data validation, monitor data quality, and provide interactive validation reports.

## 📌 Overview

This project demonstrates an end-to-end enterprise data engineering pipeline that validates incoming datasets before they are loaded into a data warehouse. By combining workflow orchestration, distributed data processing, and automated data quality checks, the platform ensures that only trusted and validated data reaches downstream analytical systems.

The entire solution is containerized with Docker, making it easy to deploy, scale, and reproduce across development and production environments.

---

## 🏗️ Architecture

```text
Raw CSV Files
       │
       ▼
 Apache Airflow
(Workflow Orchestration)
       │
       ▼
 Apache Spark + Great Expectations
(Data Validation & Processing)
       │
 ┌─────┴───────────────┐
 │                     │
 ▼                     ▼
Valid Data         Invalid Data
 │                     │
 ▼                     ▼
PostgreSQL        Failed Archive
(Data Warehouse)      │
 │                     │
 ▼                     ▼
Great Expectations Validation Results
 │
 ▼
Nginx (Interactive Data Docs)
```

---

## ✨ Key Features

* 📂 Automated CSV data ingestion
* ⚙️ Workflow orchestration with Apache Airflow
* ⚡ Distributed data validation using Apache Spark
* ✅ Enterprise-grade data quality checks with Great Expectations
* 🗄️ PostgreSQL used for metadata storage and analytical warehouse
* 📊 Interactive Data Docs served through Nginx
* 📦 Fully containerized deployment using Docker Compose
* 🔄 Automatic routing of successful and failed datasets
* 📈 Scalable architecture suitable for enterprise environments

---

## 🛠️ Technology Stack

| Category               | Technologies            |
| ---------------------- | ----------------------- |
| Workflow Orchestration | Apache Airflow          |
| Distributed Processing | Apache Spark (PySpark)  |
| Data Validation        | Great Expectations      |
| Database               | PostgreSQL              |
| Web Server             | Nginx                   |
| Containerization       | Docker & Docker Compose |
| Programming Language   | Python                  |
| Data Format            | CSV                     |

---

## 📂 Project Structure

```text
.
├── docker-compose.yml
├── .env
├── README.md
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── generate_mock_data.py
│       └── data_quality_pipeline.py
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       └── validate_and_process.py
├── great_expectations/
│   ├── great_expectations.yml
│   ├── expectations/
│   └── checkpoints/
├── config/
│   └── pg_init.sql
├── data/
│   ├── incoming/
│   ├── archive/
│   └── warehouse/
└── web/
    └── nginx.conf
```

---

## 🔄 Data Pipeline

1. Raw CSV datasets are placed into the **Incoming** directory.
2. Apache Airflow detects new datasets and triggers the processing workflow.
3. Apache Spark loads the data and executes distributed validation using Great Expectations.
4. Validation reports are generated automatically.
5. Valid datasets are stored inside PostgreSQL.
6. Invalid datasets are moved to the Failed Archive.
7. Interactive Data Docs are published through Nginx.
8. Validation metadata is stored for auditing and monitoring purposes.

---

## ✅ Data Validation Rules

The pipeline validates datasets against predefined business rules, including:

* Transaction ID must be unique
* Transaction ID cannot be null
* Customer ID cannot be null
* Transaction Amount must be greater than zero
* Transaction Date must be a valid timestamp
* Product Category must belong to an approved list

---

## 📊 Monitoring

The platform provides comprehensive monitoring through:

* Apache Airflow Dashboard
* Spark Cluster UI
* Great Expectations Data Docs
* PostgreSQL Metadata Store
* Container Logs
* Validation Reports

---

## 🚀 Deployment

```bash
docker compose build

docker compose up -d
```

Available Services

| Service                      | URL                   |
| ---------------------------- | --------------------- |
| Airflow                      | http://localhost:8080 |
| Spark Master UI              | http://localhost:8081 |
| Great Expectations Data Docs | http://localhost:8082 |
| PostgreSQL                   | localhost:5432        |

---

## 🧪 Verification

* Generate sample datasets using the mock data DAG.
* Execute the Data Quality Pipeline.
* Monitor execution in Apache Airflow.
* Review Spark job logs.
* Verify validated records in PostgreSQL.
* Inspect interactive Great Expectations Data Docs.
* Confirm failed datasets are archived correctly.

---

## 🎯 Learning Outcomes

This project demonstrates practical experience with:

* Enterprise Data Engineering
* Workflow Orchestration
* Distributed Data Processing
* Data Quality Engineering
* ETL Pipeline Design
* Data Validation Automation
* Docker-Based Infrastructure
* PostgreSQL Data Warehousing
* Scalable Data Pipelines
* Production-Ready Architecture

---
