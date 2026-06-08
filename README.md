# 🏥 Healthcare Real-Time Streaming Pipeline

An end-to-end **Real-Time Data Engineering Project** built using Local Kafka setup, Apache Spark Structured Streaming, Docker, Python, and Streamlit** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline simulates healthcare patient records, streams them through Kafka topics, processes and cleans data using Spark Structured Streaming, generates business-ready healthcare KPIs, and visualizes insights through an interactive Streamlit dashboard.

---

## 📐 Architecture Overview

```text
Healthcare Data Simulator
          │
          ▼
Bronze Layer (Producer)
          │
          ▼
Kafka Topic (Docker)
          │
          ▼
Silver Layer (Consumer)
          │
          ▼
Gold Layer (Aggregation)
          │
          ▼
Streamlit Dashboard
```

## 📁 Project Structure

```text
Kafka_Streaming_project/
│
├── producer.py
├── consumer.py
├── gold_layer.py
├── dashboard.py
├── requirements.txt
├── README.md
└── architecture.png
```

## 🥉 Bronze Layer — Data Generation

Implemented in `producer.py`

- Generates healthcare records using Faker
- Creates JSON messages
- Publishes messages to Kafka topics
- Simulates real-time patient events

## 🥈 Silver Layer — Data Processing

Implemented in `consumer.py`

- Consumes Kafka messages
- Parses JSON records
- Cleans and validates data
- Handles null values
- Creates curated healthcare datasets

## 🥇 Gold Layer — Analytics & KPIs

Implemented in `gold_layer.py`

- Reads Silver Layer data
- Creates aggregated healthcare KPIs
- Generates dashboard-ready datasets
- Produces analytical metrics

## 📊 Dashboard

Implemented in `dashboard.py`

Features:
- Real-time monitoring
- KPI cards
- Interactive charts
- Healthcare analytics
- Trend analysis

Launch:

```bash
streamlit run dashboard.py
```

## ⚙️ Tech Stack

- Apache Kafka
- Docker
- Apache Spark Structured Streaming
- PySpark
- Python
- Faker
- Streamlit
- Pandas
- Linux (WSL Ubuntu)
- Git & GitHub

## 🚀 How to Run

### Start Kafka

```bash
docker compose up -d
```

### Run Producer

```bash
python producer.py
```

### Run Consumer

```bash
spark-submit consumer.py
```

### Run Gold Layer

```bash
spark-submit gold_layer.py
```

### Launch Dashboard

```bash
streamlit run dashboard.py
```

## 👤 Author

**Salman Shaikh**

Data Engineering Portfolio Project
