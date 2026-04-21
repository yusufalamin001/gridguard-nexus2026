# ⚡ GridGuard
### Predictive Grid Failure & Dispatch Optimization System

> Built for the **SPE NEXUS 3.0 Energy Hackathon** — Lagos State University, May 2026

![GridGuard UI Demo](https://img.shields.io/badge/UI-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white) ![Data Engine](https://img.shields.io/badge/OLAP-DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black) ![Machine Learning](https://img.shields.io/badge/ML-XGBoost-136699?style=for-the-badge) 

---

## 🔍 The Problem

Nigeria's national electricity grid operates in chronic stress. According to the **NERC Q4 2025 Quarterly Report**:

- Grid frequency averaged **49.38Hz–50.65Hz** — outside the regulatory safe band every single day.
- System voltage averaged **297.96kV–347.03kV** — outside the grid code range.
- Plant availability factor was only **39.64%** — meaning over 60% of installed capacity was unavailable.
- A partial grid collapse occurred on **29 December 2025**, caused by a single circuit breaker failure at the Benin transmission station.

When failures occur, Distribution Companies (DisCos) and the Transmission Company of Nigeria (TCN) respond **reactively** — dispatching crews only after the outage has already cascaded. The economic cost is severe. The weighted average ATC&C loss across all DisCos in Q4 2025 was **34.90%** — translating to a cumulative revenue loss of **₦139.19 billion** in a single quarter.

---

## 🎯 What Makes This Different

Most grid prediction projects stop at a binary answer: *will it fail or not?*

GridGuard answers the operational question utility managers actually face:

**"Given my limited crews, where do I send them first to protect the most value?"**

This reframes the output from a pure data science result into a **decision support tool** — which is what DISCO control room operators and NERC regulators actually need to minimize economic bleeding and protect human life.

---

## 💡 The Solution: A Dual ETL & ELT Architecture

**GridGuard** is an AI-powered Predictive Dispatch System built on a modern data stack. It transforms raw grid telemetry into actionable crew dispatch decisions using two distinct data paradigms.

### Layer 1 — Physical Prediction (The ETL Pipeline)
To predict physical grid failures, we use a traditional **Extract, Transform, Load (ETL)** approach. 
- **Extract:** We pull raw historical weather data from the **NASA POWER API** and critical infrastructure density from the **OpenStreetMap (OSM)** Overpass API.
- **Transform:** The pipeline engineers complex features in Python, calculating thermal stress multipliers, humidity-driven voltage sags, and rolling temporal averages.
- **Load & Predict:** The clean, transformed telemetry is loaded into DuckDB, where an **XGBoost** model reads it to calculate a live failure probability score (0-100%) for every transmission corridor based on localized grid physics.

### Layer 2 — Financial Consequence (The ELT Pipeline)
Instead of static approximations for financial loss, GridGuard uses a modern **Extract, Load, Transform (ELT)** paradigm.
- **Extract & Load:** Live DisCo performance data is streamed via Parquet files directly from **Hugging Face** and loaded raw into our vectorized **DuckDB** database.
- **Transform (On the Fly):** At the exact moment the dashboard loads, DuckDB runs lightning-fast `COALESCE` SQL joins. It dynamically calculates real-time ATC&C losses and computes the exact Naira loss per hour (₦/kWh unserved) if a specific line collapses.

### Layer 3 — Optimized Dispatch (The Streamlit UI)
Given real-world constraints (e.g., 3 field crews available), the system merges the Physical Risk (Layer 1) and the Financial Consequence (Layer 2) to output a mathematically optimized dispatch queue.

> *"Dispatch Crew Alpha to Benin-Onitsha 330kV: 78% failure risk, serves 1 Hospital and 2 Markets, preventing an estimated loss of ₦4.2M/hour."*

---

## 🗂️ Project Structure

```text
gridguard-nexus2026/
│
├── data/
│   └── gridguard.db                # Local DuckDB OLAP database (Gitignored)
│
├── pipeline/
│   ├── ingest.py                   # ETL: REST API ingestion (OSM, NASA POWER)
│   ├── ingest_hf_disco.py          # ELT: Pipeline for Hugging Face financial data
│   ├── simulate.py                 # Physics-based SCADA generation (thermal/voltage derating)
│   └── features.py                 # Feature engineering for XGBoost
│
├── model/
│   ├── train.py                    # XGBoost classifier training
│   ├── evaluate.py                 # Model evaluation and metrics
│   └── predict.py                  # Live inference pipeline
│
├── scoring/
│   ├── consequence.py              # SQL-based Economic consequence transformation
│   └── dispatch.py                 # Crew dispatch queue generator
│
├── dashboard/
│   └── app.py                      # Custom HTML/CSS Streamlit UI with Folium/Plotly
│
├── config.py                       # Global environment variables and database paths
├── requirements.txt
├── .env.example
└── README.md                                                                                                                
```

### 🚀 Running Locally

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/gridguard-nexus2026.git
cd gridguard-nexus2026
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure Environment
Create a `.env` file in the root directory and define the database path:

```bash
GRIDGUARD_DB_PATH=data/gridguard.db
```

### 4. Execute the Data Pipelines
> ⚠️ Ensure you use the `-m` flag to maintain module paths.

```bash
# ETL: Extract & Load physical parameters (NASA/OSM)
python -m pipeline.ingest

# ELT: Extract & Load financial parameters (Hugging Face)
python -m pipeline.ingest_hf_disco

# Simulate grid physics and voltage sags
python -m pipeline.simulate
```

### 5. Train Model & Generate Predictions

```bash
# Train the XGBoost classifier
python -m model.train

# Run inference on live telemetry
python -m model.predict
```

### 6. Run the Scoring & Dispatch Engine

```bash
# Calculate dynamic economic consequences
python -m scoring.consequence

# Generate the optimized dispatch queue
python -m scoring.dispatch
```
### 7. Launch the Control Room Dashboard
```bash
streamlit run dashboard/app.py
```
---

## 🏗️ Production Architecture Roadmap

GridGuard is currently built on a local stack but is designed to scale into a full enterprise-grade system.

### 1. Pipeline Orchestration
- **Current:** Manual execution (`python -m pipeline.ingest`)
- **Production:** Use Apache Airflow or Prefect to orchestrate DAGs and automate data ingestion workflows.

### 2. Real-Time Streaming (SCADA Integration)
- **Current:** Batch processing using local DuckDB tables  
- **Production:** Replace simulations with real-time streaming via Apache Kafka or AWS Kinesis, ingesting MQTT data from substations.

### 3. Cloud Data Warehousing & Transformation
- **Current:** Local DuckDB database  
- **Production:** Migrate to Snowflake or Google BigQuery  
- Use **dbt** for scalable SQL transformations within the warehouse.

### 4. MLOps & Model Registry
- **Current:** Local XGBoost `.pkl` files  
- **Production:** Integrate MLflow for:
  - Experiment tracking  
  - Model versioning  
  - Drift monitoring  
  - Continuous training pipelines  

### 5. Frontend Deployment
- **Current:** Local Streamlit server  
- **Production:**  
  - Containerize with Docker  
  - Deploy via AWS Fargate or Google Cloud Run  
  - Enable high availability for control room operators  

---

## 👥 Team

| Name          | Role                                         |
|---------------|----------------------------------------------|
| Yusuf Al-amin | Data Engineer & ML Lead                      |
| Azeez Wasiu      | Electrical/Power Engineering (Domain Expert) |
| Alade Rahmat   | Frontend & UI/UX Designer                    |
| Obanor Mercy  | Business Impact & Pitch Lead                 |

---

## 📄 License

This project is licensed under the **MIT License** — open for educational and research use.
