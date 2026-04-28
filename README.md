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

This reframes the output from a pure data science result into a **decision support tool** — which is what DisCo control room operators and NERC regulators actually need to minimise economic bleeding and protect human life.

---

## 💡 The Solution: A Three-Layer Architecture

**GridGuard** is an AI-powered Predictive Dispatch System built on a modern data stack. It transforms raw grid telemetry into actionable crew dispatch decisions across three distinct layers.

### Layer 1 — Physical Prediction (ETL Pipeline)
To predict physical grid failures, we use a traditional **Extract, Transform, Load (ETL)** approach.
- **Extract:** Raw historical weather data from the **NASA POWER API** and critical infrastructure density from the **OpenStreetMap (OSM)** Overpass API (20km radius per corridor substation).
- **Transform:** The pipeline engineers temporal features in Python — voltage lag variables (1h, 2h), rolling frequency averages (6h), volatility windows, and thermal stress multipliers derived from humidity and temperature.
- **Load & Predict:** Clean telemetry is stored in DuckDB. An **XGBoost** model trained on graduated soft labels (0.20 → 0.82 → 1.0 across the 6-hour degradation ramp) outputs a live failure probability per corridor. The model is trained on Oct/Nov data and validated against the NERC-confirmed Dec 29 Benin-Onitsha collapse — data it has never seen.

### Layer 2 — Financial Consequence (ELT Pipeline)
Instead of static approximations for financial loss, GridGuard uses a modern **Extract, Load, Transform (ELT)** paradigm.
- **Extract & Load:** Live DisCo performance data is streamed via Parquet files directly from **Hugging Face** and loaded raw into DuckDB.
- **Transform (On the Fly):** DuckDB dynamically calculates real-time ATC&C losses using each DisCo's energy received, technical loss %, commercial loss %, and the NERC Band A tariff of ₦225/kWh (MYTO 2025). Output: exact Naira loss per hour (₦M/hr) per corridor if that line goes down.

### Layer 3 — Optimized Dispatch (Streamlit Dashboard)
Given real-world crew constraints (e.g., 3 field crews available), the system merges Physical Risk (Layer 1) with Financial Consequence (Layer 2) into a mathematically ranked dispatch queue:

> *"Dispatch Crew Alpha to Ikeja-Ota 132kV: 95.25% failure risk, ₦44.44M/hr loss exposure, serving 405 Hospitals, 476 Schools and 132 Markets — highest economic consequence in the grid."*

Crews are assigned by consequence rank — not raw probability — so limited resources always protect the highest value first.

---

## 🗂️ Project Structure

```text
gridguard-nexus2026/
│
├── data/
│   └── gridguard.duckdb            # Local DuckDB OLAP database (Gitignored)
│
├── pipeline/
│   ├── ingest.py                   # ETL: REST API ingestion (OSM 20km radius, NASA POWER)
│   ├── ingest_hf_disco.py          # ELT: HuggingFace DisCo financial performance data
│   ├── simulate.py                 # Physics-based SCADA generation with graduated soft labels
│   └── features.py                 # Temporal feature engineering for XGBoost
│
├── model/
│   ├── train.py                    # XGBoost booster training on soft labels (no SMOTE)
│   ├── evaluate.py                 # Model evaluation notes (see train.py Step 5)
│   └── predict.py                  # Live inference pipeline with --demo flag
│
├── scoring/
│   ├── consequence.py              # Economic consequence scoring (prob × loss × infra)
│   └── dispatch.py                 # Ranked dispatch queue generator
│
├── dashboard/
│   └── app.py                      # Streamlit control room UI (Folium + Plotly)
│
├── config.py                       # DUCKDB_PATH, FEATURE_COLS, NERC thresholds
├── logger.py                       # Structured logging for all pipeline modules
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Running Locally

### 1. Clone the Repository
```bash
git clone https://github.com/yusufalamin001/gridguard-nexus2026.git
cd gridguard-nexus2026
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv

# Windows (Git Bash)
source venv/Scripts/activate

# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure Environment
Create a `.env` file in the root directory:

```bash
GRIDGUARD_DB_PATH=data/gridguard.duckdb
```

### 4. Execute the Data Pipelines
> ⚠️ Use the `-m` flag to maintain correct module paths.

```bash
# ETL: Physical parameters — OSM infrastructure + NASA POWER weather
python -m pipeline.ingest

# ELT: Financial parameters — HuggingFace DisCo performance dataset
python -m pipeline.ingest_hf_disco

# Generate physics-based SCADA telemetry with graduated degradation labels
python -m pipeline.simulate

# Engineer temporal features for XGBoost
python -m pipeline.features
```

### 5. Train Model & Generate Predictions

```bash
# Train XGBoost on graduated soft labels — chronological Oct/Nov train, Dec test
python -m model.train

# Run live inference (--demo replays the Dec 29 2025 Benin-Onitsha collapse)
python -m model.predict --demo
```

### 6. Run the Scoring & Dispatch Engine

```bash
# Calculate economic consequence scores (probability × loss rate × infra multiplier)
python -m scoring.consequence --demo

# Rank corridors and generate the optimised dispatch queue
python -m scoring.dispatch
```

### 7. Launch the Control Room Dashboard
```bash
streamlit run dashboard/app.py
```

---

## 🔬 Model Design Decisions

**Why XGBoost over a neural network?**
XGBoost is interpretable, trains on tabular data without needing thousands of examples, and handles class imbalance well. For a grid operator who needs to explain a dispatch decision to NERC, "the model weighted voltage sag + frequency deviation + rolling averages" is defensible. A black-box neural network is not.

**Why soft labels instead of binary labels?**
Binary labels (0 or 1) force XGBoost to learn a cliff edge — normal until the moment of collapse. Real grid failures have a 6-hour degradation signature. Graduated soft labels (0.20 at −6h, 0.38 at −4h, 0.82 at −1h, 1.0 at collapse) teach the model that risk builds progressively, producing a realistic probability spread across the dispatch queue.

**Why out-of-time validation instead of random split?**
Random splits leak future information into training — a model that "knows" December patterns from training data will score artificially high. GridGuard trains exclusively on Oct/Nov data and validates on unseen December data, including the NERC-confirmed Dec 29 collapse event.

**Why synthetic data?**
Real-time SCADA data from TCN/DisCos is not publicly available. Our synthetic telemetry is generated using physical equations (thermal derating, humidity-driven insulation resistance loss) calibrated to NERC Q4 2025 grid parameters — not invented values. The Dec 29 collapse event serves as ground truth validation.

---

## 🏗️ Production Architecture Roadmap

GridGuard is built on a local stack but designed to scale into a full enterprise-grade system.

### 1. Pipeline Orchestration
- **Current:** Manual execution per module
- **Production:** Apache Airflow or Prefect DAGs for automated hourly ingestion and retraining triggers.

### 2. Real-Time SCADA Integration
- **Current:** Synthetic telemetry generated from NERC baselines
- **Production:** Apache Kafka or AWS Kinesis ingesting live MQTT streams from TCN substations directly into DuckDB.

### 3. Cloud Data Warehousing
- **Current:** Local DuckDB file (`gridguard.duckdb`)
- **Production:** Snowflake or Google BigQuery with dbt for versioned SQL transformations.

### 4. MLOps & Model Registry
- **Current:** Local XGBoost model artifact (`model/artifacts/xgboost_gridguard.json`)
- **Production:** MLflow for experiment tracking, model versioning, drift monitoring, and automated retraining pipelines.

### 5. Deployment
- **Current:** Local Streamlit server
- **Production:** Docker container deployed via AWS Fargate or Google Cloud Run for high-availability control room access across all 11 DisCo zones.

---

## 📊 Data Sources

| Source | Data | Usage |
|---|---|---|
| NERC Q4 2025 Quarterly Report | Grid frequency, voltage, plant availability, ATC&C losses | Model calibration & ground truth |
| NASA POWER API | Hourly temperature & humidity (Q4 2025) | Weather features per corridor |
| OpenStreetMap Overpass API | Hospital, school, market counts (20km radius) | Critical infrastructure scoring |
| HuggingFace — electricsheepafrica/nigerian_electricity_disco_performance | DisCo energy received, technical & commercial losses | Economic consequence calculation |
| NERC MYTO 2025 | Band A tariff ₦225/kWh | Loss rate monetisation |

---

## 👥 Team

| Name | Role |
|---|---|
| Yusuf Al-amin | Data Engineer & ML Lead |
| Azeez Wasiu | Electrical/Power Engineering (Domain Expert) |
| Alade Rahmat | Frontend & UI/UX Designer |
| Obanor Mercy | Business Impact & Pitch Lead |

---

## 📄 License

This project is licensed under the **MIT License** — open for educational and research use.