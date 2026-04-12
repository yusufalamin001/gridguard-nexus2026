# ⚡ GridGuard
### Predictive Grid Failure & Dispatch Optimization System

> Built for the **SPE NEXUS 3.0 Energy Hackathon** — Lagos State University, May 2026

---

## 🔍 The Problem

Nigeria's national electricity grid operates in chronic stress. According to the **NERC Q4 2025 Quarterly Report**:

- Grid frequency averaged **49.38Hz–50.65Hz** — outside the regulatory safe band of 49.75Hz–50.25Hz every single day
- System voltage averaged **297.96kV–347.03kV** — outside the grid code range of 313.50kV–346.50kV
- Plant availability factor was only **39.64%** — meaning over 60% of installed capacity was unavailable at any time
- A partial grid collapse occurred on **29 December 2025**, caused by a single circuit breaker failure at Benin transmission station

When failures occur, Distribution Companies (DisCos) and the Nigerian Independent System Operator (NISO) respond **reactively** — dispatching crews only after the outage has already cascaded. There is no operational system that:

1. Predicts failure windows **before** they occur
2. Ranks which zones to protect and restore **first** given limited repair crews

The economic cost is severe. The weighted average ATC&C (Aggregate Technical, Commercial and Collection) loss across all DisCos in Q4 2025 was **34.90%** — translating to a cumulative revenue loss of **₦139.19 billion** in a single quarter.

---

## 💡 The Solution

**GridGuard** is a three-layer data system that transforms raw grid telemetry into actionable crew dispatch decisions.

```
Raw Grid Data → Prediction Layer → Consequence Scoring → Dispatch Queue
```

### Layer 1 — Prediction
Ingests time-series data on grid frequency, voltage variance, plant availability factor, and DisCo energy offtake. An **XGBoost classifier** outputs a probability score per transmission corridor:

> *"78% probability of partial grid collapse within the next 6 hours on the Benin–Onitsha corridor"*

### Layer 2 — Consequence Scoring
Weights each at-risk corridor by:
- **Critical infrastructure exposure** (hospitals, markets, schools via OpenStreetMap)
- **DisCo ATC&C loss rate** (commercially stressed zones prioritized differently)
- **Economic loss per hour** (₦/kWh unserved — based on World Bank Nigeria enterprise survey data)

### Layer 3 — Dispatch Queue
Given real-world constraints (2–4 field crews per district), outputs a **ranked action list**:

> *"You have 3 crews. Send Crew 1 to Benin corridor — 78% failure risk, serves Stella Maris Hospital and 12,000 customers, estimated loss ₦4.2M/hour. Crew 2 to..."*

---

## 🎯 What Makes This Different

Most grid prediction projects stop at a binary answer: *will it fail or not?*

GridGuard answers the operational question utility managers actually face:

**"Given my limited crews, where do I send them first to protect the most value?"**

This reframes the output from a data science result to a **decision support tool** — which is what DISCO control room operators and NERC regulators actually need.

---

## 🗂️ Project Structure

```
gridguard/
│
├── data/
│   ├── raw/                  # Raw data from NERC reports and APIs
│   ├── processed/            # Cleaned and feature-engineered data
│   └── gridguard.duckdb      # DuckDB database
│
├── pipeline/
│   ├── ingest.py             # Data ingestion from NERC, NASA POWER, OSM
│   ├── features.py           # Feature engineering (lag features, rolling stats)
│   └── simulate.py           # Synthetic SCADA data generator
│
├── model/
│   ├── train.py              # XGBoost classifier training
│   ├── evaluate.py           # Model evaluation and metrics
│   └── predict.py            # Inference pipeline
│
├── scoring/
│   ├── consequence.py        # Economic consequence scoring per corridor
│   └── dispatch.py           # Crew dispatch queue generator
│
├── dashboard/
│   └── app.py                # Streamlit dashboard
│
├── notebooks/
│   └── exploration.ipynb     # EDA and feature analysis
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## 📊 Data Sources

| Source | Variables | Access |
|--------|-----------|--------|
| NERC Quarterly Reports | Frequency, voltage, TLF, DisCo ATC&C, plant availability | Free — nerc.gov.ng |
| NASA POWER API | Temperature, humidity (transformer thermal stress) | Free — no API key required |
| OpenStreetMap Overpass API | Hospitals, markets, schools per feeder zone | Free — open data |
| World Bank Nigeria Enterprise Survey | Cost of power interruption (₦/kWh unserved) | Free — published report |
| Synthetic SCADA data | Simulated feeder-level frequency/voltage readings | Generated from NERC baselines |

> **Transparency note:** Real feeder-level SCADA data from Nigerian DisCos is not publicly available. All model training uses synthetic data generated from NERC-published statistics and clearly documented as simulated. The architecture is designed to accept real SCADA data as a direct replacement without changing the pipeline structure.

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11+ |
| Data Storage | DuckDB |
| ML Model | XGBoost |
| Dashboard | Streamlit |
| Data Ingestion | CCXT-style REST clients, Pandas |
| Geospatial | OSMnx, Folium |
| Environment | Python-dotenv |

---

## 🚀 How to Run Locally

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/gridguard-nexus2026.git
cd gridguard-nexus2026
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables

```bash
cp .env.example .env
# No API keys required for NASA POWER or OpenStreetMap
# Edit .env only if you have real SCADA data sources to connect
```

### 5. Run the Data Pipeline

```bash
# Step 1: Ingest and store raw data
python pipeline/ingest.py

# Step 2: Generate synthetic SCADA data (if no real data available)
python pipeline/simulate.py

# Step 3: Engineer features
python pipeline/features.py
```

### 6. Train the Model

```bash
python model/train.py
python model/evaluate.py
```

### 7. Launch the Dashboard

```bash
streamlit run dashboard/app.py
```

Open your browser at `http://localhost:8501`

---

## 📈 Key Metrics from Evidence Base

| Metric | Value | Source |
|--------|-------|--------|
| Grid frequency violation | Every day in Q4 2025 | NERC Q4 2025, Section 2.2.2 |
| Plant availability factor | 39.64% | NERC Q4 2025, Section 2.1.2 |
| Aggregate ATC&C loss | 34.90% | NERC Q4 2025, Section 2.3.5 |
| Cumulative revenue loss (Q4) | ₦139.19 billion | NERC Q4 2025, Table 9 |
| Kaduna DisCo ATC&C | 69.45% (target: 21.32%) | NERC Q4 2025, Table 9 |
| Dec 2025 grid collapse cause | Benin-Onitsha 330kV breaker failure | NERC Q4 2025, Table 3 |

---

## 👥 Team

| Name | Role |
|------|------|
| Yusuf Al-amin | Data Engineer & ML Lead |
| [Name] | Electrical/Power Engineering — Domain Expert |
| [Name] | Frontend & Dashboard Developer |
| [Name] | Business Impact & Pitch Lead |

---

## 🏆 Hackathon

**Event:** SPE NEXUS 3.0 Energy Policy and Innovation Conference  
**Host:** SPE Lagos State University Student Chapter  
**Date:** May 2, 2026  
**Category:** Energy Innovation Hackathon  

---

## 📄 License

MIT License — open for educational and research use.

---

## 📬 Contact

For questions about this project, open an issue on this repository.
# gridguard-nexus2026
Predictive Grid Failure &amp; Dispatch Optimization System
