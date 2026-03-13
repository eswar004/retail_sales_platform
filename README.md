# 🛒 Retail Sales Analytics Platform

> How do you turn 7 raw CSV files from a Brazilian e-commerce company into a fully automated, tested, analytics-ready data platform?
> That's exactly what this project answers.

---

## The Problem

Retail companies receive sales data daily — orders, customers, products, payments, reviews.
This data arrives as raw CSV files. It's messy, disconnected, and useless until transformed.

The business needs to answer questions like:
- Is revenue growing month over month?
- Which product categories drive the most sales?
- Are we delivering on time? Where are the delays?
- Are customers happy?

Without a proper pipeline, answering these takes hours of manual work every day.
This project automates the entire journey — from raw files to live dashboard.

---

## What I Built

An end-to-end data pipeline that:
1. **Ingests** raw CSV files into Snowflake automatically
2. **Transforms** raw data through 3 clean layers using dbt
3. **Tests** data quality after every run — 16 automated checks
4. **Orchestrates** everything daily via Airflow — no manual intervention
5. **Visualises** results in a live Streamlit dashboard connected to Snowflake

---

## Why These Tools

| Tool | Why I chose it |
|------|---------------|
| **Snowflake** | Industry standard cloud warehouse. Separation of storage and compute. Free trial available. |
| **dbt Core** | Treats SQL transformations like software — version controlled, tested, documented. |
| **Apache Airflow** | Production-grade orchestration. Gives full visibility into what ran, what failed, and why. |
| **Streamlit** | Dashboard as code — lives in the repo, version controlled, anyone can run it. |
| **Docker** | Airflow runs identically on any machine. No "works on my machine" problems. |

---

## Architecture
```
┌─────────────────────────────────────────────────────────┐
│  SOURCE                                                 │
│  Kaggle Olist Dataset (7 CSV files, 99,441 orders)      │
└─────────────────────┬───────────────────────────────────┘
                      │ Python ingestion script
                      ▼
┌─────────────────────────────────────────────────────────┐
│  SNOWFLAKE — RETAIL_DB                                  │
│                                                         │
│  STAGING  ← raw CSV data lands here                     │
│     ↓                                                   │
│  BRONZE   ← organised + incremental load                │
│     ↓        (only new records each run)                │
│  SILVER   ← cleaned + business logic applied            │
│     ↓        (revenue, delivery time, sentiment)        │
│  GOLD     ← analytics ready                             │
│             fact_orders + 4 dimension tables            │
│             + one big table (OBT) for fast queries      │
└─────────────────────┬───────────────────────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
┌─────────────────┐    ┌─────────────────────┐
│    AIRFLOW      │    │   STREAMLIT         │
│                 │    │   DASHBOARD         │
│  Runs daily     │    │                     │
│  at 6am         │    │  Connects directly  │
│                 │    │  to GOLD schema     │
│  5 tasks:       │    │                     │
│  1. Load CSVs   │    │  Revenue trends     │
│  2. Bronze      │    │  Delivery analysis  │
│  3. Silver      │    │  Customer sentiment │
│  4. Gold        │    │  Product insights   │
│  5. dbt tests   │    │                     │
└─────────────────┘    └─────────────────────┘
```

---

## Key Design Decisions

**Why Medallion Architecture (Bronze → Silver → Gold)?**
Each layer is independently queryable. If Gold breaks, Silver is untouched. If Silver has a bug, Bronze still has the raw data. Debugging is fast and safe.

**Why incremental loads instead of full refresh?**
On day 100, you don't want to reload 99,000 rows just to add 500 new ones. Bronze and Silver only process new records each run. Gold rebuilds completely because it powers the dashboard and needs to be always fresh.

**Why dbt tests after every run?**
Silent data corruption is the worst kind. 775 orders had NULL revenue — we caught and fixed that. 122 customers had duplicate IDs — fixed using ROW_NUMBER(). Tests run automatically. Bad data never reaches the dashboard.

**Why Streamlit over Power BI or Tableau?**
The dashboard lives in the repo as Python code. Anyone can clone it, run it, modify it. It is version controlled. That is not possible with .pbix or Tableau workbooks.

---

## Results

| Metric | Value |
|--------|-------|
| 💰 Total Revenue | R$ 13,221,498 |
| 📦 Total Orders | 96,478 |
| 👥 Unique Customers | 93,358 |
| 🚚 Avg Delivery Time | 12.5 days |
| ⚠️ Late Delivery Rate | 8.1% |
| ⭐ Avg Review Score | 4.16 / 5 |

![Dashboard](dashboard/screenshots/dashboard_overview.png)

---

## Interesting Findings

**Revenue grew 10x from Q4 2016 to Q4 2017** — Olist was in rapid growth phase during this period.

**Roraima (RR) has the longest average delivery time at 28 days** — remote northern state with poor logistics infrastructure.

**97% of orders are delivered** — extremely high fulfilment rate for a marketplace.

**Credit card dominates at 74% of payments** — installment culture is strong in Brazil.

**8.1% late delivery rate** — below our 10% warning threshold but worth monitoring.

---

## Project Structure
```
retail-sales-platform/
│
├── dags/
│   └── retail_sales_pipeline.py     ← Airflow DAG (5 tasks, daily at 6am)
│
├── retail_sales_platform_kaggel/    ← dbt project
│   ├── models/
│   │   ├── sources/                 ← source definitions + lineage
│   │   ├── bronze/                  ← 7 models (incremental load)
│   │   ├── silver/                  ← 7 models (transformations)
│   │   └── gold/                    ← 6 models (OBT + star schema)
│   ├── macros/
│   │   ├── generate_schema_name.sql ← removes dbt schema prefix
│   │   └── utils.sql                ← reusable macros (is_late, sentiment, is_high_value)
│   └── tests/                       ← 2 singular data quality tests
│
├── scripts/
│   └── load_to_staging.py           ← loads 7 CSVs into Snowflake staging
│
├── dashboard/
│   ├── app.py                       ← Streamlit dashboard (Python)
│   └── screenshots/
│
├── data/raw/                        ← Kaggle CSVs (gitignored — too large)
├── docker-compose.yaml              ← Airflow on Docker
└── .env.example                     ← credential template (never commit .env)
```

---

## Quickstart

**Prerequisites:** Python 3.12, Docker Desktop, Snowflake free trial, Kaggle account
```bash
# 1. Clone and set up environment
git clone https://github.com/eswar004/retail-sales-platform.git
cd retail-sales-platform
pip install uv && uv sync
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Add your credentials
cp .env.example .env
# Edit .env with your Snowflake details

# 3. Download dataset from Kaggle → place CSVs in data/raw/

# 4. Load data and run pipeline
python scripts/load_to_staging.py
cd retail_sales_platform_kaggel && dbt run && dbt test

# 5. Start Airflow
cd .. && docker compose up -d

# 6. Open dashboard
streamlit run dashboard/app.py
```

---


## Author

**Eswar** — Data Engineer

[GitHub](https://github.com/eswar004) | [LinkedIn](linkedin-url)