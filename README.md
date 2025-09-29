# Airbnb Berlin Data Engineering Pipeline 🚀

> **End-to-end data engineering project** building production-grade pipelines and analytics on Airbnb listings, hosts & reviews for Berlin.   
> 
> Uses **DBT**, **Dagster**, **Snowflake**, **Elementary** and **Preset** to demonstrate modern best practices in data modeling, orchestration, observability and visualization.


## 📌 Table of Contents

* [Project Overview](#-project-overview)
* [Tech Stack](#-tech-stack)
* [Architecture](#-architecture)
* [Data Sources](#-data-sources)
* [Data Modeling & Features](#-data-modeling--features)
* [Observability & Monitoring](#-observability--monitoring)
* [Dashboards](#-dashboards)
* [Screenshots & Diagrams](#-screenshots--diagrams)
* [Future Improvements](#-future-improvements)
* [How to Run Locally](#-how-to-run-locally)
* [Author](#-author)


## 📖 Project Overview

This project simulates a **production data platform**:

* Ingests **Airbnb Berlin public data** (listings, hosts, reviews).
* Applies **transformations, cleansing, and enrichment** using **DBT**.
* Orchestrates jobs with **Dagster**.
* Stores curated data in **Snowflake**.
* Adds **metadata, lineage, and quality monitoring**.
* Exposes insights via interactive dashboards in **Preset**.

**Scale / Metrics:**

* ~18k listings, ~14k hosts, ~410k reviews processed.
* Pipeline executes **incremental loads in <5 minutes** per run.
* ~15 DBT models, including snapshots and marts.

➡️ Goal: showcase **real-life data engineering skills**: pipelines, SCD type 2, sentiment analysis, observability, dashboards.


## 🛠 Tech Stack

| Component                    | Purpose                                 |
| ---------------------------- | --------------------------------------- |
| **DBT**                      | Data modeling, tests, snapshots, macros |
| **Dagster**                  | Orchestration of DBT tasks & scheduling |
| **Snowflake**                | Cloud DWH for raw & transformed data    |
| **Elementary**               | Data observability, test alerts (Slack) |
| **Preset / Apache Superset** | BI dashboards & analytics               |
| **GitHub**                   | Version control & portfolio             |


## 🗺 Architecture

```text
[Airbnb Public Data] 
        │
        ▼
 ┌───────────────┐
 │ Snowflake RAW │  <-- data landing zone
 └───────────────┘
        │
        ▼
 ┌─────────────────────────┐
 │ DBT Transformations     │
 │ • Staging               │
 │ • Cleansed dims/facts   │
 │ • Snapshots (SCD2)      │
 │ • Marts                 │
 └─────────────────────────┘
        │
        ▼
 ┌───────────────┐
 │ Dagster Jobs  │  <-- orchestration
 └───────────────┘
        │
        ▼
 ┌─────────────┐
 │ Preset Dash │  <-- visual analytics
 └─────────────┘
```


## 📂 Data Sources

| Table / File           | Description               | Rows approx. |
| ---------------------- | ------------------------- | ------------ |
| `raw_listings`         | Airbnb listings in Berlin | 18k          |
| `raw_hosts`            | Property hosts info       | 14k          |
| `raw_reviews`          | Reviews + sentiment score | 410k         |
| `seed_full_moon_dates` | Full moon calendar (CSV)  | 300          |


## 🧩 Data Modeling & Features

### Core Layers

1. **Staging** – basic cleaning & typing of raw tables.
2. **Dimensions & Facts**
   * `dim_hosts_cleansed`
   * `dim_listings_cleansed`
   * `fct_reviews` (incremental)
3. **Snapshots**
   * `scd_raw_listings` → SCD Type 2 tracking of listing changes.
4. **Marts**
   * `mart_fullmoon_reviews` → join with lunar calendar to evaluate review sentiment after full moons.

### Advanced Features

* SCD2 snapshots for historical tracking.
* Sentiment & temporal analysis of reviews.
* Profiling, lineage & metadata.
* Automated alerts to Slack on failed tests.


## 🔍 Observability & Monitoring

* **Elementary integration** for:
  * Data quality checks
  * Test & freshness reports
  * Slack alerts including model owner tags
* **Audit tables** capturing:
  * Invocation IDs
  * Status, rows affected, execution time


## 📊 Dashboards

Interactive boards in **Preset**:

* Sentiment distribution for reviews after full moons vs other days.
* Superhost vs host ratio.
* Listing growth trends.
* Price distribution histograms.

<img src="/dbtlearn/assets/berlin_insights_dashboard.png" width="500"/>

> Interactive dashboards built in **Preset**, connected directly to Snowflake marts.


## 🖼 Screenshots & Diagrams

| Area                 | Preview                                                                                   |
| -------------------- | ----------------------------------------------------------------------------------------- |
| Input schema         | <img src="/dbtlearn/assets/input_schema.png" width="500"/>                                |
| Dagster Job Graph    | <img src="/dbtlearn/assets/dagster_orchestration_workflow.JPG" width="500"/>              |
| Observability report | <img src="/dbtlearn/assets/Elementary%20observability%20report.png" width="500"/>         |
| Slack Alert Sample   | <img src="/dbtlearn/assets/Slack%20observability%20alert.JPG" width="500"/>               |


## 🚧 Future Improvements

* Deploy on CI/CD pipeline with dbt Cloud / Dagster Cloud.
* Add real-time ingestion layer (e.g., Kafka).
* Extend to other cities or datasets.
* Enrich reviews with NLP models (topic detection).


## 🚀 How to Run Locally

### 1️⃣ Setup Python Environment

1. Ensure a compatible Python version for DBT Snowflake: [Python compatibility](https://docs.getdbt.com/faqs/Core/install-python-compatibility)

2. Create & activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

---

### 2️⃣ Install DBT

```bash
pip install dbt-snowflake==<version>
```

> Replace `<version>` with a Python-compatible version.

---

### 3️⃣ Snowflake Setup

1. Create a free/trial Snowflake account.

2. Follow [Snowflake environment setup](https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero/blob/main/_course_resources/course-resources.md) or use [auto-importer](https://dbt-data-importer.streamlit.app/).

3. Note account URL and credentials for DBT connection.

---

### 4️⃣ Initialize & Test DBT

```bash
dbt init        # Enter Snowflake credentials
dbt debug       # Verify connection
```

---

### 5️⃣ Run Pipelines & Tests

* **DBT directly:**

```bash
dbt run
dbt test
```

* **Via Dagster:**

```bash
cd <dagster-folder>
dagster dev   # Trigger DBT jobs via UI
```

---

### 6️⃣ Preset Dashboards

1. Connect Preset to Snowflake.

2. Use materialized tables to build charts & dashboards

---

### 7️⃣ Tips

* Use `dbt debug` after credential or config changes.
* Use `dbt docs generate && dbt docs serve` to explore DAG and metadata.

## 👤 Author

**\[Enzo Avalos]** – Data Engineer   
<p>
  <a href="https://www.linkedin.com/in/enzo-g-avalos">
    <img src="https://img.shields.io/badge/linkedin-%230077B5.svg?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn" />
  </a>
  <a href="mailto:avalos.enzo.g@gmail.com">
    <img src="https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white" alt="Gmail" />
  </a>
  <a href="https://github.com/enzoavalos">
    <img src="https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white" alt="Github" />
  </a>
</p>

> Passionate about building robust, observable, and business-oriented data platforms.