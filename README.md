# Smart Parking Monitoring System — Timor Plaza, Dili

**Real-time parking monitoring using IoT simulation, Apache Kafka and Apache Spark**

> Big Data Capstone Project — S1 Siencia Komputador, DIT 2026
> Case study: **Timor Plaza Parking, Dili, Timor-Leste**

A complete, runnable Big Data system that monitors 50 parking slots in real time.
Simulated IoT sensors publish events to Kafka, Apache Spark Structured Streaming
cleans and aggregates them, PostgreSQL stores the results, and a Streamlit
dashboard shows live occupancy, analytics and alerts.

---

## Table of contents

1. [Architecture](#1-architecture)
2. [Technology stack](#2-technology-stack)
3. [Project structure](#3-project-structure)
4. [Installation (Windows / VS Code)](#4-installation-windows--vs-code)
5. [Installing Kafka and PostgreSQL](#5-installing-kafka-and-postgresql)
6. [Running the system](#6-running-the-system)
7. [The dataset](#7-the-dataset)
8. [Data quality](#8-data-quality)
9. [Analytics](#9-analytics)
10. [The dashboard](#10-the-dashboard)
11. [Alert system](#11-alert-system)
12. [Database schema](#12-database-schema)
13. [Testing](#13-testing)
14. [Performance & scalability](#14-performance--scalability)
15. [Live demonstration script](#15-live-demonstration-script)
16. [Troubleshooting](#16-troubleshooting)
17. [Capstone requirement checklist](#17-capstone-requirement-checklist)

---

## 1. Architecture

```
        50 simulated IoT parking sensors  (Area A: 25  |  Area B: 25)
                              |
                              v
                     Kafka Producer                   data_generator/ + producer/
                              |
                              v
                 Apache Kafka topic: parking-events   VELOCITY
                              |
                              v
            Apache Spark Structured Streaming         spark/spark_streaming.py
                              |
                  +-----------+-----------+
                  |                       |
            Data Cleaning            Transformation    spark/data_cleaning.py
       (7 validation rules,           (timestamps,
        deduplication)                 date/hour)
                  |                       |
                  +-----------+-----------+
                              |
                              v
                     Aggregation & Analytics          spark/analytics.py
             (occupancy, hourly trend, statistics)
                              |
                              v
                  PostgreSQL: smart_parking           database/
        parking_events | parking_summary | parking_hourly_analytics
              system_metrics | parking_alerts | 3 views
                              |
                              v
                   Streamlit dashboard                dashboard/
        KPIs | live map | charts | history | alerts | system health
```

The pipeline implements every stage the manual requires:

| Manual stage | Where it lives |
|---|---|
| Data Collection | `data_generator/generate_dataset.py` |
| Data Ingestion | `producer/kafka_producer.py` → Kafka |
| Data Processing | `spark/spark_streaming.py`, `spark/data_cleaning.py` |
| Data Storage | `database/schema.sql`, `database/database.py` |
| Data Analytics | `spark/analytics.py`, `notebooks/parking_analysis.ipynb` |
| Visualization | `dashboard/app.py` |
| Real-Time Application | the whole running pipeline |

### The 5 V's

| V | How this project demonstrates it |
|---|---|
| **Volume** | 10,000 records minimum; tested up to **1,000,000** |
| **Velocity** | continuous Kafka stream at a configurable 1 / 5 / 10+ events per second |
| **Variety** | JSON on the wire, CSV at rest, relational tables in PostgreSQL |
| **Veracity** | 7 validation rules + deduplication before any analytics |
| **Value** | live availability for drivers, peak-hour planning for management |

---

## 2. Technology stack

| Layer | Technology |
|---|---|
| Language | Python 3.9+ |
| Ingestion | Apache Kafka 3.x, `kafka-python` |
| Processing | Apache Spark 3.5.1 (Structured Streaming), PySpark |
| Storage | PostgreSQL 13+, `psycopg2` |
| Dashboard | Streamlit, Plotly |
| Data | pandas, NumPy |
| Config | python-dotenv |
| Testing | pytest, psutil |

---

## 3. Project structure

```
smart-parking-monitoring/
│
├── config/
│   ├── __init__.py
│   └── config.py                  # every setting, loaded from .env
│
├── data_generator/
│   ├── __init__.py
│   └── generate_dataset.py        # IoT sensor simulator (batch + streaming)
│
├── producer/
│   ├── __init__.py
│   └── kafka_producer.py          # publishes events to parking-events
│
├── spark/
│   ├── __init__.py
│   ├── spark_streaming.py         # the Structured Streaming job
│   ├── data_cleaning.py           # validation, cleaning, deduplication
│   └── analytics.py               # descriptive / trend / statistical
│
├── database/
│   ├── __init__.py
│   ├── database.py                # storage layer + CLI
│   └── schema.sql                 # 5 tables, 3 views, indexes
│
├── dashboard/
│   ├── __init__.py
│   ├── app.py                     # the Streamlit page (4 tabs)
│   ├── components.py              # KPI cards, parking map, alerts
│   └── charts.py                  # every Plotly chart
│
├── scripts/
│   ├── load_demo_data.py          # fill the DB without Kafka/Spark
│   └── performance_test.py        # scalability testing
│
├── tests/
│   ├── conftest.py
│   ├── test_generator.py          # 22 tests
│   ├── test_kafka.py              # 14 tests
│   ├── test_spark.py              # 18 tests
│   └── test_database.py           # 17 tests
│
├── notebooks/
│   └── parking_analysis.ipynb     # exploratory analysis & quality report
│
├── data/
│   ├── raw/                       # generated CSV datasets
│   └── processed/reports/         # performance test results
│
├── logs/                          # rotating log files
│
├── .env                           # your configuration (never commit it)
├── .env.example                   # template
├── .gitignore
├── .streamlit/config.toml         # dashboard theme
├── pytest.ini
├── requirements.txt
├── run_system.bat                 # Windows launcher
├── run_system.sh                  # Linux / macOS launcher
└── README.md
```

---

## 4. Installation (Windows / VS Code)

### 4.1 Requirements

* Python 3.9 or newer — <https://www.python.org/downloads/>
* Java 11 or 17 (Spark needs a JVM) — `java -version` must work
* PostgreSQL 13+ — <https://www.postgresql.org/download/windows/>
* Apache Kafka 3.x — <https://kafka.apache.org/downloads>

### 4.2 Set the project up

```bash
cd smart-parking-monitoring

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

Or simply double-click `run_system.bat` and choose **[1] Setup**.

### 4.3 Configure

```bash
copy .env.example .env
```

Open `.env` and set your own PostgreSQL password:

```ini
POSTGRES_PASSWORD=your_real_password
```

> The password is never written in the source code — every module reads it
> from `.env` through `config/config.py`.

---

## 5. Installing Kafka and PostgreSQL

### PostgreSQL

1. Install PostgreSQL and remember the `postgres` password.
2. Create the database and the schema:

```bash
python database/database.py --init
```

That single command creates the `smart_parking` database if it does not exist,
then all 5 tables, their indexes and the 3 views. Verify with:

```bash
python database/database.py --check --stats
```

### Kafka (Windows, KRaft mode — no ZooKeeper needed)

Extract Kafka to a short path such as `C:\kafka`, then:

```bash
cd C:\kafka

REM 1. Generate a cluster id (only the first time)
bin\windows\kafka-storage.bat random-uuid

REM 2. Format the storage with that id
bin\windows\kafka-storage.bat format -t YOUR_UUID_HERE -c config\kraft\server.properties

REM 3. Start the broker (leave this window open)
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

If you use the classic ZooKeeper distribution instead:

```bash
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
bin\windows\kafka-server-start.bat config\server.properties
```

The producer creates the `parking-events` topic itself when you pass
`--create-topic`, so no manual topic creation is needed.

---

## 6. Running the system

Open **four terminals** (or use `run_system.bat` → **[A] ALL**).

### Step 1 — Generate the dataset

```bash
python data_generator/generate_dataset.py --records 10000
```

```
Records written : 10,000
File            : data/raw/timor_plaza_parking_10000.csv
Slots / sensors : 50 / 50
```

### Step 2 — Start the Kafka producer

```bash
python producer/kafka_producer.py --create-topic --rate 5
```

```
[Kafka] Event 10001 sent  | TP-B-012 | OCCUPIED | car        | 2026-09-18T08:15:20
[Kafka] Event 10002 sent  | TP-A-004 | FREE     | motorcycle | 2026-09-18T08:15:21
[Kafka] Event 10003 sent  | TP-B-017 | OCCUPIED | motorcycle | 2026-09-18T08:15:22
```

Useful options:

```bash
python producer/kafka_producer.py --rate 1        # 1 event/second
python producer/kafka_producer.py --rate 10       # 10 events/second
python producer/kafka_producer.py --rate 0        # as fast as possible
python producer/kafka_producer.py --mode csv --limit 10000 --rate 0   # replay the dataset
python producer/kafka_producer.py --dry-run --limit 5                 # no broker needed
```

### Step 3 — Start Spark Structured Streaming

```bash
python spark/spark_streaming.py
```

or, with `spark-submit`:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/spark_streaming.py
```

Each micro-batch prints a report:

```
==============================================================================
  BATCH 7      | 2026-09-18 08:16:05 | TIMOR PLAZA SMART PARKING
==============================================================================
  Records received   : 3,000
  Valid / Rejected   : 3,000 / 0   (duplicates: 0)
  Inserted into DB   : 3,000
------------------------------------------------------------------------------
  Total Slots        : 50
  Occupied / Free    : 36 / 14
  Occupancy Rate     : 72.00%
  Availability Rate  : 28.00%
  SYSTEM STATUS      : WARNING
------------------------------------------------------------------------------
  Processing time    : 0.84 s
  Throughput         : 3,571 records/s
  Latency            : 0.412 s
==============================================================================
```

### Step 4 — Open the dashboard

```bash
streamlit run dashboard/app.py
```

Then open <http://localhost:8501>.

### Running without Kafka

Two fallbacks are built in, so a broker problem can never block a demonstration:

```bash
# A. Spark reads a directory of CSV files as a real streaming source
python spark/spark_streaming.py --source csv --once

# B. Load the dataset straight into PostgreSQL (10 seconds, dashboard ready)
python scripts/load_demo_data.py --truncate
```

---

## 7. The dataset

### Schema

```json
{
    "event_id": 1,
    "slot_id": "TP-A-001",
    "sensor_id": "S-A-001",
    "parking_area": "A",
    "vehicle_type": "car",
    "status": "occupied",
    "timestamp": "2026-09-18T08:15:20",
    "duration_minutes": 35
}
```

| Field | Type | Description |
|---|---|---|
| `event_id` | Integer | Unique event identifier |
| `slot_id` | String | Parking slot: `TP-A-001` … `TP-B-025` |
| `sensor_id` | String | Sensor: `S-A-001` … `S-B-025` |
| `parking_area` | String | `A` or `B` |
| `vehicle_type` | String | `car` or `motorcycle` |
| `status` | String | `free` or `occupied` |
| `timestamp` | DateTime | Event time |
| `duration_minutes` | Integer | Parking duration |

### The Timor Plaza parking model

```
AREA A                          AREA B
TP-A-001  (sensor S-A-001)      TP-B-001  (sensor S-B-001)
TP-A-002  (sensor S-A-002)      TP-B-002  (sensor S-B-002)
   ...                             ...
TP-A-025  (sensor S-A-025)      TP-B-025  (sensor S-B-025)

Total: 50 parking slots, 50 sensors
```

### The simulator is not random noise

Three properties make the generated data realistic, and each one is covered by
a test:

* **A daily demand curve** — quiet mornings, a lunch peak around 13:00 and an
  evening peak around 18:00, with busier weekends. The mall is modelled as open
  between 08:00 and 22:00.
* **A consistent state machine** — every slot alternates `free` ↔ `occupied`;
  a slot is never occupied twice in a row, and a `free` event always carries the
  real duration of the stay that just ended.
* **Dili's vehicle mix** — 60% motorcycles by default (`MOTORCYCLE_RATIO`).

### Generating other sizes

```bash
python data_generator/generate_dataset.py --records 10000
python data_generator/generate_dataset.py --records 50000    --days 30
python data_generator/generate_dataset.py --records 100000   --days 30
python data_generator/generate_dataset.py --records 500000   --days 60
python data_generator/generate_dataset.py --records 1000000  --days 90
```

| Option | Meaning |
|---|---|
| `--records N` | number of events (default 10000) |
| `--days N` | period the events span (default 7) |
| `--seed N` | random seed, for a reproducible dataset |
| `--dirty-ratio 0.02` | corrupt 2% of the records on purpose |
| `--output PATH` | explicit output file |

---

## 8. Data quality

`spark/data_cleaning.py` applies eight rules to every event before it reaches
the analytics layer:

| # | Check | Rejection reason |
|---|---|---|
| 1 | mandatory field null or empty | `missing_value` |
| 2 | slot not one of the 50 | `invalid_slot_id` |
| 3 | sensor not one of the 50 | `invalid_sensor_id` |
| 4 | area not `A` / `B` | `invalid_parking_area` |
| 5 | status not `free` / `occupied` | `invalid_status` |
| 6 | vehicle not `car` / `motorcycle` | `invalid_vehicle_type` |
| 7 | timestamp unparsable | `invalid_timestamp` |
| 8 | duration negative or > 24 h | `outlier_duration` |

Repeated `event_id` values are removed by `dropDuplicates`, and PostgreSQL
enforces the same rule with a `UNIQUE` constraint plus
`ON CONFLICT DO NOTHING` — so **restarting Spark can never duplicate data**.

Rejected records are not thrown away silently: they are separated with their
reason and counted in `system_metrics.invalid_records`, which the dashboard
shows on the **System** tab.

### Proving that the cleaning works

```bash
python data_generator/generate_dataset.py --records 2000 --dirty-ratio 0.05 --output data/raw/dirty.csv
mkdir data\raw\stream_input
copy data\raw\dirty.csv data\raw\stream_input\
python spark/spark_streaming.py --source csv --once
```

```
  Records received   : 2,033
  Valid / Rejected   : 1,900 / 102   (duplicates: 31)
```

---

## 9. Analytics

### Descriptive

```
Occupancy Rate    = Occupied Slots / Total Slots × 100
Availability Rate = 100 − Occupancy Rate
```

Total slots, occupied, free, occupancy rate, availability rate, total vehicles
and average parking duration.

### Trend

Hourly and daily occupancy, usage trend and **automatic peak-hour detection**
(the hour with the most arrivals). On the reference dataset:

```
08:00 →  232 events        17:00 → 1,028 events
09:00 →  379 events        18:00 → 1,087 events   <- PEAK
12:00 →  938 events        19:00 →   881 events
13:00 →  996 events        22:00 →   129 events
```

### Statistical

Average, maximum and minimum occupancy; mean, median, standard deviation,
minimum and maximum of the parking duration.

The same formulas are implemented once, in `spark/analytics.py`, and reused by
the streaming job, the dashboard and the notebook.

---

## 10. The dashboard

```
TIMOR PLAZA — SMART PARKING MONITORING
REAL-TIME PARKING MANAGEMENT
```

**Live Monitoring** — four KPI cards (total slots, available, occupied,
occupancy %), the alert banner, the live map of all 50 slots, the occupancy
trend, free vs occupied, occupancy by area and the live event feed.

```
TIMOR PLAZA PARKING — AREA A · 3/25 occupied

┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│ A-001  │ │ A-002  │ │ A-003  │ │ A-004  │
│  FREE  │ │  FREE  │ │  OCC   │ │  FREE  │
└────────┘ └────────┘ └────────┘ └────────┘
```

**Analytics** — descriptive KPIs, hourly activity with the peak hour
highlighted, average occupancy by hour, events by vehicle type, the duration
distribution and the statistical indicators.

**History** — filters on date range, parking area, status and vehicle type;
total events, average occupancy, peak hour and average duration for the
selection; the event table and a CSV export.

**System** — Kafka / Spark / PostgreSQL / Dashboard health, processed records,
streaming rate, processing time, latency, data-quality counters, the throughput
chart and the alert history.

The sidebar controls auto-refresh (2–60 seconds) and shows the active
connection settings.

### A note on the colours

The parking map is the part people read fastest, so its colours were validated
rather than chosen by taste. A red/green pair — the obvious choice — collapses
to a colour difference of ΔE 4.1 under deuteranopia and becomes unreadable for
roughly 8% of men. The palette uses green `#1baf7a` for FREE and orange-red
`#eb6834` for OCCUPIED, which keeps the expected meaning and separates by
ΔE 9.2. Every tile also prints `FREE` / `OCC` in text, and every alert carries
an icon and the level in words, so colour never carries meaning alone.

---

## 11. Alert system

| Occupancy | Level | Dashboard |
|---|---|---|
| < 70% | **NORMAL** | `● SYSTEM STATUS: NORMAL` |
| 70% – 89% | **WARNING** | `⚠ WARNING — Parking occupancy reached 82%.` |
| ≥ 90% | **CRITICAL** | `✖ CRITICAL — Parking occupancy reached 94%.` |

Thresholds are configurable in `.env`:

```ini
ALERT_WARNING_THRESHOLD=70
ALERT_CRITICAL_THRESHOLD=90
```

The Spark job writes a row into `parking_alerts` only when the level *changes*,
so the alert history stays readable instead of repeating the same state.

---

## 12. Database schema

**Database:** `smart_parking`

| Table | Purpose |
|---|---|
| `parking_events` | every cleaned event (`UNIQUE(event_id)`) |
| `parking_summary` | real-time occupancy snapshots |
| `parking_hourly_analytics` | hourly aggregates (`UNIQUE(date, hour)`) |
| `system_metrics` | processed records, processing time, throughput, latency |
| `parking_alerts` | NORMAL / WARNING / CRITICAL transitions |

| View | Purpose |
|---|---|
| `v_current_slot_status` | latest state of each of the 50 slots (live map) |
| `v_area_occupancy` | occupancy per area |
| `v_peak_hours` | hourly ranking (peak-hour detection) |

Useful commands:

```bash
python database/database.py --init        # create everything
python database/database.py --check       # test the connection
python database/database.py --stats       # row counts
python database/database.py --truncate    # empty the tables, keep the schema
python database/database.py --reset       # drop and recreate everything
```

---

## 13. Testing

```bash
pytest -v
```

```
tests/test_database.py  ................. [ 23%]
tests/test_generator.py ...................... [ 54%]
tests/test_kafka.py     ..............   [ 74%]
tests/test_spark.py     .................. [100%]

71 passed
```

71 tests cover the parking model, the generator and its daily curve, the schema
and the JSON contract, rate control and CSV replay, all eight cleaning rules,
deduplication, every analytics formula, the database schema, idempotent writes
and the filtered reads behind the History tab.

Tests that need an external service are **skipped with a clear message** when it
is not running, instead of failing:

```bash
pytest -v -m "not kafka"            # skip the tests that need a broker
pytest -v -m "not spark"            # skip the PySpark tests
pytest tests/test_generator.py -v   # one file
```

---

## 14. Performance & scalability

```bash
python scripts/performance_test.py --sizes 10000 50000 100000 500000 1000000
```

Measured on a 2-core / 8 GB machine, Spark in `local[*]`:

| Records | Processing Time | Throughput | Latency | CPU | Memory |
| ------: | --------------: | ---------: | ------: | --: | -----: |
| 10,000 | 4.30 s | 2,325 rec/s | 0.724 s | 198% | 726 MB |
| 50,000 | 3.84 s | 13,032 rec/s | 0.281 s | 198% | 784 MB |
| 100,000 | 3.75 s | 26,666 rec/s | 0.261 s | 198% | 840 MB |
| 500,000 | 8.56 s | 58,385 rec/s | 0.246 s | 198% | 1,110 MB |
| 1,000,000 | 11.63 s | 85,968 rec/s | 0.200 s | 200% | 1,204 MB |

Reading the table:

* **Throughput grows with volume** — from 2.3k to 86k records/s. Spark pays a
  fixed per-job cost (planning, task scheduling) that dominates a small
  dataset; past ~100k records the real work dominates and the system scales
  close to linearly.
* **Latency stays flat** at roughly 0.2–0.7 s per micro-batch regardless of the
  total size, which is what makes the "real-time" claim true: a 100× bigger
  dataset does not make an individual event slower.
* **CPU saturates both cores** (≈200% of one core) — the pipeline is
  compute-bound, so it would scale further on more cores.
* **Memory grows sub-linearly** — 100× the data costs only 1.7× the memory,
  because Spark streams partitions instead of loading everything at once.

The benchmark runs a **warm-up pass** before measuring. Without it the first
row would include JVM start-up and code generation — several seconds unrelated
to data size — and the whole table would be misleading.

Results are written to `data/processed/reports/performance_<timestamp>.csv`
and `.md`.

---

## 15. Live demonstration script

```
STEP 1   Generate 10,000 records
         python data_generator/generate_dataset.py --records 10000
              |
STEP 2   Start Kafka
         bin\windows\kafka-server-start.bat config\kraft\server.properties
              |
STEP 3   Start the producer
         python producer/kafka_producer.py --create-topic --rate 5
              |
STEP 4   Start Spark Streaming
         python spark/spark_streaming.py
              |
STEP 5   Data enters PostgreSQL
         python database/database.py --stats
              |
STEP 6   Open the dashboard
         streamlit run dashboard/app.py
              |
STEP 7   Parking status changes
         (visible live in the producer window)
              |
STEP 8   The dashboard updates automatically
```

### Scenarios to show

| Scenario | What to show |
|---|---|
| **1 — 10,000 events** | the producer counter, then `--stats` reaching 10,000 |
| **2 — Occupancy increases** | the map filling up and the trend line rising |
| **3 — Occupancy reaches 90%** | the banner turning **CRITICAL**, a row appearing in `parking_alerts` |
| **4 — A slot becomes free** | a tile flipping to green and the KPI cards updating |

To force scenario 3 during a rehearsal, raise the producer rate
(`--rate 10`): arrivals then outpace departures and occupancy climbs quickly.

---

## 16. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `NoBrokersAvailable` | Kafka is not running. Start the broker, then check the port with `netstat -an \| findstr 9092`. |
| `Cannot reach PostgreSQL` | The service is stopped, or `POSTGRES_PASSWORD` in `.env` is wrong. Test with `python database/database.py --check`. |
| `database "smart_parking" does not exist` | Run `python database/database.py --init`. |
| `JAVA_HOME is not set` | Install Java 11 or 17 and set `JAVA_HOME`; check with `java -version`. |
| Spark cannot download the Kafka package | The first run needs internet to fetch `spark-sql-kafka`. On an offline machine use `python spark/spark_streaming.py --source csv`. |
| `Py4JJavaError ... python worker` | Spark cannot find Python. Set `set PYSPARK_PYTHON=python` in the terminal. |
| Dashboard shows "no sensor data yet" | Nothing has reached PostgreSQL yet. Run `python scripts/load_demo_data.py` to check the dashboard on its own. |
| `ModuleNotFoundError: config` | Run the commands **from the project root**, not from inside a subfolder. |
| Spark restarts and reprocesses everything | Delete the checkpoint: `python spark/spark_streaming.py --reset-checkpoint`. |
| Port 8501 already in use | `streamlit run dashboard/app.py --server.port 8502`. |

Every component writes to `logs/`: `data_generator.log`, `producer.log`,
`spark.log`, `database.log`, `performance.log`.

---

## 17. Capstone requirement checklist

| Requirement | Status | Where |
|---|---|---|
| Real problem | ✅ | parking congestion at Timor Plaza |
| Timor Plaza case study | ✅ | 50 slots, Areas A and B, Dili vehicle mix |
| IoT simulation | ✅ | `data_generator/generate_dataset.py` |
| ≥ 10,000 records | ✅ | 10,000 default, tested to 1,000,000 |
| Kafka | ✅ | topic `parking-events` |
| Kafka streaming | ✅ | `producer/kafka_producer.py`, 1/5/10+ events/s |
| Spark | ✅ | PySpark 3.5.1 |
| Spark Structured Streaming | ✅ | `spark/spark_streaming.py` |
| Data cleaning | ✅ | 8 rules, `spark/data_cleaning.py` |
| Data transformation | ✅ | normalisation, timestamp parsing, date/hour |
| Data aggregation | ✅ | hourly, daily, area, vehicle type |
| PostgreSQL | ✅ | 5 tables, 3 views, indexes |
| Descriptive analytics | ✅ | `spark/analytics.py` |
| Trend analysis | ✅ | hourly/daily + peak-hour detection |
| Statistical analysis | ✅ | mean, median, std dev, min, max |
| Real-time monitoring | ✅ | auto-refreshing dashboard |
| Dashboard | ✅ | Streamlit, 4 tabs |
| KPI | ✅ | total / available / occupied / occupancy % |
| Charts | ✅ | trend, free vs occupied, area, vehicle, hourly, duration |
| Alerts | ✅ | NORMAL / WARNING / CRITICAL + history |
| Data quality testing | ✅ | `--dirty-ratio`, `tests/test_spark.py`, notebook |
| Performance testing | ✅ | `scripts/performance_test.py` |
| Scalability testing | ✅ | 10k → 1M records |
| Logging | ✅ | rotating files in `logs/` |
| Error handling | ✅ | every external call is guarded |
| README | ✅ | this file |
| Live demonstration | ✅ | section 15 |

---

## Author

**Big Data Capstone Project — S1 Siencia Komputador, DIT 2026**
Case study: Timor Plaza Parking, Dili, Timor-Leste
