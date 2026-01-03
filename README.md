# GitHub README for Your Project
# Real-Time E-Commerce Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-red.svg)](https://www.getdbt.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> A production-grade streaming data pipeline that processes 1,000+ e-commerce events per minute with <5 second end-to-end latency. Built to demonstrate modern data engineering practices including real-time processing, medallion architecture, and business intelligence.

![Dashboard Preview](docs/images/dashboard_overview.png)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Business Metrics](#business-metrics)
- [Performance](#performance)
- [Learnings & Challenges](#learnings--challenges)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## 🎯 Overview

This project simulates a real-time e-commerce analytics system that answers critical business questions:

- **What's our current conversion rate?** (updates every 5 seconds)
- **Where do users drop off?** (funnel visualization)
- **Which products drive revenue?** (real-time rankings)
- **Who are our best customers?** (RFM segmentation)
- **Which users should we retarget?** (hot leads identification)

Built as part of my transition from banking operations to data engineering, this project showcases skills in streaming architectures, data transformation, and business analytics.

---

## 🏗️ Architecture

### Architecture Diagram
![Architecture](docs/images/architecture_diagram.g)

**Design Principles:**
- **Decoupling:** Kafka separates producers from consumers
- **Fault Tolerance:** PySpark checkpointing + message replay
- **Scalability:** Each component scales independently
- **Data Quality:** Validation at every layer (defense in depth)

---

## ✨ Features

### 🔄 Real-Time Processing
- **5-second latency** from event generation to dashboard
- **1,000+ events/minute** throughput
- **Micro-batch processing** with PySpark Structured Streaming
- **Automatic checkpointing** for fault recovery

### 📊 Business Analytics
- **Conversion Funnel:** Page View → Add to Cart → Purchase
- **RFM Segmentation:** Champion, Loyal, Potential, At Risk, Lost
- **Product Performance:** Revenue rankings, conversion rates
- **User Behavior:** Lifecycle stages, engagement levels
- **Hot Leads:** Cart abandoners with purchase propensity scores

### 🏛️ Medallion Architecture
- **Bronze Layer:** Raw events from Kafka (audit trail)
- **Silver Layer:** Cleaned, deduplicated, validated data
- **Gold Layer:** Business-ready aggregations and metrics

### 🎨 Interactive Dashboard
- **3 Pages:** Overview, User Behavior, Live Stream
- **Auto-refresh:** Every 5 seconds (configurable)
- **Export-ready:** Screenshots for reports
- **Mobile-responsive:** Works on tablets/phones

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Streaming** | Apache Kafka (Redpanda) | Message broker for event streaming |
| **Processing** | PySpark 3.5 (Structured Streaming) | Real-time stream processing |
| **Storage** | PostgreSQL 15 | Data warehouse (Bronze/Silver/Gold) |
| **Transformation** | dbt 1.7 | SQL-based transformations & testing |
| **Visualization** | Streamlit + Plotly | Interactive dashboard |
| **Orchestration** | Docker Compose | Container management |
| **Language** | Python 3.9+ | Producer, consumer, dashboard |

---

## 🚀 Quick Start

### Prerequisites

- **Operating System:** Linux, macOS, or Windows with WSL2
- **Docker & Docker Compose:** v20.10+
- **Python:** 3.9 or higher
- **RAM:** 8GB minimum (12GB recommended)
- **Disk Space:** 5GB free

### Installation

```bash
# 1. Clone repository
git clone https://github.com/YOUR_USERNAME/streaming-ecommerce-pipeline.git
cd streaming-ecommerce-pipeline

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start infrastructure (Kafka, PostgreSQL)
cd docker
docker-compose up -d

# Wait 30 seconds for services to initialize
docker ps  # Verify 3 containers running

# 5. Initialize database schema
docker exec -it postgres-streaming psql -U postgres -d streaming_db -f /docker-entrypoint-initdb.d/init-db.sql
```

### Running the Pipeline

**Terminal 1: Start Event Producer**
```bash
cd producer
python event_producer.py
```

**Terminal 2: Start PySpark Consumer**
```bash
cd consumer
python spark_streaming_consumer.py
```

**Terminal 3: Run dbt Transformations**
```bash
cd ecommerce_analytics
dbt run
```

**Terminal 4: Launch Dashboard**
```bash
cd dashboard
streamlit run app.py
```

**Access Dashboard:** http://localhost:8501

---

## 📁 Project Structure

```
streaming-ecommerce-pipeline/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── LICENSE                            # MIT License
├── .gitignore                         # Git ignore rules
│
├── docs/                              # Documentation & diagrams
│   ├── images/                        # Screenshots
│   └── architecture.md                # Detailed architecture docs
│
├── docker/                            # Docker configurations
│   ├── docker-compose.yml             # Service definitions
│   └── init-db.sql                    # PostgreSQL initialization
│
├── producer/                          # Event generation
│   ├── event_producer.py              # Kafka producer script
│   └── .env.example                   # Environment variables template
│
├── consumer/                          # Stream processing
│   ├── spark_streaming_consumer.py    # PySpark consumer
│   └── .env.example                   # Environment variables template
│
├── ecommerce_analytics/               # dbt project
│   ├── dbt_project.yml                # dbt configuration
│   ├── models/
│   │   ├── silver/                    # Staging & intermediate models
│   │   │   ├── schema.yml
│   │   │   ├── stg_clickstream_events.sql
│   │   │   └── int_user_sessions.sql
│   │   └── gold/                      # Business marts
│   │       ├── schema.yml
│   │       ├── mart_daily_metrics.sql
│   │       ├── mart_conversion_funnel.sql
│   │       ├── mart_product_performance.sql
│   │       └── mart_user_behavior.sql
│   └── tests/                         # Data quality tests
│
└── dashboard/                         # Streamlit app
    ├── app.py                         # Main dashboard
    ├── db_connector.py                # Database connection pool
    └── .env.example                   # Environment variables template
```

---

## 🔄 Data Flow

### 1. Event Generation (Producer)
```python
# Simulates realistic user behavior
User visits site → Views products → Adds to cart (30% chance) → Purchases (50% of carts)
```

**Event Types:**
- `page_view`: User lands on product page
- `add_to_cart`: User adds item to cart
- `purchase`: User completes checkout

**Event Schema:**
```json
{
  "event_id": "EVT_a1b2c3d4",
  "event_timestamp": "2026-01-03T14:23:45",
  "user_id": "USER_12345",
  "session_id": "SESSION_abcdef",
  "event_type": "purchase",
  "product_id": "P001",
  "product_name": "Wireless Earbuds",
  "product_category": "Electronics",
  "product_price": 79.99,
  "quantity": 2,
  "device_type": "Mobile",
  "country": "Nigeria"
}
```

### 2. Message Broker (Kafka/Redpanda)
- Events published to `ecommerce-clickstream` topic
- Retained for 7 days (configurable)
- Multiple consumers can read independently

### 3. Stream Processing (PySpark)
```python
# Micro-batch processing every 5 seconds
Read from Kafka → Parse JSON → Validate schema → Write to PostgreSQL Bronze
```

**Data Quality Checks:**
- Schema validation (reject malformed events)
- Null handling for required fields
- Deduplication by `event_id`

### 4. Data Transformation (dbt)

**Bronze Layer (Raw)**
```sql
-- Exact copy of streaming events
-- No transformations, complete audit trail
```

**Silver Layer (Cleaned)**
```sql
-- stg_clickstream_events: Deduplicated, enriched with derived fields
-- int_user_sessions: Aggregated by session with conversion flags
```

**Gold Layer (Business Metrics)**
```sql
-- mart_daily_metrics: Executive dashboard KPIs
-- mart_conversion_funnel: User journey analysis
-- mart_product_performance: Product rankings
-- mart_user_behavior: RFM segmentation
```

### 5. Visualization (Streamlit)
- Connects to Gold layer (pre-aggregated tables)
- Auto-refresh every 5 seconds
- Connection pooling for efficiency

---

## 📊 Business Metrics

### Key Performance Indicators

| Metric | Definition | Business Value |
|--------|-----------|----------------|
| **Conversion Rate** | (Purchases / Page Views) × 100 | Overall funnel efficiency |
| **Cart Rate** | (Add to Cart / Page Views) × 100 | Product engagement |
| **Checkout Rate** | (Purchases / Add to Cart) × 100 | Checkout process optimization |
| **Cart Abandonment** | (Abandoned Carts / Total Sessions) × 100 | Revenue leakage indicator |
| **Average Order Value** | Total Revenue / Purchases | Revenue per transaction |
| **Revenue Per User** | Total Revenue / Unique Users | Customer value |

### RFM Segmentation

**Recency:** Days since last session (1-5, 5 = most recent)  
**Frequency:** Number of sessions (1-5, 5 = most frequent)  
**Monetary:** Lifetime revenue (1-5, 5 = highest value)

**Customer Tiers:**
- **Champion (13-15 score):** Best customers, focus on retention
- **Loyal (10-12):** Regular buyers, upsell opportunities
- **Potential (7-9):** Engaged browsers, conversion campaigns
- **At Risk (5-6):** Declining activity, win-back needed
- **Lost (3-4):** Inactive, low priority

---

## ⚡ Performance

### Benchmark Results (30-minute test)

| Metric | Value |
|--------|-------|
| **Events Processed** | 15,000+ |
| **Average Latency** | 4.2 seconds |
| **Peak Throughput** | 1,200 events/min |
| **Data Quality Score** | 100% (all dbt tests passing) |
| **Revenue Tracked** | ₦2.1M (~$1,300 USD) |
| **Unique Users** | 850+ |
| **Total Sessions** | 1,200+ |
| **Conversion Rate** | 2.3% |
| **Cart Abandonment** | 23% |

### Resource Usage

| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Redpanda | 15% | 800MB | 500MB |
| PostgreSQL | 10% | 600MB | 1.2GB |
| PySpark Consumer | 45% | 2.5GB | N/A |
| Streamlit Dashboard | 5% | 250MB | N/A |

**System:** Intel i5-3210M @ 2.5GHz, 12GB RAM, 128GB SSD

---

## 💡 Learnings & Challenges

### Challenge 1: Null Quantities in Purchase Events

**Problem:** Purchase events had `NULL` quantities, breaking revenue calculations.

**Root Cause:** Python's shallow copy in event generation:
```python
# BAD: Shallow copy preserves None
cart_event = page_view_event.copy()
cart_event['quantity'] = random.randint(1, 3)
```

**Solution (Defense in Depth):**
1. **Producer:** Rewrote event generation to avoid shallow copies
2. **Consumer:** Added PySpark validation:
   ```python
   .withColumn("quantity", 
       when((col("event_type") == "purchase") & col("quantity").isNull(), 1)
       .otherwise(col("quantity"))
   )
   ```
3. **dbt Tests:** Automated checks to catch future issues

**Result:** 100% data quality, zero revenue miscalculations

**Lesson:** Validate data at EVERY layer. Trust nothing.

---

### Challenge 2: Connection Pool Exhaustion

**Problem:** Dashboard crashed after 50+ queries (PostgreSQL max_connections hit).

**Root Cause:** Each Streamlit refresh created new connections without closing them.

**Solution:** Implemented connection pooling:
```python
self.connection_pool = psycopg2.pool.SimpleConnectionPool(1, 5, ...)
```

**Result:** 40% reduction in connection overhead, zero crashes.

**Lesson:** Always use connection pooling for database-heavy apps.

---

### Challenge 3: Checkpoint Directory Conflicts

**Problem:** PySpark consumer failed to restart after crashes.

**Root Cause:** Stale checkpoint files from previous runs.

**Solution:** Added checkpoint cleanup:
```python
.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
```

**Lesson:** Design for failure recovery from day one.

---

## 🔮 Future Enhancements

### Phase 1: Cloud Migration (Next Project)
- [ ] Deploy to AWS (S3 + Glue + Redshift)
- [ ] Use Terraform for infrastructure as code
- [ ] Implement CI/CD with GitHub Actions
- [ ] Add AWS Lambda for serverless processing

### Phase 2: Advanced Analytics
- [ ] Machine learning for churn prediction
- [ ] Real-time anomaly detection (sudden traffic spikes)
- [ ] A/B testing framework
- [ ] Cohort analysis (retention curves)

### Phase 3: Production Hardening
- [ ] Add Apache Airflow for orchestration
- [ ] Implement data lineage tracking
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Add alerting (Slack/email notifications)
- [ ] Schema evolution with Avro/Protobuf

### Phase 4: Scale Testing
- [ ] Stress test with 10,000 events/min
- [ ] Implement backpressure handling
- [ ] Add Kafka partitioning strategy
- [ ] Optimize PostgreSQL indexes

---

## 🤝 Contributing

Contributions welcome! This project is part of my learning journey, and I'm happy to collaborate.

**Areas for Improvement:**
- Additional data quality tests
- More sophisticated ML models
- Alternative visualization tools (Grafana, Superset)
- Performance optimizations

**How to Contribute:**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📫 Contact

**Abdulhafiz Yusuf**

- **LinkedIn:** [linkedin.com/in/abdulhafizyusuf](www.linkedin.com/in/abdulhafiz-yusuf-883a23271)
- **Email:** talk2abdulhafiz@gmail.com

**Project Status:** ✅ Complete (January 2026)  
**Job Search:** Open to Data Engineering roles.

---

## 🙏 Acknowledgments

- **Inspiration:** [Yusuf Ganiyu](https://www.youtube.com/@CodeWithYu) for DE career advice
- **Learning Resources:** 
  - [dbt Documentation](https://docs.getdbt.com/)
  - [PySpark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
  - [Kafka Documentation](https://kafka.apache.org/documentation/)

---

<div align="center">

**⭐ If this project helped you, please star the repository! ⭐**

Built with ❤️ by Abdulhafiz Yusuf | January 2026

</div>
```

