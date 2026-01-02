
# Streaming E-Commerce Pipeline

A real-time data pipeline for processing e-commerce clickstream events using Apache Kafka, Apache Spark, and dbt.

## Architecture

![Architecture Diagram](docs/streaming_pipeline.jpg)

- **Producer**: Generates synthetic e-commerce events to Kafka
- **Consumer**: Spark Streaming application that processes events in real-time
- **Streaming:** Apache Kafka (Redpanda)
- **Data Warehouse**: PostgreSQL for storing raw and transformed data
- **Transformation**: dbt for building analytics models (silver and gold layers)
- **Orchestration**: Docker Compose for local development
- **Visualization:** Streamlit

## Project Structure

```
├── producer/              # Event producer (Kafka)
├── consumer/              # Spark Streaming consumer
├── dbt_ecommerce_analytics/  # dbt project (transformations)
├── docker/                # Docker Compose setup
├── refresh_dbt.sh         # dbt automation script
└── README.md
```

## Quick Start

1. **Start services**:
    ```bash
    docker-compose -f docker/docker-compose.yml up -d
    ```

2. **Run producer**:
    ```bash
    python producer/event_producer.py
    ```

3. **Run consumer**:
    ```bash
    python consumer/spark_streaming_consumer.py
    ```

4. **Refresh dbt models**:
    ```bash
    bash refresh_dbt.sh
    ```

## dbt Models

- **Silver**: Staging tables (`stg_clickstream_events`, `int_user_sessions`)
- **Gold**: Analytics marts (conversion funnel, daily metrics, product performance, user behavior)
