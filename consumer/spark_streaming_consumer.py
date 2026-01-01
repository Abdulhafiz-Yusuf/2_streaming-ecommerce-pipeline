"""
PySpark Structured Streaming Consumer
Reads clickstream events from Kafka → Writes to PostgreSQL bronze layer

Processing Flow:
1. Read JSON messages from Kafka
2. Parse and validate schema
3. Write to PostgreSQL in micro-batches
4. Handle failures gracefully
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, DecimalType, IntegerType
)
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR")

# PostgreSQL JDBC URL
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def create_spark_session():
    """
    Initialize Spark Session with required configurations.
    
    Why these settings:
    - spark.jars: Load PostgreSQL + Kafka drivers
    - spark.sql.streaming.schemaInference: Disable for performance
    - spark.sql.adaptive.enabled: Optimize query execution
    
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName("EcommerceClickstreamConsumer") \
        .config("spark.jars", 
                "../jars/postgresql-42.7.1.jar,"
                "../jars/spark-sql-kafka-0-10_2.12-3.5.0.jar") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def define_event_schema():
    """
    Define schema for incoming Kafka messages.
    
    Why explicit schema?
    - Streaming can't infer schema (data arrives continuously)
    - Validation: Reject malformed events early
    - Performance: No runtime schema inference overhead
    
    Returns:
        StructType: Schema matching producer events
    """
    return StructType([
        StructField("event_id", StringType(), False),  # NOT NULL
        StructField("event_timestamp", StringType(), False),  # Will convert to timestamp
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),  # Nullable
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_price", DecimalType(10, 2), True),
        StructField("quantity", IntegerType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True)
    ])


def read_from_kafka(spark, schema):
    """
    Create streaming DataFrame from Kafka topic.
    
    Kafka message structure:
    - key: (optional) partition key
    - value: JSON string (our event data)
    - timestamp: Kafka broker timestamp
    - partition: Which partition this came from
    - offset: Position in partition
    
    Args:
        spark: SparkSession
        schema: Expected event schema
        
    Returns:
        DataFrame: Streaming DataFrame with parsed events
    """
    # Read raw stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Kafka gives us binary data in 'value' column
    # Cast to string, then parse JSON
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")
    
    # Convert string timestamp to proper Timestamp type
    parsed_df = parsed_df.withColumn(
        "event_timestamp",
        col("event_timestamp").cast(TimestampType())
    )
    
    # Add ingestion timestamp (when we received it)
    parsed_df = parsed_df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    
    return parsed_df


def write_to_postgres(df, epoch_id):
    """
    Batch write function (called for each micro-batch).
    
    Why function instead of direct write?
    - Custom error handling per batch
    - Ability to add batch-level metrics
    - More control over transaction boundaries
    
    Args:
        df: DataFrame for this micro-batch
        epoch_id: Unique batch ID (for logging)
    """
    try:
        row_count = df.count()
        
        if row_count > 0:
            print(f"📦 Processing batch {epoch_id}: {row_count} events")
            
            # Write to PostgreSQL
            df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"✅ Batch {epoch_id} written successfully")
        else:
            print(f"⏭️  Batch {epoch_id}: No new events")
            
    except Exception as e:
        print(f"❌ Error writing batch {epoch_id}: {str(e)}")
        # In production: Log to monitoring system, send alert
        raise  # Re-raise to trigger Spark's retry mechanism


def main():
    """
    Main streaming pipeline execution.
    """
    print("=" * 70)
    print("🚀 Starting PySpark Streaming Consumer")
    print("=" * 70)
    print(f"📡 Kafka Broker: {KAFKA_BROKER}")
    print(f"📨 Topic: {KAFKA_TOPIC}")
    print(f"🗄️  PostgreSQL: {POSTGRES_URL}")
    print(f"📊 Target Table: {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")
    print(f"💾 Checkpoint: {CHECKPOINT_DIR}")
    print("=" * 70)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce console noise
    
    # Define schema
    event_schema = define_event_schema()
    
    # Create streaming DataFrame
    streaming_df = read_from_kafka(spark, event_schema)
    
    # Start streaming query
    query = streaming_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("\n✨ Streaming query started successfully!")
    print("📊 Processing events every 5 seconds...")
    print("⏹️  Press Ctrl+C to stop\n")
    
    # Keep running until interrupted
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping streaming query...")
        query.stop()
        print("✅ Consumer stopped gracefully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()