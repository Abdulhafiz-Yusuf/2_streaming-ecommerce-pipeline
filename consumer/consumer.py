"""
PySpark Structured Streaming Consumer
Reads clickstream events from Kafka → Writes to PostgreSQL bronze layer

Processing Flow:
1. Read JSON messages from Kafka
2. Parse and validate schema
3. Apply data quality checks
4. Write to PostgreSQL in micro-batches
5. Handle failures gracefully
"""

# 1. Standard library imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import (
    DecimalType, StructType, StructField, 
    StringType, IntegerType, TimestampType
)
from dotenv import load_dotenv
import os

# 2. Load environment variables
load_dotenv()

# 3. Configurations with default values
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-clickstream")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432") 
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "streaming_db")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/ecommerce")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "clickstream_events")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


# 4. Create Spark session
def create_spark_session():
    """
    Initialize Spark Session with required configurations.
    
    Why these settings:
    - spark.jars.packages: Auto-download PostgreSQL + Kafka drivers
    - spark.sql.streaming.schemaInference: Disabled for performance
    - spark.sql.adaptive.enabled: Optimize query execution
    - forceDeleteTempCheckpointLocation: Clean stale checkpoints
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName("EcommerceClickstreamConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.extraJavaOptions", 
                "-Djava.security.auth.login.config=/dev/null") \
        .getOrCreate()
    
    return spark


# 5. Define event schema
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
    schema = StructType([
        StructField("event_id", StringType(), False),  # NOT NULL
        StructField("event_timestamp", StringType(), False),
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
    return schema


# 6. Read and parse streaming data from Kafka
def read_streaming_data(spark, schema):
    """
    Create streaming DataFrame from Kafka topic.
    
    Kafka message structure:
    - key: (optional) partition key
    - value: JSON string (our event data)
    - timestamp: Kafka broker timestamp
    
    Args:
        spark: SparkSession
        schema: Expected event schema
        
    Returns:
        DataFrame: Streaming DataFrame with parsed and validated events
    """
    # Read raw stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka value column
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    
    # ✅ DATA QUALITY CHECK: Fix null quantities for purchase/cart events
    # Defense in depth: Even if producer fixed it, validate here
    parsed_df = parsed_df.withColumn(
        "quantity",
        when(
            (col("event_type").isin("add_to_cart", "purchase")) & 
            (col("quantity").isNull() | (col("quantity") <= 0)),
            1  # Default to 1 if invalid
        ).otherwise(col("quantity"))
    )
    
    # Add ingestion timestamp
    parsed_df = parsed_df.withColumn("ingestion_timestamp", current_timestamp())

    return parsed_df


# 7. Write streaming data to PostgreSQL
def write_streaming_to_postgres(df, epoch_id):
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


# 8. Main function to run the consumer
def main():
    """
    Main streaming pipeline execution.
    Reads from Kafka, processes events, writes to PostgreSQL.
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
    
    # Define schema and create streaming DataFrame
    schema = define_event_schema()
    streaming_df = read_streaming_data(spark, schema)
    
    # Start streaming query
    query = streaming_df.writeStream \
        .foreachBatch(write_streaming_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="5 seconds") \
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
        print("✅ Spark session stopped")


if __name__ == "__main__":
    main()