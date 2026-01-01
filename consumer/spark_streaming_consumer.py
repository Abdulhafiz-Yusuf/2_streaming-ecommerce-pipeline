# 1. standard library imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, IntegerType, TimestampType
from dotenv import load_dotenv
import os

# 2. load environment variables
load_dotenv()

# 3. configurations - ADD DEFAULT VALUES
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

# 4. create spark session - FIXED WITH RETURN STATEMENT
def create_spark_session():
    # Option 1: Use Maven coordinates (recommended)
    spark = SparkSession.builder \
        .appName("SparkStreamingConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.extraJavaOptions", 
                "-Djava.security.auth.login.config=/dev/null") \
        .getOrCreate()
    
    # IMPORTANT: Return the spark session!
    return spark

# 5. define event schema
def define_event_schema():
    schema = StructType([
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
    return schema

# 6. read and parse streaming data from source
def read_streaming_data(spark, schema):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    parsed_df = parsed_df.withColumn("ingestion_timestamp", current_timestamp())

    return parsed_df

# 7. write streaming data to postgres
def write_streaming_to_postgres(df, epoch_id):
    try:
        row_count = df.count()
        
        if row_count > 0:
            print(f"Writing batch {epoch_id} with {row_count} events to Postgres")
            
            # write to Postgres
            df.write.format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"Successfully wrote batch {epoch_id} to Postgres")
        else:
            print(f"No data to write for batch {epoch_id}")

    except Exception as e:
        print(f"Error writing batch {epoch_id} to Postgres: {str(e)}")
        raise 

# 8. main function to run the consumer
def main():
    """
    Main function to run the Spark Streaming consumer that reads from Kafka,
    processes the data, and writes to Postgres.
    """
    print("="*70)
    print("Starting Spark Streaming Consumer...")
    print("="*70)
    print(f"KAFKA_BROKER: {KAFKA_BROKER}")
    print(f"TOPIC: {KAFKA_TOPIC}")
    print(f"POSTGRES_URL: {POSTGRES_URL}")
    print(f"POSTGRES_TABLE: {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")
    print(f"CHECKPOINT_DIR: {CHECKPOINT_DIR}")
    print('='*70)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 9. define schema and read streaming data
    schema = define_event_schema()
    streaming_df = read_streaming_data(spark, schema)
    
    query = streaming_df.writeStream \
        .foreachBatch(write_streaming_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping the streaming query...")
        query.stop()
        print("Streaming query stopped.")
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()