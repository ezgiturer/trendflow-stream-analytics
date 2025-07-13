# spark/streaming-processor.py
# TrendFlow Spark Streaming Processor for Real-Time E-Commerce Analytics

import logging
import os
import json # Ensure json is imported for value_serializer if writing to Redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, avg, max as spark_max,
    when, desc, current_timestamp, to_timestamp, round as spark_round,
    collect_list, array_distinct, size, concat_ws, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the schema for incoming events (must match producer's output)
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("revenue", DoubleType(), True), # Producer generates this as Double
    StructField("user_agent", StringType(), True),
    StructField("source", StringType(), True)
])

class TrendFlowStreamProcessor:
    def __init__(self):
        # Configuration from environment variables
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.kafka_input_topic = os.getenv("KAFKA_INPUT_TOPIC", "trendflow-events")
        
        # Output topics for different analytics (used by Redis for organization)
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_password = os.getenv("REDIS_PASSWORD", None) # If Redis requires password
        
        # Checkpoint location for Spark Structured Streaming
        # Using /tmp is common for ephemeral containers, but for production,
        # consider a persistent volume or cloud storage (e.g., S3, HDFS)
        self.checkpoint_location = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints")

        # Initialize Spark Session
        logger.info("Initializing Spark Session...")
        self.spark = SparkSession.builder \
            .appName("TrendFlow-StreamProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark Session initialized for TrendFlow processing.")
        logger.info(f"Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        logger.info(f"Kafka Input Topic: {self.kafka_input_topic}")
        logger.info(f"Redis Host: {self.redis_host}:{self.redis_port}")
        logger.info(f"Spark Checkpoint Location: {self.checkpoint_location}")
    
    def create_kafka_stream(self):
        """Create Kafka stream source DataFrame."""
        logger.info(f"Connecting to Kafka topic: {self.kafka_input_topic}")
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_events(self, kafka_df):
        """Parse JSON events from Kafka and add derived columns."""
        logger.info("Parsing incoming Kafka events...")
        return kafka_df \
            .select(
                from_json(col("value").cast("string"), event_schema).alias("event")
            ) \
            .select("event.*") \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0)) \
            .withColumn("is_click", when(col("event_type") == "click", 1).otherwise(0)) \
            .withColumn("revenue_filled", when(col("revenue").isNull(), lit(0.0)).otherwise(col("revenue"))) # Ensure type consistency for nulls

    def create_trending_products_stream(self, events_df):
        """Calculates trending products based on events in sliding windows."""
        logger.info("Creating trending products stream...")
        # Use a longer window and slide duration for trending, e.g., 30 min window, 5 min slide
        # Watermark allows for late data but cleans up state
        return events_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window(col("event_timestamp"), "10 minutes", "1 minute"), # 10 min window, sliding every 1 min
                col("product_id"),
                col("product_name"),
                col("category")
            ) \
            .agg(
                count("*").alias("total_events"),
                spark_sum("is_click").alias("clicks"),
                spark_sum("is_purchase").alias("purchases"),
                spark_sum("revenue_filled").alias("revenue"),
                spark_sum("quantity").alias("total_quantity"),
                # Collect distinct user IDs to count unique users
                size(array_distinct(collect_list("user_id"))).alias("unique_users")
            ) \
            .withColumn("conversion_rate",
                        when(col("clicks") > 0,
                             spark_round(col("purchases") / col("clicks") * 100, 2))
                        .otherwise(0)) \
            .withColumn("avg_revenue_per_event",
                        when(col("total_events") > 0,
                             spark_round(col("revenue") / col("total_events"), 2))
                        .otherwise(0)) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("product_id"),
                col("product_name"),
                col("category"),
                col("total_events"),
                col("clicks"),
                col("purchases"),
                col("revenue"),
                col("total_quantity"),
                col("unique_users"),
                col("conversion_rate"),
                col("avg_revenue_per_event"),
                current_timestamp().alias("processed_at")
            )

    def create_category_analytics_stream(self, events_df):
        """Calculates category-level analytics in sliding windows."""
        logger.info("Creating category analytics stream...")
        return events_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window(col("event_timestamp"), "10 minutes", "1 minute"),
                col("category")
            ) \
            .agg(
                count("*").alias("total_events"),
                spark_sum("is_click").alias("clicks"),
                spark_sum("is_purchase").alias("purchases"),
                spark_sum("revenue_filled").alias("revenue"),
                avg("price").alias("avg_product_price_in_category"), # Avg price of products *involved* in events
                spark_max("price").alias("max_product_price_in_category"),
                spark_sum("quantity").alias("total_quantity_sold")
            ) \
            .withColumn("conversion_rate",
                        when(col("clicks") > 0,
                             spark_round(col("purchases") / col("clicks") * 100, 2))
                        .otherwise(0)) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("category"),
                col("total_events"),
                col("clicks"),
                col("purchases"),
                col("revenue"),
                spark_round(col("avg_product_price_in_category"), 2).alias("avg_product_price"),
                col("max_product_price_in_category"),
                col("total_quantity_sold"),
                col("conversion_rate"),
                current_timestamp().alias("processed_at")
            )
    
    def create_user_behavior_stream(self, events_df):
        """Analyzes user behavior patterns by user_agent and source."""
        logger.info("Creating user behavior stream...")
        return events_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window(col("event_timestamp"), "10 minutes", "1 minute"),
                col("user_agent"),
                col("source")
            ) \
            .agg(
                count("*").alias("total_events"),
                spark_sum("is_click").alias("clicks"),
                spark_sum("is_purchase").alias("purchases"),
                spark_sum("revenue_filled").alias("revenue"),
                size(array_distinct(collect_list("user_id"))).alias("unique_users_affected")
            ) \
            .withColumn("conversion_rate",
                        when(col("clicks") > 0,
                             spark_round(col("purchases") / col("clicks") * 100, 2))
                        .otherwise(0)) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_agent"),
                col("source"),
                col("total_events"),
                col("clicks"),
                col("purchases"),
                col("revenue"),
                col("unique_users_affected"),
                col("conversion_rate"),
                current_timestamp().alias("processed_at")
            )

    def write_to_console(self, df, query_name, output_mode="update", trigger_interval="30 seconds"):
        """Write stream to console for debugging."""
        logger.info(f"Starting console output for '{query_name}'...")
        return df.writeStream \
            .queryName(query_name) \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime=trigger_interval) \
            .start()
            
    def write_to_redis(self, df, key_prefix: str, output_mode="update", trigger_interval="30 seconds"):
        """
        Write stream to Redis using foreachBatch to update keys.
        Each row in the DataFrame will be stored as a JSON string in Redis.
        The key will be constructed from a prefix and relevant columns.
        """
        logger.info(f"Starting Redis sink for key prefix '{key_prefix}'...")

        def process_batch(batch_df, batch_id):
            # Convert DataFrame to RDD of dictionaries, then to JSON strings
            rows = batch_df.toJSON().collect()
            if not rows:
                return # Skip if batch is empty

            try:
                import redis
                r = redis.Redis(host=self.redis_host, port=self.redis_port, password=self.redis_password, decode_responses=True)
                
                # Use a pipeline for efficient bulk operations
                pipe = r.pipeline()
                
                for row_json_str in rows:
                    row_dict = json.loads(row_json_str)
                    
                    # Create a unique key for each entry based on the type of aggregate
                    # Example: "trending_products:window_start_end:product_id"
                    # For simplicity, let's use a generic key for now for each aggregation type
                    # The Streamlit app will fetch the latest data for each 'type'
                    
                    # Determine the specific Redis key based on the type of aggregation
                    redis_key = ""
                    if key_prefix == "trending_products":
                         # Key for trending products: e.g., "trending_products:2025-07-12T16:00:00_SKU_iphone_15"
                        window_start_str = row_dict.get("window_start", "").replace(":", "_").replace(".", "_")
                        product_id = row_dict.get("product_id", "unknown_product")
                        redis_key = f"{key_prefix}:{window_start_str}_{product_id}"
                        # You might also want to store a fixed key that holds the *latest* aggregated result
                        # For example, a ZSET of top N products or a single HASH for overall metrics
                        # For now, let's just store individual windows
                    elif key_prefix == "category_analytics":
                        window_start_str = row_dict.get("window_start", "").replace(":", "_").replace(".", "_")
                        category = row_dict.get("category", "unknown_category")
                        redis_key = f"{key_prefix}:{window_start_str}_{category}"
                    elif key_prefix == "user_behavior":
                        window_start_str = row_dict.get("window_start", "").replace(":", "_").replace(".", "_")
                        user_agent = row_dict.get("user_agent", "unknown_ua")
                        source = row_dict.get("source", "unknown_source")
                        redis_key = f"{key_prefix}:{window_start_str}_{user_agent}_{source}"
                    
                    # Store the JSON string in Redis
                    if redis_key:
                        pipe.set(redis_key, row_json_str)
                        # Optional: Set an expiration for the keys if you only care about recent windows
                        pipe.expire(redis_key, 60 * 60) # Expire after 1 hour (adjust as needed)

                pipe.execute()
                logger.debug(f"Batch {batch_id} written to Redis for {key_prefix}")
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to Redis for {key_prefix}: {e}")
                # In a real system, you might want to log this error to a monitoring system
                # or send it to a dead-letter queue.

        return df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .start()

    def start_processing(self):
        """Starts all streaming processes (Kafka to Spark to Redis/Console)."""
        logger.info("Starting TrendFlow stream processing...")

        # Create base stream from Kafka
        kafka_stream = self.create_kafka_stream()
        events_df = self.parse_events(kafka_stream)

        # Create different analytics streams
        trending_products = self.create_trending_products_stream(events_df)
        category_analytics = self.create_category_analytics_stream(events_df)
        user_behavior = self.create_user_behavior_stream(events_df)

        # Define the streaming queries
        queries = []

        # --- Console Outputs (for local debugging/monitoring) ---
        # REMOVE .orderBy().limit() from console outputs.
        # The sorting for "trending" should happen when reading from Redis in the Streamlit app.
        queries.append(self.write_to_console(
            trending_products, # Changed: Removed .orderBy().limit(5)
            query_name="trending_products_console",
            trigger_interval="30 seconds"
        ))

        queries.append(self.write_to_console(
            category_analytics, # Changed: Removed .orderBy().limit(5)
            query_name="category_analytics_console",
            trigger_interval="30 seconds"
        ))

        queries.append(self.write_to_console(
            user_behavior, # Changed: Removed .orderBy().limit(5)
            query_name="user_behavior_console",
            trigger_interval="30 seconds"
        ))

        # --- Redis Sinks (for Streamlit Dashboard) ---
        queries.append(self.write_to_redis(
            trending_products,
            key_prefix="trending_products",
            output_mode="update", # Update mode is appropriate for aggregations
            trigger_interval="30 seconds" # Update Redis frequently
        ))

        queries.append(self.write_to_redis(
            category_analytics,
            key_prefix="category_analytics",
            output_mode="update",
            trigger_interval="30 seconds"
        ))

        queries.append(self.write_to_redis(
            user_behavior,
            key_prefix="user_behavior",
            output_mode="update",
            trigger_interval="30 seconds"
        ))

        logger.info("All streaming queries started. Waiting for termination signal (Ctrl+C)...")

        # Wait for all queries to terminate
        try:
            for query in queries:
                query.awaitTermination() # This will block until a query terminates
        except KeyboardInterrupt:
            logger.info("Stopping stream processing due to KeyboardInterrupt...")
        except Exception as e:
            logger.error(f"An error occurred during stream processing: {e}", exc_info=True)
        finally:
            logger.info("Stopping Spark Session...")
            self.spark.stop()
            logger.info("Stream processing stopped.")

def main():
    """Main function to run the Spark Stream Processor."""
    processor = TrendFlowStreamProcessor()
    processor.start_processing()

if __name__ == "__main__":
    main()