# ===== consumer/spark_consumer.py =====
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import logging
from analytics import WeatherAnalytics
import sqlite3
from datetime import datetime

# Load environment variables
load_dotenv('config/.env')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherStreamProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.input_topic = os.getenv('KAFKA_WEATHER_TOPIC', 'weather-data')
        self.output_topic = os.getenv('KAFKA_ANALYTICS_TOPIC', 'weather-analytics')
        self.db_path = os.getenv('DB_PATH', 'data.db')
        
        # Initialize Spark Session with Kafka package
        self.spark = SparkSession.builder \
            .appName("WeatherStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for weather data
        self.weather_schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("temp", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("weather_desc", StringType(), True),
            StructField("utc_timestamp", StringType(), True),
            StructField("local_timestamp", StringType(), True),
            StructField("dt", IntegerType(), True),
            StructField("timezone_offset", IntegerType(), True)
        ])
        
        self.analytics = WeatherAnalytics()
    
    def create_database_tables(self):
        """Create SQLite database tables"""
        SCHEMA = {
            'streaming_weather': '''
                CREATE TABLE IF NOT EXISTS streaming_weather(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    lat REAL,
                    lon REAL,
                    temp REAL,
                    humidity INTEGER,
                    weather_desc TEXT,
                    utc_timestamp TEXT NOT NULL,
                    country TEXT,
                    local_timestamp TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    UNIQUE(city, utc_timestamp)
                );
            ''',
            'city_analytics': '''
                CREATE TABLE IF NOT EXISTS city_analytics(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    country TEXT,
                    avg_temp REAL,
                    avg_humidity REAL,
                    max_temp REAL,
                    min_temp REAL,
                    record_count INTEGER,
                    last_updated TEXT,
                    processed_at TEXT NOT NULL,
                    UNIQUE(city, last_updated)
                );
            '''
        }
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        for query in SCHEMA.values():
            cur.execute(query)
        conn.commit()
        conn.close()
    
    def save_to_database(self, df, table_name: str):
        """Save DataFrame to SQLite database"""
        def write_batch(batch_df, batch_id):
            try:
                # Convert to Pandas and save to SQLite
                pandas_df = batch_df.toPandas()
                if len(pandas_df) == 0:
                    return
                    
                pandas_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                conn = sqlite3.connect(self.db_path)
                pandas_df.to_sql(table_name, conn, if_exists='append', index=False)
                conn.close()
                
                logger.info(f"Saved {len(pandas_df)} records to {table_name}")
            except Exception as e:
                logger.error(f"Error saving to database: {e}")
        
        return write_batch
    
    def process_stream(self):
        """Main stream processing logic"""
        try:
            # Create database tables
            self.create_database_tables()
            
            # Read from Kafka
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON data
            weather_df = kafka_df.select(
                from_json(col("value").cast("string"), self.weather_schema).alias("data")
            ).select("data.*") \
            .withColumn("utc_timestamp", to_timestamp("utc_timestamp", "yyyy-MM-dd HH:mm:ss"))
            
            # Save raw streaming data to database
            raw_data_query = weather_df.writeStream \
                .foreachBatch(self.save_to_database(weather_df, "streaming_weather")) \
                .outputMode("append") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            # Calculate analytics on windowed data
            windowed_analytics = weather_df \
                .withWatermark("utc_timestamp", "10 minutes") \
                .groupBy(
                    window("utc_timestamp", "5 minutes", "1 minute"),
                    "city", "country"
                ) \
                .agg(
                    avg("temp").alias("avg_temp"),
                    avg("humidity").alias("avg_humidity"),
                    max("temp").alias("max_temp"),
                    min("temp").alias("min_temp"),
                    count("*").alias("record_count"),
                    max("utc_timestamp").alias("last_updated")
                ) \
                .select("city", "country", "avg_temp", "avg_humidity", 
                       "max_temp", "min_temp", "record_count", "last_updated")
            
            # Save analytics to database
            analytics_query = windowed_analytics.writeStream \
                .foreachBatch(self.save_to_database(windowed_analytics, "city_analytics")) \
                .outputMode("append") \
                .trigger(processingTime='1 minute') \
                .start()
            
            # Console output for monitoring
            console_query = weather_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            # Wait for termination
            logger.info("Stream processing started. Press Ctrl+C to stop.")
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = WeatherStreamProcessor()
    processor.process_stream()