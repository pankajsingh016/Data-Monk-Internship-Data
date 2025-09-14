# ===== consumer/analytics.py =====
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

class WeatherAnalytics:
    @staticmethod
    def calculate_city_averages(df: DataFrame) -> DataFrame:
        """Calculate average temperature and humidity by city"""
        return df.groupBy("city", "country") \
                 .agg(
                     avg("temp").alias("avg_temp"),
                     avg("humidity").alias("avg_humidity"),
                     max("temp").alias("max_temp"),
                     min("temp").alias("min_temp"),
                     count("*").alias("record_count"),
                     max("utc_timestamp").alias("last_updated")
                 )
    
    @staticmethod
    def detect_temperature_anomalies(df: DataFrame, threshold: float = 2.0) -> DataFrame:
        """Detect temperature anomalies using z-score"""
        # Calculate mean and std for each city
        city_stats = df.groupBy("city") \
                      .agg(avg("temp").alias("mean_temp"),
                           stddev("temp").alias("std_temp"))
        
        # Join back with original data and calculate z-score
        df_with_stats = df.join(city_stats, "city")
        
        return df_with_stats.withColumn(
            "temp_zscore",
            (col("temp") - col("mean_temp")) / col("std_temp")
        ).filter(abs(col("temp_zscore")) > threshold)
    
    @staticmethod
    def calculate_weather_trends(df: DataFrame) -> DataFrame:
        """Calculate weather trends by hour"""
        return df.withColumn("hour", hour("utc_timestamp")) \
                 .groupBy("hour") \
                 .agg(
                     avg("temp").alias("avg_temp_by_hour"),
                     avg("humidity").alias("avg_humidity_by_hour"),
                     count("*").alias("records_per_hour")
                 ) \
                 .orderBy("hour")
    
    @staticmethod
    def top_cities_by_metric(df: DataFrame, metric: str = "temp", top_n: int = 10) -> DataFrame:
        """Get top N cities by a specific metric"""
        return df.groupBy("city", "country") \
                 .agg(avg(metric).alias(f"avg_{metric}")) \
                 .orderBy(col(f"avg_{metric}").desc()) \
                 .limit(top_n)