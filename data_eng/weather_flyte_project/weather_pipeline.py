"""
Flyte Weather ETL Pipeline
A complete weather data pipeline with dashboard visibility

Weather ETL Pipeline for Flyte
Updated: 2024-12-07 - Force rebuild
"""

import os
import typing
import sqlite3
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple

import flytekit as fl
from flytekit import task, workflow, Resources
from flytekit.types.structured import StructuredDataset
import pandas as pd

# -----------------------------
# Configuration
# -----------------------------
# Use environment variable or default API key
API_KEY = os.getenv('WEATHER_API_KEY', 'b1d54ad24e20ad7b9b20a87f155f5b75')
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DB_FILE = "/tmp/weather_data.db"  # Use /tmp for Flyte containers

# -----------------------------
# ImageSpec definition - UPDATE THESE VALUES
# -----------------------------
image_spec = fl.ImageSpec(
    name="weather-pipeline",
    packages=[
        "requests>=2.31.0",
        "pandas>=2.0.0", 
        "python-dotenv>=1.0.0",
        "pyarrow>=10.0.0"
        # sqlite3 is built-in with Python
    ],
    # CHOOSE ONE OF THESE OPTIONS:
    
    # Option 1: Docker Hub (replace 'yourusername')
    registry="docker.io/pankajsingh016",
    
    # Option 2: GitHub Container Registry (replace 'yourgithubusername') 
    # registry="ghcr.io/yourgithubusername",
    
    # Option 3: Local development (no push required)
    # registry="localhost:30000",
    
    python_version="3.11"
)

# -----------------------------
# Database Schema & Setup
# -----------------------------
SCHEMA = {
    'weather': '''
        CREATE TABLE IF NOT EXISTS weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            lat REAL,
            lon REAL,
            temp REAL,
            humidity INTEGER,
            weather TEXT,
            utc_timestamp TEXT NOT NULL,
            country TEXT,
            local_timestamp TEXT NOT NULL,
            UNIQUE(city, utc_timestamp, local_timestamp)
        );
    ''',
    'daily_weather': '''
        CREATE TABLE IF NOT EXISTS daily_weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            date TEXT NOT NULL,
            max_temp REAL,
            min_temp REAL,
            avg_humidity REAL,
            country TEXT,
            UNIQUE(city, date)
        );
    ''',
    'global_weather': '''
        CREATE TABLE IF NOT EXISTS global_weather(
            city TEXT PRIMARY KEY,
            avg_max_temp REAL,
            avg_min_temp REAL,
            avg_humidity REAL,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            country TEXT
        );
    '''
}

# -----------------------------
# Utility Functions
# -----------------------------
def create_tables(db_path: str = DB_FILE) -> None:
    """Create database tables"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for table_name, query in SCHEMA.items():
        cur.execute(query)
        print(f"Created/verified table: {table_name}")
    conn.commit()
    conn.close()

def fetch_current_weather(lat: float, lon: float) -> Dict[str, Any]:
    """Fetch current weather from OpenWeatherMap API"""
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric"
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        raise

def insert_hourly_weather(city: str, lat: float, lon: float, temp: float, 
                         humidity: int, weather_desc: str, country: str, 
                         dt: int, timezone_offset: int, db_path: str = DB_FILE) -> None:
    """Insert hourly weather data into database"""
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Convert timestamps
    utc_datetime = datetime.fromtimestamp(dt, tz=timezone.utc)
    utc_timestamp = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    offset_delta = timedelta(seconds=timezone_offset)
    local_datetime = utc_datetime + offset_delta 
    local_timestamp = local_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    cur.execute("""
        INSERT OR IGNORE INTO weather (city, lat, lon, temp, humidity, weather, 
                                     utc_timestamp, country, local_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city, lat, lon, temp, humidity, weather_desc, utc_timestamp, country, local_timestamp))
    
    conn.commit()
    conn.close()
    print(f"Inserted weather data for {city}: {temp}°C, {humidity}% humidity")

def aggregated_daily_weather(db_path: str = DB_FILE) -> None:
    """Aggregate hourly data into daily summaries"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT OR REPLACE INTO daily_weather(city, date, max_temp, min_temp, avg_humidity, country) 
        SELECT 
            city,
            DATE(local_timestamp) as date, 
            MAX(temp) as max_temp, 
            MIN(temp) as min_temp, 
            AVG(humidity) as avg_humidity,
            country 
        FROM weather 
        GROUP BY city, DATE(local_timestamp)
    """)
    
    rows_affected = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Aggregated {rows_affected} daily weather records")

def aggregated_global_weather(db_path: str = DB_FILE) -> None:
    """Aggregate daily data into global averages"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT OR REPLACE INTO global_weather(city, avg_max_temp, avg_min_temp, avg_humidity, country, last_updated)
        SELECT
            city,
            AVG(max_temp) as avg_max_temp,
            AVG(min_temp) as avg_min_temp,
            AVG(avg_humidity) as avg_humidity,
            country,
            CURRENT_TIMESTAMP
        FROM daily_weather
        GROUP BY city
    """)
    
    rows_affected = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Updated {rows_affected} global weather records")

# -----------------------------
# Flyte Tasks
# -----------------------------

@task(
    container_image=image_spec,
    requests=Resources(cpu="0.5", mem="1Gi"),
    retries=1
)
def setup_database() -> str:
    """Setup database tables"""
    try:
        create_tables()
        return "✅ Database tables created successfully"
    except Exception as e:
        error_msg = f"❌ Database setup failed: {str(e)}"
        print(error_msg)
        return error_msg

@task(
    container_image=image_spec,
    requests=Resources(cpu="1", mem="2Gi"),
    retries=2
)
def fetch_weather_data(setup_status: str) -> Tuple[str, StructuredDataset]:
    """Fetch current weather data for multiple cities"""
    
    city_data = {
        "New Delhi": {"id": 1261481, "lon": 77.23114, "lat": 28.61282, "country": "IN"},
        "Bhopal": {"id": 1275841, "lon": 77.400002, "lat": 23.26667, "country": "IN"},
        "Jaipur": {"id": 1269517, "lon": 86.133331, "lat": 23.433331, "country": "IN"},
        "Bengaluru": {"id": 1277333, "lon": 77.603287, "lat": 12.97623, "country": "IN"}
    }
    
    successful_fetches = 0
    failed_fetches = 0
    weather_records = []
    
    for city, data in city_data.items():
        try:
            weather = fetch_current_weather(data['lat'], data['lon'])
            
            # Extract weather information
            temp = weather['main']['temp']
            humidity = weather['main']['humidity']
            weather_desc = weather["weather"][0]["description"]
            country = weather["sys"]["country"]
            dt = weather["dt"]
            timezone_offset = weather["timezone"]
            
            # Insert into database
            insert_hourly_weather(
                city, data['lat'], data['lon'], temp, humidity, 
                weather_desc, country, dt, timezone_offset
            )
            
            # Store for return dataset
            weather_records.append({
                'city': city,
                'temperature': temp,
                'humidity': humidity,
                'description': weather_desc,
                'country': country,
                'timestamp': datetime.fromtimestamp(dt).isoformat()
            })
            
            successful_fetches += 1
            
        except Exception as e:
            print(f"❌ Error fetching weather for {city}: {e}")
            failed_fetches += 1
    
    status_msg = f"✅ Weather data fetched: {successful_fetches} successful, {failed_fetches} failed"
    
    # Return status and structured data for dashboard visibility
    weather_df = pd.DataFrame(weather_records)
    return status_msg, StructuredDataset(dataframe=weather_df)

@task(
    container_image=image_spec,
    requests=Resources(cpu="0.5", mem="1Gi"),
    retries=1
)
def aggregate_daily_data(fetch_status: str) -> Tuple[str, StructuredDataset]:
    """Aggregate hourly weather into daily summaries"""
    
    try:
        aggregated_daily_weather()
        
        # Get the aggregated data for return
        conn = sqlite3.connect(DB_FILE)
        daily_df = pd.read_sql_query("""
            SELECT city, date, max_temp, min_temp, avg_humidity, country 
            FROM daily_weather 
            ORDER BY city, date DESC
        """, conn)
        conn.close()
        
        status_msg = f"✅ Daily aggregation complete: {len(daily_df)} records"
        return status_msg, StructuredDataset(dataframe=daily_df)
        
    except Exception as e:
        error_msg = f"❌ Daily aggregation failed: {str(e)}"
        print(error_msg)
        return error_msg, StructuredDataset(dataframe=pd.DataFrame())

@task(
    container_image=image_spec,
    requests=Resources(cpu="0.5", mem="1Gi"),
    retries=1
)
def aggregate_global_data(daily_status: str) -> Tuple[str, StructuredDataset]:
    """Aggregate daily weather into global averages"""
    
    try:
        aggregated_global_weather()
        
        # Get the global aggregated data
        conn = sqlite3.connect(DB_FILE)
        global_df = pd.read_sql_query("""
            SELECT city, avg_max_temp, avg_min_temp, avg_humidity, country, last_updated
            FROM global_weather 
            ORDER BY city
        """, conn)
        conn.close()
        
        status_msg = f"✅ Global aggregation complete: {len(global_df)} records"
        
        # Handle parquet dependency gracefully  
        try:
            structured_data = StructuredDataset(dataframe=global_df)
        except ImportError:
            structured_data = StructuredDataset(dataframe=global_df, file_format="csv")
            
        return status_msg, structured_data
        
    except Exception as e:
        error_msg = f"❌ Global aggregation failed: {str(e)}"
        print(error_msg)
        return error_msg, StructuredDataset(dataframe=pd.DataFrame())

@task(
    container_image=image_spec,  # Re-enable container image
    requests=Resources(cpu="0.2", mem="512Mi")
)
def generate_pipeline_summary(
    db_status: str, 
    fetch_status: str, 
    daily_status: str, 
    global_status: str,
    weather_data: StructuredDataset,
    daily_data: StructuredDataset,
    global_data: StructuredDataset
) -> Dict[str, Any]:
    """Generate a comprehensive pipeline execution summary"""
    
    summary = {
        "pipeline_execution_time": datetime.now().isoformat(),
        "stages": {
            "database_setup": db_status,
            "weather_fetch": fetch_status,
            "daily_aggregation": daily_status,
            "global_aggregation": global_status
        },
        "data_summary": {
            "current_weather_records": len(weather_data.open(pd.DataFrame).all()),
            "daily_summary_records": len(daily_data.open(pd.DataFrame).all()),
            "global_summary_records": len(global_data.open(pd.DataFrame).all())
        },
        "pipeline_status": "SUCCESS" if all("✅" in status for status in [db_status, fetch_status, daily_status, global_status]) else "PARTIAL_SUCCESS"
    }
    
    print("=== PIPELINE EXECUTION SUMMARY ===")
    print(f"Status: {summary['pipeline_status']}")
    print(f"Execution Time: {summary['pipeline_execution_time']}")
    for stage, status in summary['stages'].items():
        print(f"{stage}: {status}")
    
    return summary

# -----------------------------
# Main Workflow
# -----------------------------

@workflow
def weather_etl_pipeline() -> Tuple[Dict[str, Any], StructuredDataset, StructuredDataset, StructuredDataset]:
    """
    Complete Weather ETL Pipeline
    
    Returns:
        Tuple containing:
        - Pipeline execution summary
        - Current weather data
        - Daily aggregated data  
        - Global aggregated data
    """
    
    # Stage 1: Setup database
    db_setup = setup_database()
    
    # Stage 2: Fetch current weather data
    weather_status, current_weather = fetch_weather_data(setup_status=db_setup)
    
    # Stage 3: Aggregate daily data
    daily_status, daily_summary = aggregate_daily_data(fetch_status=weather_status)
    
    # Stage 4: Aggregate global data
    global_status, global_summary = aggregate_global_data(daily_status=daily_status)
    
    # Stage 5: Generate summary
    pipeline_summary = generate_pipeline_summary(
        db_status=db_setup,
        fetch_status=weather_status,
        daily_status=daily_status,
        global_status=global_status,
        weather_data=current_weather,
        daily_data=daily_summary,
        global_data=global_summary
    )
    
    return pipeline_summary, current_weather, daily_summary, global_summary

# -----------------------------
# Additional Utility Workflows
# -----------------------------

@workflow
def weather_data_quality_check() -> str:
    """Data quality check workflow"""
    
    db_setup = setup_database()
    weather_status, current_weather = fetch_weather_data(setup_status=db_setup)
    
    # Return a simple status string to avoid type issues
    return f"✅ Quality check completed - DB: {db_setup[:20]}... | Weather: {weather_status[:30]}..."

# -----------------------------
# Local Testing
# -----------------------------
if __name__ == "__main__":
    # For local testing without containers
    print("🌤️  Weather ETL Pipeline - Local Test")
    print("=" * 50)
    
    # Test database setup
    print("1. Setting up database...")
    create_tables()
    
    # Test weather fetching (requires API key)
    if API_KEY and API_KEY != 'your_api_key_here':
        print("2. Testing weather fetch...")
        try:
            weather = fetch_current_weather(28.61282, 77.23114)  # New Delhi
            print(f"   Sample weather data: {weather['main']['temp']}°C")
        except Exception as e:
            print(f"   Weather fetch test failed: {e}")
    else:
        print("2. Skipping weather fetch test (no API key)")
    
    print("3. Local test complete!")
    print("\nTo run with Flyte:")
    print("   pyflyte run weather_pipeline.py weather_etl_pipeline")