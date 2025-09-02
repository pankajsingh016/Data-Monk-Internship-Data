import csv
from dagster import asset, MaterializeResult, MetadataValue
from .utils.tables import create_tables
from .utils.weather_utils import fetch_current_weather
from .utils.hourly_weather import insert_hourly_weather
from .utils.daily_weather import aggregated_daily_weather
from .utils.global_weather import aggregated_global_weather

DB_FILE = "data.db"
CSV_FILE = "indian_cities.csv"

@asset
def setup_database():
    create_tables(DB_FILE)
    return MaterializeResult(metadata={"status": "Database created"})


@asset(deps=[setup_database])
def fetch_weather():
    inserted_cities = []
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["city"]
            lat = row["lat"]
            lon = row["lon"]

            try:
                data = fetch_current_weather(lat, lon)
                temp = data["main"]["temp"]
                humidity = data["main"]["humidity"]
                weather_desc = data["weather"][0]["description"]
                country = data["sys"]["country"]
                dt = data["dt"]
                tz_offset = data["timezone"]

                insert_hourly_weather(city, lat, lon, temp, humidity, weather_desc, country, dt, tz_offset, DB_FILE)
                inserted_cities.append(f"{city}: {temp}°C, {humidity}% {weather_desc}")

            except Exception as e:
                inserted_cities.append(f"{city}: ERROR {e}")

    return MaterializeResult(
        metadata={"cities_processed": MetadataValue.md("\n".join(inserted_cities))}
    )


@asset(deps=[fetch_weather])
def fetch_daily_weather():
    aggregated_daily_weather(DB_FILE)
    return MaterializeResult(metadata={"status": "Daily weather aggregation complete"})

@asset(deps=[fetch_daily_weather])
def global_weather():
    aggregated_global_weather(DB_FILE)
    return MaterializeResult(metadata={"status": "Global weather aggregation complete"})
