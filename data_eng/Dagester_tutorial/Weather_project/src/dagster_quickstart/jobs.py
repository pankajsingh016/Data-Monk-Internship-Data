from dagster import define_asset_job
from .assets import setup_database, weather, daily_weather, global_weather

# Define a job that runs all assets in dependency order
weather_job = define_asset_job(
    name="weather_job",
    selection=[
        setup_database,
        weather,
        daily_weather,
        global_weather,
    ],
)
