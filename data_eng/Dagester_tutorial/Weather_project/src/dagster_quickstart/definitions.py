from dagster import Definitions, load_assets_from_modules
from . import assets, schedules 

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=[schedules.weather_schedule],
)
