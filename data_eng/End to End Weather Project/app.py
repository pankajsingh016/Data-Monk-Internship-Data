# main.py -------> Orchestrates the ETL process
# create_tables.py ---------> Creates all database tables
# weather_utils.py ---------> Extracts weather data via API
# hourly_weather.py --------> Saves hourly records into database
# daily_weather.py ---------> Aggregates hourly to daily
# global_weather.py --------> Aggregates daily to city-wide averages
# india_cities.py ----------> List of cities to process
# data.db ------------> SQLite database file

# creating city data script.py ===> from open weather city.json fetch the city and its lat,lon 