import csv
import time
from create_tables import create_tables
from weather_utils import fetch_current_weather
from hourly_weather import insert_hourly_weather
from daily_weather import aggregated_daily_weather
from global_weather import aggregated_global_weather

CSV_FILE="indian_cities.csv"

def run_etl():
    create_tables()
    
    with open(CSV_FILE, newline='',encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            city = row['city']
            lat = row['lat']
            lon = row['lon']

            try:
                # Feature needed to be taken [‘date’, ‘time’, ‘temperature’, ‘condition’, ‘humidity’, ‘location_name’, ‘region’, ‘country’, ‘latitude’, ‘longitude’, ‘local_time’]
               
                data = fetch_current_weather(lat, lon)
                print(data)
                temp = data['main']['temp']
                humidity = data['main']['humidity']
                weather_desc = data["weather"][0]["description"]
                country = data["sys"]["country"]
                dt = data["dt"]
                timezone_offset=data["timezone"]

                insert_hourly_weather(city,lat,lon, temp, humidity,weather_desc,country,dt,timezone_offset)
                print(f"Stored {city}->{temp} celsius, {humidity}%, {weather_desc}")
            
            except Exception as e:
                print(f"Error fetching {city}:{e}")
    
    aggregated_daily_weather()
    aggregated_global_weather()


if __name__ == "__main__":
    count = 0
    while True:
        run_etl()
        print(f"Loop {count} Started.")
        
        for remaning in range(300,0,-1):
            mins,sec = divmod(remaning,60)
            print(f"Sleeping:{mins:02d}:{sec:02d} remaning",end="\r")
            time.sleep(1)
        
        print(f"Hour {count} Completed.")
        count+=1
        
        
        