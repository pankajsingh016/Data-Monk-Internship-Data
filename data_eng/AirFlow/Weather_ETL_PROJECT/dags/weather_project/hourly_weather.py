import sqlite3
from datetime import datetime, timezone, timedelta

DB_FILE = "data.db"


def insert_hourly_weather(city,lat,lon,temp,humidity,weather_desc,country,dt,timezone_offset):
    
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    

    utc_datetime = datetime.fromtimestamp(dt,tz=timezone.utc)
    utc_timestamp = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    offset_delta = timedelta(seconds=timezone_offset)
    local_datetime = utc_datetime + offset_delta 
    local_timestamp = local_datetime.strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
                INSERT OR IGNORE INTO weather (city, lat, lon, temp, humidity, weather, utc_timestamp,country,local_timestamp)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,(city,lat,lon,temp,humidity,weather_desc,utc_timestamp,country,local_timestamp))
    
    conn.commit()
    conn.close()

