import sqlite3
from datetime import datetime, timezone

DB_FILE = "data.db"


def insert_hourly_weather(city,lat,lon,temp,humidity,weather_desc):
    
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
                INSERT OR IGNORE INTO weather (city, lat, lon, temp, humidity, weather, timestamp)
                VALUES (?,?,?,?,?,?,?)
                """,(city,lat,lon,temp,humidity,weather_desc,timestamp))
    
    conn.commit()
    conn.close()

