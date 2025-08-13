import sqlite3

DB_FILE="data.db"


def aggregated_global_weather():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    cur.execute("""
                INSERT INTO global_weather(city, avg_max_temp, avg_min_temp, avg_humidity,country)
                SELECT
                    city,
                    AVG(max_temp),
                    AVG(min_temp),
                    AVG(avg_humidity),
                    country 
                From daily_weather
                Group By city
                ON CONFLICT(city) DO UPDATE SET
                    avg_max_temp=excluded.avg_max_temp,
                    avg_min_temp=excluded.avg_min_temp,
                    avg_humidity=excluded.avg_humidity
                """)
    conn.commit()
    conn.close()