import sqlite3

DB_FILE = "data.db"


def aggregated_daily_weather():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    cur.execute("""
                Insert or Replace Into daily_weather(city,date,max_temp,min_temp, avg_humidity) 
                SELECT city,Date(timestamp) as date, Max(temp), Min(temp), AVG(humidity) From weather 
                Group By city, Date(timestamp)
                """)
    conn.commit()
    conn.close()