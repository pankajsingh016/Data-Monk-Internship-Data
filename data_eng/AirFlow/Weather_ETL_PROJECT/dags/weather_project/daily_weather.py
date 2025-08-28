import sqlite3

# DB_FILE = "../../data.db"



def aggregated_daily_weather(DB_FILE):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    cur.execute("""
                Insert or Replace Into daily_weather(city,date,max_temp,min_temp, avg_humidity,country) 
                SELECT city,Date(local_timestamp) as date, Max(temp), Min(temp), AVG(humidity),country From weather 
                Group By city, Date(local_timestamp)
                """)
    conn.commit()
    conn.close()