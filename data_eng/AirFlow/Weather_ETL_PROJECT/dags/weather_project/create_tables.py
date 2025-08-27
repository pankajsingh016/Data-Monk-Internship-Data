import sqlite3

SCHEMA = {
    'weather': '''
        CREATE TABLE IF NOT EXISTS weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            lat REAL,
            lon REAL,
            temp REAL,
            humidity INTEGER,
            weather TEXT,
            utc_timestamp TEXT NOT NULL,
            country TEXT,
            local_timestamp TEXT NOT NULL,
            UNIQUE(city, utc_timestamp, local_timestamp)
        );
    ''',
    'daily_weather': '''
        CREATE TABLE IF NOT EXISTS daily_weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            date TEXT NOT NULL,
            max_temp REAL,
            min_temp REAL,
            avg_humidity REAL,
            country TEXT,
            UNIQUE(city, date)
        );
    ''',
    'global_weather': '''
        CREATE TABLE IF NOT EXISTS global_weather(
            city TEXT PRIMARY KEY,
            avg_max_temp REAL,
            avg_min_temp REAL,
            avg_humidity REAL,
            last_updated TEXT,
            country TEXT
        );
    '''
}

def create_tables(db_path='./weaither_project/data.db'):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for q in SCHEMA.values():
        cur.execute(q)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_tables()
    print('Tables created (data.db)')
