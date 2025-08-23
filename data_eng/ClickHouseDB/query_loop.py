from clickhouse_driver import Client
import time

client = Client('localhost')

while True:
    result = client.execute("""
        SELECT
            count() AS rows,
            avg(value) AS avg_val,
            max(created_at) AS latest
        FROM workload_test.live_data
    """)
    print(result)
    time.sleep(0.5)
