from clickhouse_driver import Client
import time
import random

client = Client('localhost')

i = 0
while True:
    batch = [(i + j, random.random()) for j in range(1000)]  # batch insert 1000 rows
    client.execute(
        "INSERT INTO workload_test.live_data (id, value) VALUES",
        batch
    )
    i += 1000
    print(f"Inserted up to id={i}")
    time.sleep(0.2)  # control insert speed
