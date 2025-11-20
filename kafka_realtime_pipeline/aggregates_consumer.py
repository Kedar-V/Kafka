import json
import psycopg2
from kafka import KafkaConsumer

conn = psycopg2.connect(
    dbname="kafka_db",
    user="kafka_user",
    password="kafka_password",
    host="localhost",
    port="5432"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS trip_aggregates (
    city VARCHAR(100),
    total_revenue NUMERIC(10,2),
    window_end TIMESTAMP
);
""")

consumer = KafkaConsumer(
    "trip_aggregates",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="agg-consumer",
)

print("Listening to aggregates...")

for msg in consumer:
    rec = msg.value
    cur.execute("""
        INSERT INTO trip_aggregates (city, total_revenue, window_end)
        VALUES (%s, %s, %s);
    """, (rec["city"], rec["total_revenue"], rec["window_end"]))

    print("Inserted:", rec)
