import json
import psycopg2
from kafka import KafkaConsumer
from sklearn.linear_model import SGDClassifier
import numpy as np
import joblib
import os
from datetime import datetime

# ------------------------------------------------------
# Load or initialize model
# ------------------------------------------------------
MODEL_PATH = "completion_model.pkl"

if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
else:
    # Features: distance, fare, hour
    model = SGDClassifier(loss="log_loss")
    model.partial_fit([[0,0,0]], [0], classes=[0,1])

# ------------------------------------------------------
# EWMA anomaly detection
# ------------------------------------------------------
ema_mean = 0
ema_std = 1
seen = 0

def anomaly_score(fare):
    global ema_mean, ema_std, seen
    seen += 1
    alpha = 0.1

    if seen == 1:
        ema_mean = fare
        return 0

    prev_mean = ema_mean
    ema_mean = alpha * fare + (1 - alpha) * ema_mean
    ema_std = np.sqrt(alpha * (fare - prev_mean)**2 + (1 - alpha) * ema_std**2)

    return abs(fare - ema_mean) / (ema_std + 1e-5)

# ------------------------------------------------------
# Feature builder
# ------------------------------------------------------
def build_features(trip):
    hour = datetime.fromisoformat(trip["start_time"]).hour
    return np.array([
        trip["distance_km"],
        trip["fare"],
        hour,
    ]).reshape(1, -1)

# ------------------------------------------------------
# PostgreSQL connection
# ------------------------------------------------------
conn = psycopg2.connect(
    dbname="kafka_db",
    user="kafka_user",
    password="kafka_password",
    host="localhost",
    port="5432",
)
conn.autocommit = True
cur = conn.cursor()

# NEW TABLE
cur.execute("""
CREATE TABLE IF NOT EXISTS trips_ml (
    trip_id VARCHAR(50) PRIMARY KEY,
    status VARCHAR(50),
    city VARCHAR(50),
    distance_km NUMERIC(10,2),
    fare NUMERIC(10,2),
    start_time TIMESTAMP,
    completion_prob NUMERIC(5,4),
    anomaly BOOLEAN
);
""")

# ------------------------------------------------------
# Kafka Consumer
# ------------------------------------------------------
consumer = KafkaConsumer(
    "trips",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("[Consumer] Listening for trip events...")

for msg in consumer:
    trip = msg.value

    # Build features
    X = build_features(trip)

    # Predict probability
    prob = float(model.predict_proba(X)[0][1])

    # Train on outcome
    y = 1 if trip["status"] == "Completed" else 0
    model.partial_fit(X, [y])
    joblib.dump(model, MODEL_PATH)

    # Anomaly detection
    z = anomaly_score(trip["fare"])
    is_anomaly = True if z > 1.5 else False

    # Insert into the NEW table
    cur.execute("""
        INSERT INTO trips_ml (trip_id, status, city, distance_km, fare, start_time,
                              completion_prob, anomaly)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (trip_id) DO UPDATE
        SET completion_prob = EXCLUDED.completion_prob,
            anomaly = EXCLUDED.anomaly;
    """, (
        trip["trip_id"],
        trip["status"],
        trip["city"],
        trip["distance_km"],
        trip["fare"],
        trip["start_time"],
        prob,
        is_anomaly
    ))

    print(f"[Consumer] Trip {trip['trip_id']} | prob={prob:.3f} | anomaly={is_anomaly}")
