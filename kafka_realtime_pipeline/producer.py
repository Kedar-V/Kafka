# producer.py
import time
import json
import uuid
import random
from datetime import datetime, timedelta

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

CITIES = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte",
    "Curitiba", "Salvador", "Recife", "Fortaleza"
]
STATUSES = ["Requested", "Ongoing", "Completed", "Cancelled"]
PAYMENT_METHODS = ["Credit Card", "PIX", "Debit Card", "Cash"]
DRIVER_IDS = [f"DRV-{i:03d}" for i in range(1, 51)]
RIDER_IDS = [f"RID-{i:03d}" for i in range(1, 501)]


def random_coord(base_lat, base_lng, delta=0.05):
    """Generate random lat/lng around a base point."""
    return (
        base_lat + random.uniform(-delta, delta),
        base_lng + random.uniform(-delta, delta),
    )


def generate_synthetic_trip():
    """Generates synthetic ride-sharing trip data."""
    city = random.choice(CITIES)

    # Rough city centers (tiny made-up examples just for variety)
    city_coords = {
        "São Paulo": (-23.5505, -46.6333),
        "Rio de Janeiro": (-22.9068, -43.1729),
        "Belo Horizonte": (-19.9167, -43.9345),
        "Curitiba": (-25.4284, -49.2733),
        "Salvador": (-12.9777, -38.5016),
        "Recife": (-8.0476, -34.8770),
        "Fortaleza": (-3.7319, -38.5267),
    }
    base_lat, base_lng = city_coords[city]

    pickup_lat, pickup_lng = random_coord(base_lat, base_lng)
    dropoff_lat, dropoff_lng = random_coord(base_lat, base_lng)

    # Very rough distance proxy
    distance_km = round(random.uniform(1.0, 25.0), 2)

    # Pricing: base + per km + small randomness
    base_fare = 4.0
    per_km = 2.5
    surge = random.choice([1.0, 1.0, 1.2, 1.5])  # mostly no surge
    fare = round((base_fare + per_km * distance_km) * surge, 2)

    status = random.choices(
        STATUSES,
        weights=[0.1, 0.1, 0.7, 0.1],  # mostly Completed
        k=1,
    )[0]

    now = datetime.now()
    # Start time between now and 10 minutes ago
    start_time = now - timedelta(minutes=random.uniform(0, 10))
    # Trip duration proportional to distance
    duration_min = distance_km * random.uniform(1.5, 3.0)
    end_time = start_time + timedelta(minutes=duration_min)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "status": status,
        "city": city,
        "pickup_lat": round(pickup_lat, 6),
        "pickup_lng": round(pickup_lng, 6),
        "dropoff_lat": round(dropoff_lat, 6),
        "dropoff_lng": round(dropoff_lng, 6),
        "driver_id": random.choice(DRIVER_IDS),
        "rider_id": random.choice(RIDER_IDS),
        "distance_km": distance_km,
        "fare": fare,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
    }


def run_producer():
    """Kafka producer that sends synthetic trips to the 'trips' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
