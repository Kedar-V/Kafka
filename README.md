**Real-Time Ride-Sharing Trips — Kafka + PostgreSQL + Streamlit**

This repository is a hands-on project demonstrating a real-time streaming pipeline using Apache Kafka, PostgreSQL, and Streamlit. The original assignment asked to adapt a streaming e-commerce demo; this version models ride-sharing trips and generates synthetic trip events that flow through Kafka, are persisted in PostgreSQL, and displayed live via a Streamlit dashboard.

**What's Included**
- `kafka_realtime_pipeline/producer.py`: synthetic ride-sharing trip generator and Kafka producer.
- `kafka_realtime_pipeline/consumer.py`: Kafka consumer that inserts trip events into PostgreSQL.
- `kafka_realtime_pipeline/dashboard.py`: Streamlit app that queries PostgreSQL and displays live metrics and charts.
- `kafka_realtime_pipeline/docker-compose.yml`: (optional) Docker Compose orchestration for Kafka, Zookeeper, and PostgreSQL (if provided/used).
- `kafka_realtime_pipeline/requirements.txt`: Python dependencies for running producer, consumer, and dashboard.

**High-Level Architecture**
- Synthetic events -> Kafka topic `trips`
- Kafka consumer -> writes events into `trips` table in PostgreSQL
- Streamlit dashboard -> queries PostgreSQL and auto-refreshes to show live metrics

**Data Domain**
This version models ride-sharing trips across several Brazilian cities. Each event contains trip metadata such as pickup/dropoff coordinates, driver/rider IDs, fare, status, start/end times, and payment method.

Sample event (JSON):

```
{
	"trip_id": "a1b2c3d4",
	"status": "Completed",
	"city": "São Paulo",
	"pickup_lat": -23.550500,
	"pickup_lng": -46.633300,
	"dropoff_lat": -23.540000,
	"dropoff_lng": -46.620000,
	"driver_id": "DRV-001",
	"rider_id": "RID-012",
	"distance_km": 8.23,
	"fare": 24.57,
	"start_time": "2025-11-19T12:34:56.789012",
	"end_time": "2025-11-19T12:50:12.123456",
	"payment_method": "PIX"
}
```

**Database Schema (created by `consumer.py`)**
- Table: `trips`
- Columns: `trip_id (PK)`, `status`, `city`, `pickup_lat`, `pickup_lng`, `dropoff_lat`, `dropoff_lng`, `driver_id`, `rider_id`, `distance_km`, `fare`, `start_time`, `end_time`, `payment_method`.

**Prerequisites**
- Docker & Docker Compose (recommended) OR local Kafka + PostgreSQL installations.
- Python 3.8+ and `pip`.

**Quick Start — using Docker Compose (recommended)**
1. From the project root, change into the pipeline folder:

```bash
cd kafka_realtime_pipeline
```

2. Start the services with Docker Compose (if the provided `docker-compose.yml` defines Kafka and Postgres):

```bash
docker-compose up -d
```

3. Install Python dependencies (create a venv if you like):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

4. Run the consumer (writes to Postgres):

```bash
python consumer.py
```

5. Run the producer (generates synthetic trip events to Kafka):

```bash
python producer.py
```

6. Run the Streamlit dashboard (live visualization):

```bash
streamlit run dashboard.py
```

Open the Streamlit URL that appears in the terminal (usually `http://localhost:8501`).

**If you don't use Docker**
- Ensure Kafka is reachable at `localhost:9092` and PostgreSQL at `localhost:5432` with the credentials present in `consumer.py` and `dashboard.py`.

**Configuration**
- Kafka bootstrap server: configured in `producer.py` and `consumer.py` (`localhost:9092`).
- PostgreSQL URL: configured in `dashboard.py` (`postgresql://kafka_user:kafka_password@localhost:5432/kafka_db`). Update these values to match your environment.

**Customizing the Data Generator**
- Edit `CITIES`, `STATUSES`, `PAYMENT_METHODS`, driver and rider pools in `producer.py` to change domains.
- Adjust `generate_synthetic_trip()` to modify trip frequency, distributions, or fields.

**Troubleshooting Tips**
- If the producer fails with connection errors, confirm Kafka is running and reachable at the configured address.
- If the consumer cannot connect to Postgres, verify DB user/password, and that the DB accepts connections from your host.
- Streamlit can be restarted anytime; the dashboard reads the latest data from Postgres on each refresh interval.

**Next Steps / Ideas**
- Add Flink or Kafka Streams job for enrichment/aggregation.
- Persist aggregates to a materialized view and visualize latency or heatmaps.
- Add authentication to the Streamlit dashboard for multi-user access.

---

**Machine Learning: Real-Time Anomaly Detection & Predictive Modeling**

This project demonstrates **sequential on-stream modeling** with two complementary ML approaches:

**1. Completion Probability Prediction**
- **Model**: Logistic Regression with Stochastic Gradient Descent (SGDClassifier)
- **Features**: Distance (km), Fare ($), Hour of day
- **Target**: Trip completion (Completed = 1, other statuses = 0)
- **Training**: Incremental learning via `partial_fit()` as each trip event arrives
- **Use Case**: Predict likelihood a trip will complete based on distance, fare pricing, and time-of-day patterns
- **Output**: `completion_prob` (0.0 to 1.0) stored in PostgreSQL for each trip

Example interpretation:
- High completion probability (0.85+): typical trip characteristics, likely to succeed
- Low completion probability (<0.6): unusual combination of features (short distance but high fare, or late-night trips), may warrant driver/rider alerts

**2. EWMA (Exponential Weighted Moving Average) Anomaly Detection**
- **Metric**: Fare amount per trip
- **Method**: Continuous drift detection using exponential smoothing
- **Calculation**:
  - Maintain running mean and standard deviation with alpha=0.1 (decay factor)
  - For each incoming trip, compute z-score: `(fare - ema_mean) / ema_std`
  - Flag as anomaly if z > 1.5 (1.5 standard deviations from moving average)
- **Use Case**: Detect unusually high or low fares that may indicate pricing errors, surge pricing anomalies, or fraudulent activity
- **Output**: `anomaly` (True/False) boolean flag in PostgreSQL

Example anomalies:
- A 50 km trip with $15 fare (suspiciously low)
- A 2 km trip with $100 fare (surge pricing, or data error)

**Implementation Details**
- **Consumer Logic** (`consumer.py`):
  - Loads or initializes SGDClassifier from disk (`completion_model.pkl`)
  - For each trip event:
    1. Build feature vector: `[distance_km, fare, hour]`
    2. Predict completion probability using current model
    3. Observe actual outcome and call `partial_fit()` to train incrementally
    4. Compute EWMA anomaly score and persist both predictions to `trips_ml` table
    5. Save updated model back to disk for next restart
  
- **Database Schema** (`trips_ml` table):
  - Stores original trip fields plus two ML columns: `completion_prob` (NUMERIC), `anomaly` (BOOLEAN)
  
- **Dashboard Integration** (`dashboard.py`):
  - Visualizes completion probability distribution (histogram)
  - Scatter plot: Fare vs Completion Probability, colored by anomaly flag
  - Alert section: Lists all detected anomalies in current window with trip details

**Why This Approach?**
- **Online Learning**: Model updates continuously without retraining from scratch; adapts to changing trip patterns
- **Low Latency**: Predictions happen in real-time as events arrive, suitable for alerting and decision-making
- **Drift Detection**: EWMA captures temporal patterns (e.g., evening surge pricing) and flags unusual deviations
- **Interpretability**: Simple features and explicit statistical methods make results explainable to stakeholders

**Extending the ML Pipeline**
- Add more features: weather, traffic conditions, driver rating, rider history
- Use multivariate anomaly detection (e.g., Isolation Forest) for complex patterns
- Implement A/B testing for model variants
- Export predictions to downstream systems (alerting, real-time pricing adjustment)

---

Created for the assignment: implement a real-time streaming pipeline modeling ride-sharing trips with sequential ML modeling. See files under `kafka_realtime_pipeline/` for code and configuration.

**Output Samples**
Below are two sample outputs (SVG images) demonstrating the dashboard UI and a single trip event preview. You can find the files in `kafka_realtime_pipeline/assets/`.

![Dashboard sample output](kafka_realtime_pipeline/assets/op1.png)

![Single trip event sample](kafka_realtime_pipeline/assets/op2.png)

![Model Output](kafka_realtime_pipeline/assets/op3.png)

