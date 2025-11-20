from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time

import json
import datetime


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def parse_trip(json_str):
    d = json.loads(json_str)
    return (
        d["city"],
        float(d["fare"]),
        d["start_time"]
    )


def serialize_result(value):
    city, revenue, window_end = value
    return json.dumps({
        "city": city,
        "total_revenue": revenue,
        "window_end": window_end
    }).encode("utf-8")


# ------------------------------------------------------------------
# Flink Job
# ------------------------------------------------------------------

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ----------------------------------------------------
    # Kafka Source (Flink 1.15+ API)
    # ----------------------------------------------------
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("trips") \
        .set_group_id("flink-trip-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(KafkaRecordSerializationSchema.value_deserializer()) \
        .build()

    # stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Trips Kafka Source"
    )

    # ----------------------------------------------------
    # Parse JSON
    # (city, fare, timestamp_str)
    # ----------------------------------------------------
    parsed = stream.map(
        lambda x: parse_trip(x.decode("utf-8")),
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.STRING()])
    )

    # ----------------------------------------------------
    # Assign timestamps (event time)
    # ----------------------------------------------------
    def extract_timestamp(rec):
        # rec[2] is start_time string
        dt = datetime.datetime.fromisoformat(rec[2])
        return int(dt.timestamp() * 1000)

    watermarked = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Time.seconds(5))
        .with_timestamp_assigner(extract_timestamp)
    )

    # ----------------------------------------------------
    # Tumbling Window Aggregation (1 minute)
    # ----------------------------------------------------
    aggregated = (
        watermarked
        .key_by(lambda r: r[0])  # key by city
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .reduce(
            lambda a, b: (a[0], a[1] + b[1], b[2]),  # sum revenue
            output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.STRING()])
        )
    )

    # ----------------------------------------------------
    # Kafka Sink
    # ----------------------------------------------------
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("trip_aggregates")
            .set_value_serialization_schema(
                KafkaRecordSerializationSchema.value_serializer(serialize_result)
            )
            .build()
        ) \
        .build()

    aggregated.sink_to(sink)

    env.execute("Ride-Sharing Flink Aggregation Job")


if __name__ == "__main__":
    main()
