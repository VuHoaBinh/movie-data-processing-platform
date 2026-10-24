from __future__ import annotations

import json
import os
from typing import Iterable

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
    DeliveryGuarantee,
)
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows


class MovieWindowAggregate(AggregateFunction):
    def create_accumulator(self):
        return {"title": "", "genre": "", "views": 0, "users": set()}

    def add(self, event, accumulator):
        movie_id, title, genre, user_id = event
        accumulator["title"] = title
        accumulator["genre"] = genre
        accumulator["views"] += 1
        accumulator["users"].add(user_id)
        return accumulator

    def get_result(self, accumulator):
        return (
            accumulator["title"],
            accumulator["genre"],
            accumulator["views"],
            len(accumulator["users"]),
        )

    def merge(self, left, right):
        left["views"] += right["views"]
        left["users"].update(right["users"])
        left["title"] = right["title"] or left["title"]
        left["genre"] = right["genre"] or left["genre"]
        return left


class AddWindowStart(ProcessWindowFunction):
    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable[tuple]):
        for title, genre, views, unique_viewers in elements:
            yield (key, title, genre, context.window().start, views, unique_viewers)


def parse_watch_event(raw: str):
    event = json.loads(raw)
    return (
        str(event["movie_id"]),
        str(event["title"]),
        str(event["genre"]),
        str(event["user_id"]),
    )


def to_json(result) -> str:
    movie_id, title, genre, window_start_ms, live_views, unique_viewers = result
    return json.dumps(
        {
            "movie_id": movie_id,
            "title": title,
            "genre": genre,
            "window_start_ms": int(window_start_ms),
            "live_views": int(live_views),
            "unique_viewers": int(unique_viewers),
        },
        ensure_ascii=False,
    )


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    source_topic = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    sink_topic = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
    window_minutes = int(os.getenv("TREND_WINDOW_MINUTES", "5"))
    window_seconds = int(os.getenv("TREND_WINDOW_SECONDS", str(window_minutes * 60)))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable("/usr/bin/python3")
    env.enable_checkpointing(10_000)
    env.add_jars(
        "file:///opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.1.jar"
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics(source_topic)
        .set_group_id("movie-trending-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    source = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "movie-watch-events",
    )
    event_type = Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
    )
    result_type = Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(), Types.LONG()]
    )
    window_results = (
        source.map(parse_watch_event, output_type=event_type)
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_seconds)))
        .aggregate(
            MovieWindowAggregate(),
            AddWindowStart(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=result_type,
        )
    )

    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(sink_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(brokers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    window_results.map(to_json, output_type=Types.STRING()).sink_to(kafka_sink)
    env.execute("movie-trending-five-minute-windows")


if __name__ == "__main__":
    main()
