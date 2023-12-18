import json
import httpx
import logging
import asyncio
from pprint import pprint
from collections import Counter
from confluent_kafka import Producer, Consumer
from utils.produce_to_kafka import produce_to_kafka
from utils.fetch_indicator_data import fetch_indicator_data
from utils import (
    HEADERS,
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_GENERAL_TOPIC,
    KAFKA_TARGET_COUNTRIES,
    GROUP_ID,
)

logging.basicConfig(
    filename="logs/first_pipeline.log", filemode="w", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def first_pipeline(
    headers,
    indicators,
    kafka_bootstrap_servers,
    group_id,
    kafka_general_topic,
    kafka_second_topic,
    producer,
):
    targeted_countries = Counter()

    async with httpx.AsyncClient(
        timeout=120.0,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    ) as session:
        tasks = [
            fetch_indicator_data(
                session, headers, indicator, kafka_general_topic, producer, logger
            )
            for indicator in indicators
        ]
        await asyncio.gather(*tasks)

    config = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(config)
    consumer.subscribe([kafka_general_topic])

    try:
        message = consumer.poll(1.0)

        if message is not None and not message.error():
            value = message.value().decode("utf-8")
            data = json.loads(value)

            for pulse in data["pulse_info"]["pulses"]:
                targeted_countries.update(pulse["targeted_countries"])

    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    finally:
        consumer.close()

    top_targeted_counties = dict(targeted_countries.most_common(10))
    await produce_to_kafka(producer, top_targeted_counties, kafka_second_topic)
    return top_targeted_counties


if __name__ == "__main__":
    file_path = "data\ipv4.json"
    with open(file_path, "r") as file:
        pulses = json.load(file)

    producer_config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVER}
    producer = Producer(producer_config)

    try:
        result = asyncio.run(
            first_pipeline(
                HEADERS,
                pulses,
                KAFKA_BOOTSTRAP_SERVER,
                GROUP_ID,
                KAFKA_GENERAL_TOPIC,
                KAFKA_TARGET_COUNTRIES,
                producer,
            )
        )
        pprint(result)
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        producer.flush(30)
