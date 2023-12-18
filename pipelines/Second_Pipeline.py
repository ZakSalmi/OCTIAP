import json
import logging
import asyncio
from pprint import pprint
from collections import Counter
from confluent_kafka import Producer, Consumer
from utils.produce_to_kafka import produce_to_kafka
from utils.consume_from_kafka import consume_from_kafka
from utils.fetch_indicator_data import fetch_indicator_data
from utils.concurrent import concurrent
from utils import (
    HEADERS,
    KAFKA_GENERAL_TOPIC,
    KAFKA_TARGET_COUNTRIES,
    CONSUMER_CONFIG,
    PRODUCER_CONFIG,
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="logs/second_pipeline.log",
    filemode="w",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(console_handler)


async def second_pipeline(
    session,
    headers,
    indicator,
    kafka_general_topic,
    kafka_second_topic,
    producer,
    general_consumer,
    target_countries_consumer,
):
    targeted_countries = Counter()

    try:
        sent_data = await fetch_indicator_data(session, headers, indicator, logger)

        await produce_to_kafka(producer, sent_data, logger, kafka_general_topic)
        logger.info(f"Produced data to Kafka topic: {kafka_general_topic}")

        received_data = await consume_from_kafka(
            general_consumer, logger, kafka_second_topic
        )
        logger.info(f"Consumed data from Kafka topic: {kafka_general_topic}")

        if received_data["pulse_info"]["pulses"]:
            for pulse in received_data["pulse_info"]["pulses"]:
                targeted_countries.update(pulse["targeted_countries"])

        await produce_to_kafka(
            producer,
            dict(targeted_countries.most_common(10)),
            logger,
            kafka_second_topic,
        )

        target_countries_data = await consume_from_kafka(
            target_countries_consumer, logger, kafka_second_topic
        )
        pprint(target_countries_data)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    file_path = "data\ipv4.json"
    with open(file_path, "r") as file:
        pulses = json.load(file)

    producer = Producer(PRODUCER_CONFIG)
    general_consumer = Consumer(CONSUMER_CONFIG)
    target_countries_consumer = Consumer(CONSUMER_CONFIG)

    try:
        asyncio.run(
            concurrent(
                HEADERS,
                pulses,
                KAFKA_GENERAL_TOPIC,
                KAFKA_TARGET_COUNTRIES,
                producer,
                general_consumer,
                target_countries_consumer,
                logger,
                second_pipeline,
            )
        )

    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        producer.flush(30)
        general_consumer.close()
        target_countries_consumer.close()
