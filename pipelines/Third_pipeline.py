import json
import logging
import asyncio
from pprint import pprint
from collections import Counter
from confluent_kafka import Producer, Consumer
from utils.produce_to_kafka import produce_to_kafka
from utils.concurent_send_and_receive import concurrent_send_and_receive
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
    filename="logs/third_pipeline.log",
    filemode="w",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(console_handler)


async def third_pipeline(
    session,
    headers,
    indicator,
    kafka_general_topic,
    kafka_second_topic,
    producer,
    general_consumer,
    current_targets_consumer,
):
    current_targets = Counter()
    try:
        sent_data = await fetch_indicator_data(session, headers, indicator, logger)

        received_data = await concurrent_send_and_receive(
            producer, general_consumer, sent_data, logger, kafka_general_topic
        )

        for pulse in received_data["pulse_info"]["pulses"]:
            if pulse["targeted_countries"]:
                # Update the target country count
                current_targets[tuple(pulse["targeted_countries"])] += 1

                # Check for changes in the top target country/region
                top_target = current_targets.most_common(1)[0][0]

                # Report changes to the second topic
                report = {"timestamp": pulse["modified"], "top_target": top_target}
                await produce_to_kafka(
                    producer,
                    report,
                    logger,
                    kafka_second_topic,
                )

        report_task = asyncio.create_task(
            consume_from_kafka(current_targets_consumer, logger, kafka_second_topic)
        )

        pprint(report_task.result())

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    file_path = "data\ipv4.json"
    logger.info("Starting to open the file path.")
    with open(file_path, "r") as file:
        pulses = json.load(file)
        logger.info("File loaded succesfully.")

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
                third_pipeline,
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
