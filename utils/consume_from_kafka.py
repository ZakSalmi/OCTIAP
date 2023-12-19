import json
import time
from utils import KAFKA_GENERAL_TOPIC


async def consume_from_kafka(
    consumer, logger, kafka_topic=KAFKA_GENERAL_TOPIC, timeout_seconds=2
):
    consumer.subscribe([kafka_topic])

    try:
        start_time = time.time()

        while True:
            msg = consumer.poll(1.0)

            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout_seconds:
                logger.warning(
                    f"Timeout ({timeout_seconds} seconds) reached. No message received."
                )
                break
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                break

            value = msg.value().decode("utf-8")
            logger.info(f"Consumed data from Kafka topic: {kafka_topic}")
            return value

    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except json.JSONDecodeError as json_error:
        logger.error(f"Error decoding JSON: {json_error}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
