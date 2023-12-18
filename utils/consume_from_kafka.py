import json
from confluent_kafka import KafkaException
from utils import KAFKA_GENERAL_TOPIC


async def consume_from_kafka(consumer, logger, kafka_topic=KAFKA_GENERAL_TOPIC):
    consumer.subscribe([kafka_topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            value = msg.value().decode("utf-8")
            print(f"Received processed value: {value}")

    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except json.JSONDecodeError as json_error:
        logger.error(f"Error decoding JSON: {json_error}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
