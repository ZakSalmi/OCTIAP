import json
from utils import KAFKA_GENERAL_TOPIC


async def produce_to_kafka(producer, message, logger, kafka_topic=KAFKA_GENERAL_TOPIC):
    try:
        message_json = json.dumps(message, indent=6)
        producer.produce(kafka_topic, value=message_json)
        logger.info(f"Produced data to Kafka topic: {kafka_topic}")
        producer.flush()
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.info(f"An error occurred: {e}")
    finally:
        producer.flush(10)
