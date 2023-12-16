import json
from utils import KAFKA_GENERAL_TOPIC

async def produce_to_kafka(producer, message, kafka_topic=KAFKA_GENERAL_TOPIC):
    try:
        message_json = json.dumps(message)
        producer.produce(kafka_topic, value=message_json)
        producer.flush()
    except Exception as e:
        print(f"An error occurred: {e}")
