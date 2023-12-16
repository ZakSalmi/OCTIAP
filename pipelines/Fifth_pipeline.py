import json
import httpx
import logging
import asyncio
from pprint import pprint
from collections import Counter
from confluent_kafka import Producer, Consumer
from utils.get_indicators import get_indicators
from utils.kafka_producer import produce_to_kafka
from utils.fetch_indicator_data import fetch_indicator_data
from utils import HEADERS, KAFKA_BOOTSTRAP_SERVER, KAFKA_GENERAL_TOPIC, KAFKA_THREAT_TYPES, GROUP_ID

logging.basicConfig(filename='logs/fifth_pipeline.log', filemode='w', level=logging.INFO)
logger = logging.getLogger(__name__)

async def fifth_pipeline(headers, indicators, kafka_bootstrap_servers, group_id, kafka_general_topic, kafka_fifth_topic, producer):
    current_threat_types = Counter()

    async with httpx.AsyncClient(timeout=120.0, limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)) as session:
        tasks = [fetch_indicator_data(session, headers, indicator, kafka_general_topic, producer, logger) for indicator in indicators]
        await asyncio.gather(*tasks)

    config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    consumer.subscribe([kafka_general_topic])

    try:
        message = consumer.poll(1.0)

        if message is not None and not message.error():
            value = message.value().decode('utf-8')
            data = json.loads(value)

            for pulse in data['pulse_info']['pulses']:
                if pulse['tags']:
                    current_threat_types[tuple(pulse['tags'])] += 1

                    top_threat_type = current_threat_types.most_common(1)[0][0]

                    report = {"timestamp": pulse["modified"], "top_threat_type": top_threat_type}
                    await produce_to_kafka(report, kafka_bootstrap_servers, kafka_fifth_topic)
                    return report

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    with open('data/pulses.json', 'r') as file:
        pulses = json.load(file)

    ipv4_indicators = get_indicators(pulses, 'IPv4')

    producer_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER}
    producer = Producer(producer_config)

    try:
        result = asyncio.run(fifth_pipeline(HEADERS, ipv4_indicators, KAFKA_BOOTSTRAP_SERVER, GROUP_ID, KAFKA_GENERAL_TOPIC, KAFKA_THREAT_TYPES, producer))
        pprint(result)
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        producer.flush(30)
