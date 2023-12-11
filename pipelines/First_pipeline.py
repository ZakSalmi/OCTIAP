import json
import httpx
import logging
import asyncio
from pprint import pprint
from collections import Counter
from confluent_kafka import Producer, Consumer, KafkaException

logging.basicConfig(filename='logs/first_pipeline.log', filemode='w',level=logging.INFO)
logger = logging.getLogger(__name__)

OTX_API_KEY = "b33bf61f62c7e7da86bb84481591a0844630323831ec210fd8c2e2efbfdc131a"

HEADERS = {
    'Content-Type': 'application/json',
    'X-OTX-API-KEY': OTX_API_KEY,
}

KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_GENERAL_TOPIC = 'otx_data_topic'
KAFKA_TARGET_COUNTRIES = 'Target_countries'
GROUP_ID = 'target_filter_group'


async def kafka_producer(message, bootstrap_server=KAFKA_BOOTSTRAP_SERVER, kafka_topic=KAFKA_GENERAL_TOPIC):
    producer = Producer({'bootstrap.servers': bootstrap_server})
    
    try:
        message_json = json.dumps(message)
        producer.produce(kafka_topic, value=message_json)
        producer.flush()
    finally:
        producer.flush(30)


def get_indicators(pulses, pulse_type):
    """
    Return a list of pulses of a specific type
    """
    indicators = [indicator for pulse in pulses for indicator in pulse['indicators'] if indicator['type'] == pulse_type]
    return indicators


async def fetch_indicator_data(session, headers, indicator, kafka_bootstrap_servers, kafka_topic, retries=2):
    for _ in range(retries):
        try:
            url = f"https://otx.alienvault.com/api/v1/indicators/IPv4/{indicator['indicator']}/general"
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            await kafka_producer(data, kafka_bootstrap_servers, kafka_topic)
            return data
        except httpx.PoolTimeout as e:
            logger.warning(f"Connection pool timeout. Retrying... ({e})")
            await asyncio.sleep(5)
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error: {e}")
            break
        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")
            break
    logger.error(f"Max retries reached. Unable to fetch data for indicator {indicator['indicator']}.")
    return []



async def first_pipeline(headers, indicators, kafka_bootstrap_servers, group_id, kafka_general_topic, kafka_second_topic):
    try:
        targeted_countries = Counter()

        async with httpx.AsyncClient(timeout=120.0, limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)) as session:
            tasks = [fetch_indicator_data(session, headers, indicator, kafka_bootstrap_servers, kafka_general_topic) for indicator in indicators]
            results = await asyncio.gather(*tasks)

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
                    targeted_countries.update(pulse['targeted_countries'])

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

        top_threat_sources = dict(targeted_countries.most_common(10))
        await kafka_producer(top_threat_sources, kafka_bootstrap_servers, kafka_second_topic)
        return top_threat_sources

    except Exception as e:
        logger.exception(f"An unexpected error occurred in the first_pipeline: {e}")
        return {}


if __name__ == "__main__":
    with open('data\pulses.json', 'r') as file:
        pulses = json.load(file)

    ipv4_indicators = get_indicators(pulses, 'IPv4')

    try:
        result = asyncio.run(first_pipeline(HEADERS, ipv4_indicators, KAFKA_BOOTSTRAP_SERVER, GROUP_ID, KAFKA_GENERAL_TOPIC, KAFKA_TARGET_COUNTRIES))
        pprint(result)
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
