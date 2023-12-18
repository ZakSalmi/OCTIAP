import asyncio
from utils.produce_to_kafka import produce_to_kafka
from utils.consume_from_kafka import consume_from_kafka


async def concurrent_send_and_receive(producer, consumer, message, logger, kafka_topic):
    consumer_task = asyncio.create_task(
        consume_from_kafka(consumer, logger, kafka_topic)
    )

    await produce_to_kafka(producer, message, logger, kafka_topic)

    await consumer_task

    return consumer_task.result()
