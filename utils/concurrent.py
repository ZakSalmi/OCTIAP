import httpx
import asyncio


async def concurrent(
    headers,
    indicators,
    kafka_general_topic,
    kafka_second_topic,
    producer,
    general_consumer,
    target_countries_consumer,
    logger,
    pipeline,
):
    tasks = []
    try:
        async with httpx.AsyncClient(
            timeout=120.0,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        ) as session:
            logger.info("Starting the first pipeline.")
            for indicator in indicators:
                task = pipeline(
                    session,
                    headers,
                    indicator,
                    kafka_general_topic,
                    kafka_second_topic,
                    producer,
                    general_consumer,
                    target_countries_consumer,
                )
                tasks.append(task)
            await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
