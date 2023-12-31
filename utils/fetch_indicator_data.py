import httpx
import asyncio


async def fetch_indicator_data(
    session, headers, indicator, logger, retries=3, retry_delay=5
):
    for _ in range(retries):
        try:
            url = f"https://otx.alienvault.com/api/v1/indicators/IPv4/{indicator['indicator']}/general"
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data
        except httpx.PoolTimeout as e:
            logger.warning(f"Connection pool timeout. Retrying... ({e})")
            await asyncio.sleep(retry_delay)
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error: {e}")
            break
        except KeyboardInterrupt:
            logger.info("Script terminated by user.")
        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")
            break

    logger.error(
        f"Max retries reached. Unable to fetch data for indicator {indicator['indicator']}."
    )
    return []
