import logging

import redis.asyncio as redis

from .config import settings

logger = logging.getLogger(__name__)


class Producer:
    def __init__(self):
        self.redis = None

    async def connect(self):
        logger.info(f"Connecting to Redis: {settings.REDIS_URL}")
        self.redis = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        try:
            await self.redis.ping()
            logger.info("Redis connected successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def close(self):
        if self.redis:
            await self.redis.close()

    async def publish_candle(self, candle):
        try:
            # Publish to Redis Stream
            # We use xadd
            payload = candle.model_dump(mode="json")
            # Redis streams entries are field-value pairs strings.
            # We can store the JSON string in a "data" field or explode the fields.
            # The prompt says: Payload Schema: JSON ...
            # Typically consumers parse the payload.

            # For simplicity and to match common patterns, we can store the whole JSON body in one field
            # or add all fields. Redis streams supports structured data.
            # Let's add fields directly as strings.

            # Since pydantic model_dump can handle nested types, we need to ensure everything is string for redis.
            # Actually, `model_dump(mode='json')` returns python dict with primitives.
            # Redis-py xadd expects a dict of bytes/str/numbers.

            # However, the output schema shows nested JSON structure isn't really there, it's flat.
            # { "event_type": ..., "symbol": ..., ... }
            # So we can just pass the dict from model_dump.

            # One detail: timestamps should be strings (ISO format).
            # `model_dump(mode='json')` converts datetime to str if we configured json_encoders properly.
            # Pydantic V2 `model_dump(mode='json')` produces types compatible with JSON (str for datetime).

            await self.redis.xadd(settings.REDIS_STREAM_KEY, payload)
            logger.debug(f"Published candle to {settings.REDIS_STREAM_KEY}: {payload['symbol']} {payload['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")


producer = Producer()
