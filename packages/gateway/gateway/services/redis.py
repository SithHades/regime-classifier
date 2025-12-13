from typing import Optional
import redis.asyncio as redis
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    redis_encoding: str = "utf-8"

    class Config:
        env_file = ".env"

settings = Settings()

class RedisService:
    def __init__(self, url: str = settings.redis_url):
        self.url = url
        self.redis: Optional[redis.Redis] = None
        self.mock_data = {} # For mock mode

    async def connect(self):
        # In a real scenario, we might want to check connection
        if not self.redis:
            self.redis = redis.from_url(self.url, encoding=settings.redis_encoding, decode_responses=True)

    async def get(self, key: str) -> Optional[str]:
        if self.redis:
            try:
                return await self.redis.get(key)
            except redis.ConnectionError:
                # Fallback to mock if connection fails (for sandbox/testing without redis)
                return self.mock_data.get(key)
        return self.mock_data.get(key)

    async def set(self, key: str, value: str):
        if self.redis:
             try:
                await self.redis.set(key, value)
                return
             except redis.ConnectionError:
                pass
        self.mock_data[key] = value

    async def close(self):
        if self.redis:
            await self.redis.close()

# Global instance
redis_service = RedisService()
