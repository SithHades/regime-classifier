import asyncio
import logging

import uvicorn

from .config import settings
from .connector import connector
from .db import db
from .health import app
from .producer import producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def start_health_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.HEALTH_CHECK_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    logger.info("Starting Sentinel Service...")

    # Connect to resources
    await db.connect()
    await producer.connect()

    try:
        # Run Connector and Health Server concurrently
        await asyncio.gather(
            connector.start(),
            start_health_server(),
        )
    except asyncio.CancelledError:
        logger.info("Sentinel stopping...")
    except Exception as e:
        logger.error(f"Sentinel crashed: {e}")
    finally:
        await connector.stop()
        await producer.close()
        await db.close()
        logger.info("Sentinel stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
