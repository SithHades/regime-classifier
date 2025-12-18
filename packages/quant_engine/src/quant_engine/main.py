import time
import json
import logging
import os
import signal
import sys
import asyncio
import redis.asyncio as redis
from common.models import Candle
from quant_engine.repository import Repository, Config
from quant_engine.classifier import RegimeClassifier
from pydantic import ValidationError

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for shutdown
running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received. Exiting...")
    running = False

def parse_candle_from_msg(msg_data: dict) -> Candle:
    """
    Parses the candle data from the Redis Stream message.
    """
    try:
        if 'payload' in msg_data:
            data = json.loads(msg_data['payload'])
            return Candle.model_validate(data)
        return Candle.model_validate(msg_data)
    except Exception as e:
        logger.error(f"Failed to parse candle: {e}")
        raise

async def process_candle(candle: Candle, repository: Repository, classifier: RegimeClassifier):
    """
    Processes a single new candle.
    """
    logger.info(f"Processing candle for {candle.symbol} ({candle.timeframe}) at {candle.timestamp}")

    # Fetch history
    # Note: get_recent_candles is synchronous (DB access).
    # In a full async app, we'd wrap this in run_in_executor or use asyncpg.
    # For now, we accept the blocking call or wrapped it if we wanted strict async.
    # Given we are in an async function, let's keep it simple as requested "this function asynchronously" usually implies the entry point or IO bound parts.
    # But since DB is sync, it will block.
    try:
        loop = asyncio.get_running_loop()
        history = await loop.run_in_executor(None, repository.get_recent_candles, candle.symbol, candle.timeframe, 100)
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        history = []

    # Merge logic
    if history and history[-1].timestamp == candle.timestamp:
        full_window = history
    else:
        full_window = history + [candle]

    # Classify (cpu bound, fast enough usually, or could run in executor)
    result = classifier.classify(full_window)

    if result:
        logger.info(f"Classified {candle.symbol} as {result.regime_label}")
        # Save regime is async
        await repository.save_regime(result, timeframe=candle.timeframe)
    else:
        logger.warning(f"Could not classify {candle.symbol} (insufficient data?)")

async def run_service():
    logger.info("Starting Quant Engine Service...")

    config = Config()
    repository = Repository(config)
    classifier = RegimeClassifier(repository, config)

    redis_client = repository.redis_client
    stream_key = config.stream_key
    group_name = config.consumer_group
    consumer_name = config.consumer_name

    # Create Consumer Group
    try:
        await redis_client.xgroup_create(stream_key, group_name, id='0', mkstream=True)
        logger.info(f"Created consumer group {group_name} on stream {stream_key}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {group_name} already exists.")
        else:
            logger.error(f"Error creating group: {e}")
            sys.exit(1)

    logger.info(f"Listening to stream {stream_key} as {consumer_name}...")

    while running:
        try:
            # Block for 1000ms waiting for new messages
            entries = await redis_client.xreadgroup(group_name, consumer_name, {stream_key: ">"}, count=1, block=1000)

            if entries:
                for stream, messages in entries:
                    for message_id, message_data in messages:
                        try:
                            logger.debug(f"Received message {message_id}: {message_data}")
                            candle = parse_candle_from_msg(message_data)

                            # Process asynchronously
                            # If we want parallelism, we can spawn a task.
                            # But usually stream processing is ordered per consumer.
                            # "We should have this function asynchronously" -> await process_candle
                            await process_candle(candle, repository, classifier)

                            # Acknowledge
                            await redis_client.xack(stream_key, group_name, message_id)
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")

        except redis.exceptions.ConnectionError as e:
             logger.error(f"Redis connection error: {e}")
             await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error in stream loop: {e}")
            await asyncio.sleep(1) # Backoff

    logger.info("Quant Engine Service Stopped.")
    await redis_client.aclose()

def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    asyncio.run(run_service())

if __name__ == "__main__":
    main()
