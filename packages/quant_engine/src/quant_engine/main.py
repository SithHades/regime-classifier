import time
import json
import logging
import os
import signal
import sys
import redis
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

def process_candle(candle: Candle, repository: Repository, classifier: RegimeClassifier):
    """
    Processes a single new candle.
    """
    logger.info(f"Processing candle for {candle.symbol} at {candle.timestamp}")

    # Fetch history
    history = repository.get_recent_candles(candle.symbol, limit=100)

    # Merge logic
    if history and history[-1].timestamp == candle.timestamp:
        full_window = history
    else:
        full_window = history + [candle]

    # Classify
    result = classifier.classify(full_window)

    if result:
        logger.info(f"Classified {candle.symbol} as {result.regime_label}")
        repository.save_regime(result)
    else:
        logger.warning(f"Could not classify {candle.symbol} (insufficient data?)")

def main():
    logger.info("Starting Quant Engine Service...")

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = Config()
    repository = Repository(config)
    classifier = RegimeClassifier(repository, config)

    redis_client = repository.redis_client
    stream_key = config.STREAM_KEY
    group_name = config.CONSUMER_GROUP
    consumer_name = config.CONSUMER_NAME

    # Create Consumer Group
    try:
        redis_client.xgroup_create(stream_key, group_name, id='0', mkstream=True)
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
            entries = redis_client.xreadgroup(group_name, consumer_name, {stream_key: ">"}, count=1, block=1000)

            if entries:
                for stream, messages in entries:
                    for message_id, message_data in messages:
                        try:
                            logger.debug(f"Received message {message_id}: {message_data}")
                            candle = parse_candle_from_msg(message_data)
                            process_candle(candle, repository, classifier)

                            # Acknowledge
                            redis_client.xack(stream_key, group_name, message_id)
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")

        except redis.exceptions.ConnectionError as e:
             logger.error(f"Redis connection error: {e}")
             time.sleep(5)
        except Exception as e:
            logger.error(f"Error in stream loop: {e}")
            time.sleep(1) # Backoff

    logger.info("Quant Engine Service Stopped.")

if __name__ == "__main__":
    main()
