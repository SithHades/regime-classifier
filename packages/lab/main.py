import time
import schedule
import logging
from training import train_model

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def job():
    logger.info("Running scheduled training job...")
    try:
        train_model()
    except Exception as e:
        logger.error(f"Job failed: {e}")

def main():
    logger.info("Starting Lab Service (Online Mode)...")

    # Run once immediately on startup? Or wait for schedule?
    # Usually safer to run once to ensure state is fresh, then schedule.
    # But for a strict schedule, we might just wait.
    # Let's run once on startup for now so we have a model.
    job()

    # Schedule: "runs every Sunday at 00:00 UTC"
    # Note: 'schedule' uses system time. We assume container is in UTC.
    schedule.every().sunday.at("00:00").do(job)

    logger.info("Scheduler started. Waiting for next job...")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
