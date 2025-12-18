import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException

from .config import settings

logger = logging.getLogger(__name__)


class HealthMonitor:
    def __init__(self):
        self.last_heartbeat = datetime.now()

    def update_heartbeat(self):
        self.last_heartbeat = datetime.now()

    def is_healthy(self) -> bool:
        delta = datetime.now() - self.last_heartbeat
        return delta.total_seconds() < settings.LIVENESS_THRESHOLD_SECONDS


health_monitor = HealthMonitor()

app = FastAPI()


@app.get("/health")
async def health_check():
    if health_monitor.is_healthy():
        return {"status": "ok", "last_heartbeat": health_monitor.last_heartbeat}
    else:
        logger.warning("Health check failed: no data received recently.")
        # Requirements say: "returning 200 OK if data has been received in the last 60 seconds."
        # If not, it should probably return 503 Service Unavailable or similar.
        # "Liveness Check: Expose a simple /health HTTP endpoint returning 200 OK if..."
        # Implies non-200 if not.
        raise HTTPException(status_code=503, detail="No data received recently")


@app.get("/")
async def root():
    return {"service": settings.APP_NAME, "status": "running"}
