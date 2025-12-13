from fastapi import FastAPI, Request
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from contextlib import asynccontextmanager

from .routers import regime, portfolio
from .middleware.x402 import X402Middleware
from .services.redis import redis_service
from .limiter import limiter

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_service.connect()
    yield
    # Shutdown
    await redis_service.close()

app = FastAPI(
    title="Regime Classifier Gateway",
    version="1.0.0",
    lifespan=lifespan
)

# Rate Limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Middleware
app.add_middleware(X402Middleware)

# Register Routers
app.include_router(regime.router)
app.include_router(portfolio.router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
