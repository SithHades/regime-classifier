from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from x402.fastapi.middleware import require_payment

from .limiter import limiter
from .routers import portfolio, regime
from .services.redis import redis_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_service.connect()
    yield
    # Shutdown
    await redis_service.close()


app = FastAPI(title="Regime Classifier Gateway", version="1.0.0", lifespan=lifespan)

# Rate Limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Middleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.middleware("http")(
    require_payment(
        price="0.01", pay_to_address="0x6dbe7555f408021C1d6EB9c84512cb1a72eE1E3F", path=["/v1/portfolio/risk", "/v1/regime"]
    )
)

# Register Routers
app.include_router(regime.router)
app.include_router(portfolio.router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
