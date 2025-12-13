from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import uuid

# Simplified x402 implementation
class X402Middleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip for endpoints that don't need payment or are public/docs
        if request.url.path in ["/docs", "/openapi.json", "/redoc", "/health"]:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")

        # Check if L402 header is present
        if not auth_header or not auth_header.startswith("L402"):
            return self._request_payment()

        # Parse token: L402 <token>:<preimage>
        # In a real app, token is the macaroon
        parts = auth_header.split(" ")
        if len(parts) != 2:
            return self._request_payment()

        credentials = parts[1].split(":")
        if len(credentials) != 2:
            return self._request_payment()

        macaroon, preimage = credentials

        # Verify payment (Mock verification)
        if not self._verify_payment(macaroon, preimage):
             return JSONResponse(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                content={"detail": "Invalid payment"},
                headers={"WWW-Authenticate": 'L402 macaroon="", invoice=""'}
            )

        response = await call_next(request)
        return response

    def _request_payment(self):
        # Generate mock invoice and macaroon
        invoice = f"lnbc10n...{uuid.uuid4()}" # Mock invoice
        macaroon = f"macaroon_{uuid.uuid4()}" # Mock macaroon

        # In a real app, we would store the preimage hash and verify against it.
        # Here we just return a 402. The client would "pay" and get the preimage.
        # For our mock, we assume the client knows the magic preimage or we accept anything valid-looking for now
        # OR better: we log what the expected preimage is so we can test it?
        # Since this is a stateless mock for now, let's just demand a specific format or dummy value.

        return JSONResponse(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            content={"detail": "Payment Required"},
            headers={
                "WWW-Authenticate": f'L402 macaroon="{macaroon}", invoice="{invoice}"'
            }
        )

    def _verify_payment(self, macaroon: str, preimage: str) -> bool:
        # Mock verification logic
        # For simplicity in this demo, accept if preimage is "valid_preimage"
        # or if it matches a pattern.
        # Let's say any preimage is valid if it's not empty for this MVP.
        return len(preimage) > 0
