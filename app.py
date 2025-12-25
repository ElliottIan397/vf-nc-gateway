import os
import time
import secrets
import asyncio
import json
from typing import Dict, Any, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from pydantic import BaseModel, Field

# ----------------------------
# Config (ENV)
# ----------------------------
NC_BASE_URL = os.getenv("NC_BASE_URL", "https://store.midtennop.com").rstrip("/")

NC_FRONTEND_TOKEN_PATH = os.getenv("NC_FRONTEND_TOKEN_PATH", "/api-frontend/Authenticate/GetToken")
NC_BACKEND_TOKEN_PATH  = os.getenv("NC_BACKEND_TOKEN_PATH",  "/api-backend/Authenticate/GetToken")
NC_PRICE_PATH_TEMPLATE = os.getenv(
    "NC_PRICE_PATH_TEMPLATE",
    "/api-backend/PriceCalculation/GetFinalPrice/{productId}/{customerId}"
)

NC_ADMIN_EMAIL    = os.getenv("NC_ADMIN_EMAIL", "")
NC_ADMIN_PASSWORD = os.getenv("NC_ADMIN_PASSWORD", "")

# Cookie/session settings
COOKIE_NAME   = os.getenv("COOKIE_NAME", "mtop_session")
COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", ".midtennop.com")  # critical for subdomain sharing
COOKIE_PATH   = os.getenv("COOKIE_PATH", "/")
COOKIE_TTL_SECONDS = int(os.getenv("COOKIE_TTL_SECONDS", "3600"))  # 60 min sliding by default
COOKIE_SAMESITE = os.getenv("COOKIE_SAMESITE", "lax").lower()      # lax recommended

# CORS
ALLOWED_ORIGINS = [o.strip() for o in os.getenv(
    "ALLOWED_ORIGINS",
    "https://www.midtennop.com,https://store.midtennop.com"
).split(",") if o.strip()]

# httpx
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12.0"))

# ----------------------------
# In-memory stores (swap to Redis later)
# ----------------------------
# session_id -> {"customer_id": int, "expires_at": float}
SESSIONS: Dict[str, Dict[str, Any]] = {}

# backend admin token cache
ADMIN_TOKEN: Optional[str] = None
ADMIN_TOKEN_EXPIRES_AT: float = 0.0
ADMIN_TOKEN_LOCK = asyncio.Lock()

# ----------------------------
# Models
# ----------------------------
class LoginBody(BaseModel):
    email: Optional[str] = None
    username: Optional[str] = None
    password: str

class PricesBody(BaseModel):
    productIds: List[int] = Field(min_length=1, max_length=20)
    quantity: int = 1
    includeDiscounts: bool = True
    additionalCharge: float = 0.0

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="Voiceflow ↔ nopCommerce Gateway", version="1.0.0")


# ----------------------------
# Helpers
# ----------------------------
def now() -> float:
    return time.time()

def new_session_id() -> str:
    return secrets.token_urlsafe(32)

def get_cookie_secure_flag(request: Request) -> bool:
    # On Render, request.url.scheme is usually correct behind proxy;
    # Still safest to force Secure in production.
    return True

def set_session_cookie(response: Response, session_id: str, request: Request) -> None:
    response.set_cookie(
        key=COOKIE_NAME,
        value=session_id,
        max_age=COOKIE_TTL_SECONDS,
        expires=COOKIE_TTL_SECONDS,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
        secure=get_cookie_secure_flag(request),
        httponly=True,
        samesite=COOKIE_SAMESITE,  # "lax" or "none" (none requires Secure)
    )

def clear_session_cookie(response: Response, request: Request) -> None:
    response.delete_cookie(
        key=COOKIE_NAME,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
    )

def touch_session(session_id: str) -> None:
    sess = SESSIONS.get(session_id)
    if sess:
        sess["expires_at"] = now() + COOKIE_TTL_SECONDS

def require_session(request: Request) -> Tuple[str, int]:
    session_id = request.cookies.get(COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not logged in (missing session cookie).")

    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=401, detail="Session not found or expired.")

    if sess["expires_at"] < now():
        SESSIONS.pop(session_id, None)
        raise HTTPException(status_code=401, detail="Session expired.")

    # sliding TTL
    touch_session(session_id)
    return session_id, int(sess["customer_id"])

async def nc_post_json(
    path: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None
) -> Any:
    url = f"{NC_BASE_URL}{path}"

    json_body = json.dumps(payload)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            url,
            content=json_body,
            headers={
                **(headers or {}),
                "Content-Type": "application/json"
            }
        )

        if r.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "nopCommerce POST failed",
                    "status": r.status_code,
                    "url": url,
                    "body": safe_json(r)
                }
            )

        return safe_json(r)

async def nc_get_json(path: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{NC_BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, headers=headers, params=params)
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail={
                "error": "nopCommerce GET failed",
                "status": r.status_code,
                "url": url,
                "body": safe_json(r)
            })
        return safe_json(r)

def safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"text": resp.text}

async def get_admin_token() -> str:
    """
    Gets/caches backend admin token. Refreshes automatically if expired.
    NOTE: We don't know your exact token response schema, so this function
    tries common patterns.
    """
    global ADMIN_TOKEN, ADMIN_TOKEN_EXPIRES_AT

    async with ADMIN_TOKEN_LOCK:
        if ADMIN_TOKEN and ADMIN_TOKEN_EXPIRES_AT > now() + 10:
            return ADMIN_TOKEN

        if not NC_ADMIN_EMAIL or not NC_ADMIN_PASSWORD:
            raise HTTPException(status_code=500, detail="Missing NC_ADMIN_EMAIL / NC_ADMIN_PASSWORD env vars.")

        payload = {
            "email": NC_ADMIN_EMAIL,
            "password": NC_ADMIN_PASSWORD
        }

        data = await nc_post_json(NC_BACKEND_TOKEN_PATH, payload)

        # Common shapes:
        # 1) {"token":"...","expires_in":3600}
        # 2) "...." (raw string)
        # 3) {"access_token":"...","expiresIn":3600}
        token = None
        expires_in = 3600

        if isinstance(data, str):
            token = data.strip('"')
        elif isinstance(data, dict):
            token = data.get("token")
            expires_in = int(data.get("expires_in") or data.get("expiresIn") or data.get("ExpiresIn") or expires_in)

        if not token:
            raise HTTPException(status_code=502, detail={"error": "Could not parse admin token response", "data": data})

        ADMIN_TOKEN = token
        ADMIN_TOKEN_EXPIRES_AT = now() + max(60, expires_in)
        return ADMIN_TOKEN

async def get_customer_id_from_frontend(login: LoginBody) -> int:
    payload = {
        "email": login.email,
        "username": login.email,          # REQUIRED
        "password": login.password
    }

    data = await nc_post_json(NC_FRONTEND_TOKEN_PATH, payload)

    customer_id = data.get("customer_id")
    if customer_id is None:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "Could not parse customer_id from frontend auth response",
                "data": data
            }
        )

    return int(customer_id)

async def get_final_price(product_id: int, customer_id: int, quantity: int, include_discounts: bool, additional_charge: float) -> Dict[str, Any]:
    token = await get_admin_token()
    path = NC_PRICE_PATH_TEMPLATE.format(productId=product_id, customerId=customer_id)

    # Many nopCommerce endpoints accept these as query params for GET.
    # If your endpoint is POST instead, we can flip it quickly.
    params = {
        "additionalCharge": additional_charge,
        "includeDiscounts": str(include_discounts).lower(),
        "quantity": quantity
    }

    headers = {
        "Authorization": token
    }

    data = await nc_get_json(path, headers=headers, params=params)

    # Common shapes include: {"final_price": 259.47} or {"finalPrice": 259.47}
    final_price = None
    currency = data.get("currency") if isinstance(data, dict) else None

    if isinstance(data, dict):
        final_price = data.get("final_price")

    return {
        "productId": product_id,
        "customerId": customer_id,
        "final_price": final_price,
        "raw": data,
        "currency": currency
    }

# ----------------------------
# Basic CORS (minimal; adjust if you embed VF in an iframe)
# ----------------------------
@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    origin = request.headers.get("origin")
    if origin and origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.options("/{path:path}")
async def preflight(path: str, request: Request):
    # Fast preflight support
    origin = request.headers.get("origin")
    if origin and origin in ALLOWED_ORIGINS:
        r = Response(status_code=204)
        r.headers["Access-Control-Allow-Origin"] = origin
        r.headers["Vary"] = "Origin"
        r.headers["Access-Control-Allow-Credentials"] = "true"
        r.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return r
    return Response(status_code=204)

# ----------------------------
# Routes
# ----------------------------
@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/vf/login")
async def vf_login(body: LoginBody, request: Request):
    customer_id = await get_customer_id_from_frontend(body)

    session_id = new_session_id()
    SESSIONS[session_id] = {
        "customer_id": customer_id,
        "expires_at": now() + COOKIE_TTL_SECONDS
    }

    resp = Response(content='{"ok":true}', media_type="application/json")
    set_session_cookie(resp, session_id, request)
    return resp

@app.post("/vf/logout")
async def vf_logout(request: Request):
    session_id = request.cookies.get(COOKIE_NAME)
    if session_id:
        SESSIONS.pop(session_id, None)
    resp = Response(content='{"ok":true}', media_type="application/json")
    clear_session_cookie(resp, request)
    return resp

@app.post("/vf/prices")
async def vf_prices(body: PricesBody, request: Request):
    session_id, customer_id = require_session(request)

    # Fan out concurrently (limit can be added later)
    tasks = [
        get_final_price(
            product_id=int(pid),
            customer_id=customer_id,
            quantity=body.quantity,
            include_discounts=body.includeDiscounts,
            additional_charge=body.additionalCharge
        )
        for pid in body.productIds
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    prices: Dict[str, Any] = {}
    errors: Dict[str, Any] = {}

    for pid, res in zip(body.productIds, results):
        if isinstance(res, Exception):
            errors[str(pid)] = str(res)
        else:
            prices[str(pid)] = res.get("final_price")

    # Sliding TTL refresh cookie
    resp = Response(
        content=json.dumps({
            "customerId": customer_id,
            "prices": prices,
            "errors": errors
        }),
        media_type="application/json"
    )
    # refresh cookie ttl on each prices call
    set_session_cookie(resp, session_id, request)
    return resp
