import os
import time
import secrets
import asyncio
import json
from typing import Dict, Any, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# -------------------------------------------------
# Config (ENV)
# -------------------------------------------------
NC_BASE_URL = os.getenv("NC_BASE_URL", "https://store.midtennop.com").rstrip("/")

NC_FRONTEND_TOKEN_PATH = os.getenv(
    "NC_FRONTEND_TOKEN_PATH",
    "/api-frontend/Authenticate/GetToken"
)
NC_BACKEND_TOKEN_PATH = os.getenv(
    "NC_BACKEND_TOKEN_PATH",
    "/api-backend/Authenticate/GetToken"
)
NC_PRICE_PATH_TEMPLATE = os.getenv(
    "NC_PRICE_PATH_TEMPLATE",
    "/api-backend/PriceCalculation/GetFinalPrice/{productId}/{customerId}"
)

NC_ADMIN_EMAIL = os.getenv("NC_ADMIN_EMAIL", "")
NC_ADMIN_PASSWORD = os.getenv("NC_ADMIN_PASSWORD", "")

SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "3600"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12.0"))

# -------------------------------------------------
# In-memory stores (swap to Redis later)
# -------------------------------------------------
# sessionToken -> {"customer_id": int, "expires_at": float}
SESSIONS: Dict[str, Dict[str, Any]] = {}

ADMIN_TOKEN: Optional[str] = None
ADMIN_TOKEN_EXPIRES_AT: float = 0.0
ADMIN_TOKEN_LOCK = asyncio.Lock()

# -------------------------------------------------
# Models
# -------------------------------------------------
class LoginBody(BaseModel):
    email: str
    password: str


class PricesBody(BaseModel):
    sessionToken: str
    productIds: List[int] = Field(min_length=1, max_length=20)
    quantity: int = 1
    includeDiscounts: bool = True
    additionalCharge: float = 0.0

# -------------------------------------------------
# App
# -------------------------------------------------
app = FastAPI(
    title="Voiceflow ↔ nopCommerce Gateway",
    version="2.0.0"
)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def now() -> float:
    return time.time()


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def require_session_token(session_token: str) -> int:
    sess = SESSIONS.get(session_token)
    if not sess:
        raise HTTPException(status_code=401, detail="Invalid session")

    if sess["expires_at"] < now():
        SESSIONS.pop(session_token, None)
        raise HTTPException(status_code=401, detail="Session expired")

    sess["expires_at"] = now() + SESSION_TTL_SECONDS
    return int(sess["customer_id"])


async def nc_post_json(
    path: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None
) -> Any:
    url = f"{NC_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            url,
            content=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                **(headers or {})
            }
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce POST failed",
                "status": r.status_code,
                "url": url,
                "body": r.text
            }
        )

    return r.json()


async def nc_get_json(
    path: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None
) -> Any:
    url = f"{NC_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, headers=headers, params=params)

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce GET failed",
                "status": r.status_code,
                "url": url,
                "body": r.text
            }
        )

    return r.json()

# -------------------------------------------------
# nopCommerce auth helpers
# -------------------------------------------------
async def get_admin_token() -> str:
    global ADMIN_TOKEN, ADMIN_TOKEN_EXPIRES_AT

    async with ADMIN_TOKEN_LOCK:
        if ADMIN_TOKEN and ADMIN_TOKEN_EXPIRES_AT > now() + 10:
            return ADMIN_TOKEN

        if not NC_ADMIN_EMAIL or not NC_ADMIN_PASSWORD:
            raise HTTPException(
                status_code=500,
                detail="Missing NC_ADMIN_EMAIL / NC_ADMIN_PASSWORD"
            )

        data = await nc_post_json(
            NC_BACKEND_TOKEN_PATH,
            {
                "is_guest": True,
                "email": NC_ADMIN_EMAIL,
                "username": NC_ADMIN_EMAIL,
                "password": NC_ADMIN_PASSWORD
            }
        )

        token = data.get("token")
        expires_in = int(data.get("expires_in", 3600))

        if not token:
            raise HTTPException(
                status_code=502,
                detail="Could not parse admin token"
            )

        ADMIN_TOKEN = token
        ADMIN_TOKEN_EXPIRES_AT = now() + max(60, expires_in)
        return ADMIN_TOKEN


async def get_customer_id_from_frontend(login: LoginBody) -> int:
    payload = {
        "is_guest": False,
        "email": login.email,
        "username": login.email,
        "password": login.password
    }

    data = await nc_post_json(NC_FRONTEND_TOKEN_PATH, payload)

    customer_id = data.get("customer_id")
    if customer_id is None:
        raise HTTPException(
            status_code=502,
            detail="Could not parse customer_id"
        )

    return int(customer_id)

# -------------------------------------------------
# Pricing
# -------------------------------------------------
async def get_final_price(
    product_id: int,
    customer_id: int,
    quantity: int,
    include_discounts: bool,
    additional_charge: float
) -> float:
    token = await get_admin_token()

    path = NC_PRICE_PATH_TEMPLATE.format(
        productId=product_id,
        customerId=customer_id
    )

    data = await nc_get_json(
        path,
        headers={"Authorization": token},
        params={
            "quantity": quantity,
            "includeDiscounts": str(include_discounts).lower(),
            "additionalCharge": additional_charge
        }
    )

    return data.get("final_price")

# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/vf/login")
async def vf_login(body: LoginBody):
    customer_id = await get_customer_id_from_frontend(body)

    session_token = new_session_token()
    SESSIONS[session_token] = {
        "customer_id": customer_id,
        "expires_at": now() + SESSION_TTL_SECONDS
    }

    return {
        "sessionToken": session_token
    }


@app.post("/vf/prices")
async def vf_prices(body: PricesBody):
    customer_id = require_session_token(body.sessionToken)

    tasks = [
        get_final_price(
            product_id=pid,
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
            prices[str(pid)] = res

    return {
        "customerId": customer_id,
        "prices": prices,
        "errors": errors
    }
