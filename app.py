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

class SessionAssertBody(BaseModel):
    sessionToken: str

class OrderDetailsBody(BaseModel):
    sessionToken: str
    orderNumber: str

class OrderListBody(BaseModel):
    sessionToken: str
    approxOrderDateText: str | None = None


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


def require_session_token(session_token: str) -> dict:
    sess = SESSIONS.get(session_token)
    if not sess:
        raise HTTPException(status_code=401, detail="Invalid session")

    if sess["expires_at"] < now():
        SESSIONS.pop(session_token, None)
        raise HTTPException(status_code=401, detail="Session expired")

    sess["expires_at"] = now() + SESSION_TTL_SECONDS
    return sess


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

from datetime import datetime, timedelta
import dateparser


def parse_iso(date_str: str) -> datetime:
    """
    Safely parse nopCommerce ISO-ish timestamps.
    """
    if not date_str:
        raise ValueError("Missing date string")
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def resolve_date_range(text: str, window_days: int = 4):
    """
    Resolve fuzzy human date text into a +/- date window.
    Examples:
      - '8/22'
      - 'Aug 22'
      - 'mid August'
      - 'around the 22nd'
    """
    parsed = dateparser.parse(
        text,
        settings={
            "PREFER_DATES_FROM": "past",
            "RELATIVE_BASE": datetime.utcnow(),
        }
    )

    if not parsed:
        raise ValueError(f"Could not parse date text: {text}")

    start_date = parsed - timedelta(days=window_days)
    end_date = parsed + timedelta(days=window_days)
    return start_date, end_date


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

async def nc_get_frontend_json(
    path: str,
    headers: Dict[str, str]
) -> Any:
    url = f"{NC_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, headers=headers)

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce frontend GET failed",
                "status": r.status_code,
                "url": url,
                "body": r.text
            }
        )

    return r.json()


# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/vf/session/assert")
async def vf_session_assert(body: SessionAssertBody):
    require_session_token(body.sessionToken)
    return { "ok": True }


@app.post("/vf/login")
async def vf_login(body: LoginBody):
    # Authenticate against nopCommerce frontend
    data = await nc_post_json(
        NC_FRONTEND_TOKEN_PATH,
        {
            "is_guest": False,
            "email": body.email,
            "username": body.email,
            "password": body.password
        }
    )

    # Extract nopCommerce values
    frontend_token = data.get("token")
    customer_id = data.get("customer_id")

    if not frontend_token or customer_id is None:
        raise HTTPException(
            status_code=502,
            detail="Could not authenticate customer with nopCommerce"
        )

    # Create Render session
    session_token = new_session_token()

    SESSIONS[session_token] = {
        "customer_id": int(customer_id),
        "frontend_token": frontend_token,
        "expires_at": now() + SESSION_TTL_SECONDS
    }

    return {
        "sessionToken": session_token
    }



@app.post("/vf/prices")
async def vf_prices(body: PricesBody):
    sess = require_session_token(body.sessionToken)
    customer_id = sess["customer_id"]

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

@app.post("/vf/orders/details")
async def vf_order_details(body: OrderDetailsBody):
    # Validate session (refreshes TTL)
    sess = require_session_token(body.sessionToken)

    # Use CUSTOMER token, not admin
    frontend_token = sess["frontend_token"]  # token issued at login

    # Call nopCommerce frontend order details
    data = await nc_get_frontend_json(
        f"/api-frontend/Order/Details/{body.orderNumber}",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    shipping_status = data.get("shipping_status", "")
    has_shipped = shipping_status.lower() not in ["not yet shipped", ""]


    # Normalize response
    return {
        "orderNumber": data.get("custom_order_number"),
        "orderDate": data.get("created_on"),
        "orderStatus": data.get("order_status"),
        "shippingStatus": shipping_status,
        "hasShipped": has_shipped,
        "paymentMethod": data.get("payment_method"),
        "orderTotal": data.get("order_total"),
        "canReturn": data.get("is_return_request_allowed", False),
        "canReorder": data.get("is_re_order_allowed", False),
        "items": [
            {
                "name": i.get("product_name"),
                "sku": i.get("sku"),
                "quantity": i.get("quantity"),
                "price": i.get("unit_price")
            }
            for i in data.get("items", [])
        ]
    }

@app.post("/vf/orders/list")
async def vf_orders_list(body: OrderListBody):
    # Validate session
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    # Call nopCommerce frontend API
    data = await nc_get_frontend_json(
        "/api-frontend/Order/CustomerOrders",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    orders = data.get("orders", [])

    # OPTIONAL date-range filtering (safe, non-breaking)
if getattr(body, "approxOrderDateText", None):
    try:
        start_date, end_date = resolve_date_range(body.approxOrderDateText)
        filtered = [
            o for o in orders
            if o.get("created_on")
            and start_date <= parse_iso(o.get("created_on")) <= end_date
        ]
        # ONLY apply filter if it produced results
        if filtered:
            orders = filtered
    except Exception:
        # Ignore bad date input entirely
        pass

    # Normalize for VF
    return {
        "orders": [
            {
                "orderNumber": o.get("custom_order_number"),
                "orderDate": o.get("created_on"),
                "orderStatus": o.get("order_status"),
                "shippingStatus": o.get("shipping_status"),
                "orderTotal": o.get("order_total")
            }
            for o in orders
        ]
    }
