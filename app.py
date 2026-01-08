import os
import time
import secrets
import asyncio
import json
from typing import Dict, Any, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import calendar
import dateparser

from datetime import datetime, timedelta
import dateparser

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

NOP_STORE_ID = int(os.getenv("NOP_STORE_ID", "2"))

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

class AddToCartBody(BaseModel):
    sessionToken: str
    productId: int
    quantity: int = 1
    shoppingCartType: str = "ShoppingCart"

class CartUpdateItem(BaseModel):
    cartItemId: int
    quantity: int

class UpdateCartBody(BaseModel):
    sessionToken: str
    items: list[CartUpdateItem]

class CartGetBody(BaseModel):
    sessionToken: str

class WishlistReadBody(BaseModel):
    sessionToken: str

class CreateRmaBody(BaseModel):
    sessionToken: str
    orderNumber: str
    orderItemId: int
    quantity: int
    reason: str
    action: str
    comments: Optional[str] = ""

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

    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(url, headers=req_headers, json=payload)

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

    # Some nop endpoints can return empty body; be defensive
    if not r.content:
        return {}

    try:
        return r.json()
    except ValueError:
        return {}


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

async def nc_get_wishlist(frontend_token: str):
    url = f"{NC_BASE_URL}/api-frontend/Wishlist/Wishlist"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            url,
            headers={
                "Authorization": frontend_token,
                "Accept": "application/json"
            }
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce wishlist read failed",
                "status": r.status_code,
                "body": r.text
            }
        )

    return r.json()


# ✅ ADD THIS HERE (top-level, no indentation)
def build_updatecart_payload(cart_items, target_id, new_qty):
    payload = {}
    ids = []

    for item in cart_items:
        cid = item["cartItemId"]
        qty = new_qty if cid == target_id else item["quantity"]
        payload[f"itemquantity{cid}"] = str(qty)
        ids.append(str(cid))

    payload["updatecartitemids"] = ",".join(ids)
    payload["removefromcart"] = ""
    return payload

def parse_iso(date_str: str) -> datetime:
    """
    Safely parse nopCommerce ISO-ish timestamps.
    """
    if not date_str:
        raise ValueError("Missing date string")
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def resolve_month_range(text: str, rollover_days: int = 3):
    """
    Resolve a human month (e.g. 'Aug 2025', 'August', 'Aug') into a
    date range with ± rollover days.
    """

    parsed = dateparser.parse(
        text,
        settings={
            "PREFER_DATES_FROM": "past",
            "RELATIVE_BASE": datetime.utcnow(),
            "DATE_ORDER": "MDY",
        }
    )

    if not parsed:
        raise ValueError("Could not parse month")

    year = parsed.year
    month = parsed.month

    first_day = datetime(year, month, 1)
    last_day = datetime(
        year,
        month,
        calendar.monthrange(year, month)[1]
    )

    start_date = first_day - timedelta(days=rollover_days)
    end_date = last_day + timedelta(days=rollover_days)

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
# nopCommerce shipment helpers
# -------------------------------------------------
async def nc_get_shipment_items(shipment_id: int):
    token = await get_admin_token()

    return await nc_get_json(
        f"/api-backend/ShipmentItem/GetByShipmentId/{shipment_id}",
        headers={"Authorization": token}
    )

# -------------------------------------------------
# NEW: Backend shipment hydration helpers (ORDERS)
# -------------------------------------------------

async def nc_get_shipments_by_order_id(order_id: int):
    """
    Returns all backend shipments for a given order ID.
    Backend = source of truth.
    """
    token = await get_admin_token()

    return await nc_get_json(
        f"/api-backend/Shipment/GetByOrderId/{order_id}",
        headers={
            "Authorization": token,
            "Accept": "application/json"
        }
    )


async def nc_get_hydrated_shipments_for_order(order_id: int):
    """
    Fully hydrate shipments for an order:
    - shipment metadata
    - per-line quantities
    - preserves shipment boundaries
    """

    shipments = await nc_get_shipments_by_order_id(order_id)

    hydrated = []

    for s in shipments or []:
        shipment_id = s.get("id")
        if not shipment_id:
            continue

        items = await nc_get_shipment_items(shipment_id)

        hydrated.append({
            "shipmentId": shipment_id,
            "trackingNumber": s.get("tracking_number"),
            "shippedDate": s.get("shipped_date_utc"),
            "deliveryDate": s.get("delivery_date_utc"),
            "items": [
                {
                    "orderItemId": i.get("order_item_id"),
                    "quantity": i.get("quantity", 0)
                }
                for i in items or []
            ]
        })

    return hydrated


def build_order_item_fulfillment_map(hydrated_shipments: list):
    """
    Converts hydrated shipments into:
    { orderItemId: [ shipment_fulfillment, ... ] }
    """

    fulfillment_map = {}

    for s in hydrated_shipments:
        for item in s.get("items", []):
            oid = item.get("orderItemId")
            if oid is None:
                continue

            fulfillment_map.setdefault(oid, []).append({
                "shipmentId": s.get("shipmentId"),
                "quantity": item.get("quantity", 0),
                "trackingNumber": s.get("trackingNumber"),
                "shippedDate": s.get("shippedDate"),
                "deliveryDate": s.get("deliveryDate"),
            })

    return fulfillment_map

# -------------------------------------------------
# nopCommerce backend RMA helpers
# -------------------------------------------------
async def nc_get_backend_json(path: str) -> Any:
    token = await get_admin_token()

    return await nc_get_json(
        path,
        headers={
            "Authorization": token,
            "Accept": "application/json"
        }
    )

async def nc_create_return_request(payload: dict):
    token = await get_admin_token()

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            f"{NC_BASE_URL}/api-backend/ReturnRequest/Create",
            headers={
                "Authorization": token,
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            json=payload
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce ReturnRequest/Create failed",
                "status": r.status_code,
                "body": r.text
            }
        )

    return r.json()

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

async def nc_frontend_post(
    path: str,
    frontend_token: str,
    params: Dict[str, Any]
):
    url = f"{NC_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            url,
            headers={
                "Authorization": frontend_token,
                "Accept": "application/json"
            },
            params=params
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce add-to-cart failed",
                "status": r.status_code,
                "url": url,
                "body": r.text
            }
        )

    return r.json()

async def nc_frontend_post_form(
    path: str,
    frontend_token: str,
    payload: dict
):
    url = f"{NC_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            url,
            headers={
                "Authorization": frontend_token,
                "Accept": "application/json",
                "Content-Type": "application/json-patch+json"
            },
            json=payload
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce update-cart failed",
                "status": r.status_code,
                "body": r.text
            }
        )

    return r.json()

async def nc_update_wishlist(frontend_token: str, product_ids: list[int]):
    if not product_ids:
        return None  # nothing to do

    url = f"{NC_BASE_URL}/api-frontend/Wishlist/UpdateWishlist"

    payload = {
        "addtowishlist": ",".join(str(pid) for pid in product_ids),
        "updatewishlist": "true"
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            url,
            headers={
                "Authorization": frontend_token,
                "Accept": "application/json",
                "Content-Type": "application/json-patch+json",
            },
            json=payload
        )

    if r.status_code >= 400:
        # wishlist must NEVER block cart flow
        return None

    return r.json()

async def nc_update_return_request(payload: dict):
    token = await get_admin_token()

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.put(
            f"{NC_BASE_URL}/api-backend/ReturnRequest/Update",
            headers={
                "Authorization": token,
                "Accept": "application/json",
                "Content-Type": "application/json-patch+json"
            },
            json=payload
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "nopCommerce ReturnRequest/Update failed",
                "status": r.status_code,
                "body": r.text
            }
        )

    try:
        return r.json()
    except ValueError:
        return None

# -------------------------------------------------
# NEW: Backend RMA read helpers (ORDERS)
# -------------------------------------------------

async def nc_get_rmas_by_order_id(order_id: int):
    """
    Returns all RMAs (paged) using the correct nopCommerce endpoint.
    RMAs are linked to order items via order_item_id.
    """
    token = await get_admin_token()

    url = f"{NC_BASE_URL}/api-backend/ReturnRequest/Search"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            url,
            headers={
                "Authorization": token,
                "Accept": "application/json"
            },
            params={
                "storeId": NOP_STORE_ID,
                "pageIndex": 0,
                "pageSize": 2147483647  # max, matches Swagger
            }
        )

    if r.status_code != 200 or not r.content:
        return []

    try:
        payload = r.json()
    except ValueError:
        return []

    return payload.get("items", [])

def build_order_item_rma_map(rmas: list):
    """
    Converts RMA list into:
    { orderItemId: [ rma_summary, ... ] }
    """
    rma_map = {}

    for r in rmas:
        order_item_id = r.get("order_item_id")
        if not order_item_id:
            continue

        rma_map.setdefault(order_item_id, []).append({
            "rmaId": r.get("id"),
            "customNumber": r.get("custom_number"),
            "quantity": r.get("quantity", 0),
            "returnedQuantity": r.get("returned_quantity", 0),
            "statusId": r.get("return_request_status_id"),
            "status": r.get("return_request_status_id"),  # map to label later if desired
            "reason": r.get("reason_for_return"),
            "requestedAction": r.get("requested_action"),
            "createdOn": r.get("created_on_utc"),
            "updatedOn": r.get("updated_on_utc"),
        })

    return rma_map

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

import logging

logger = logging.getLogger("uvicorn.error")

@app.post("/vf/rma/create")
async def vf_create_rma(body: CreateRmaBody):
    logger.error(f"RMA BODY RECEIVED: {body.dict()}")

    # -------------------------------------------------
    # 1. Validate session
    # -------------------------------------------------
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    # -------------------------------------------------
    # 2. Load order details (frontend, customer-scoped)
    # -------------------------------------------------
    order_data = await nc_get_frontend_json(
        f"/api-frontend/Order/Details/{body.orderNumber}",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    # -------------------------------------------------
    # 3. Resolve backend order (REQUIRED for customer_id)
    # -------------------------------------------------
    order_id = order_data.get("id")
    if not order_id:
        raise HTTPException(status_code=500, detail="Order ID not found")

    backend_order = await nc_get_backend_json(
        f"/api-backend/Order/GetById/{order_id}"
    )

    customer_id = backend_order.get("customer_id")
    if not customer_id:
        raise HTTPException(status_code=500, detail="Customer ID not found")

    # -------------------------------------------------
    # 4. Find target item & validate ownership
    # -------------------------------------------------
    target_item = None
    for i in order_data.get("items", []):
        if i.get("id") == body.orderItemId:
            target_item = i
            break

    if not target_item:
        raise HTTPException(status_code=404, detail="Order item not found")

    ordered_qty = target_item.get("quantity", 0)

    if body.quantity <= 0 or body.quantity > ordered_qty:
        raise HTTPException(status_code=400, detail="Invalid return quantity")

    # -------------------------------------------------
    # 5. Get shipped quantity (reuse shipment logic)
    # -------------------------------------------------
    shipments = order_data.get("shipments", []) or []

    shipped_qty = 0
    for s in shipments:
        sid = s.get("id")
        if not sid:
            continue

        items = await nc_get_shipment_items(sid)
        for si in items:
            if si.get("order_item_id") == body.orderItemId:
                shipped_qty += si.get("quantity", 0)

    if shipped_qty < body.quantity:
        raise HTTPException(
            status_code=400,
            detail="Return quantity exceeds shipped quantity"
        )

    # -------------------------------------------------
    # 6. Resolve store
    # -------------------------------------------------
    STORE_ID = int(os.getenv("NOP_STORE_ID", "2"))

    # -------------------------------------------------
    # 7. Build minimal backend DTO (WHITELISTED)
    # -------------------------------------------------
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    payload = {
        "store_id": STORE_ID,
        "order_item_id": body.orderItemId,
        "customer_id": customer_id,
        "quantity": body.quantity,
        "reason_for_return": body.reason,
        "requested_action": body.action,
        "customer_comments": body.comments or "",
        "return_request_status_id": 0,  # ✅ Pending
        "created_on_utc": now_utc,
        "updated_on_utc": now_utc
    }

    # -------------------------------------------------
    # 8. Create RMA
    # -------------------------------------------------
    result = await nc_create_return_request(payload)

    rma_id = result.get("id")
    if not rma_id:
        raise HTTPException(status_code=500, detail="RMA ID not returned from Create")

    # -------------------------------------------------
    # 8a. Freeze
    # -------------------------------------------------    
    created_utc = result.get("created_on_utc", now_utc)
    updated_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
  
    # -------------------------------------------------
    # 9. Patch custom_number via Update
    # -------------------------------------------------
    update_payload = {
        "id": rma_id,
        "custom_number": str(rma_id),
        "store_id": STORE_ID,
        "order_item_id": body.orderItemId,
        "customer_id": customer_id,
        "quantity": body.quantity,
        "returned_quantity": 0,
        "reason_for_return": body.reason,
        "requested_action": body.action,
        "customer_comments": body.comments or "",
        "uploaded_file_id": 0,
        "staff_notes": "",
        "return_request_status_id": 10,
        "created_on_utc": created_utc,
        "updated_on_utc": updated_utc        
    }

    await nc_update_return_request(update_payload)

    # -------------------------------------------------
    # 10. Respond to VF
    # -------------------------------------------------
    return {
        "ok": True,
        "returnRequestId": rma_id,
        "message": "Return request submitted successfully"
    }

@app.post("/vf/orders/details")
async def vf_order_details(body: OrderDetailsBody):
    # -------------------------------------------------
    # 1. Validate session
    # -------------------------------------------------
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    # -------------------------------------------------
    # 2. Load frontend order (customer-scoped metadata)
    # -------------------------------------------------
    order_data = await nc_get_frontend_json(
        f"/api-frontend/Order/Details/{body.orderNumber}",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    order_id = order_data.get("id")
    if not order_id:
        raise HTTPException(status_code=500, detail="Order ID not found")

    # -------------------------------------------------
    # 3. Load backend shipment truth (NEW)
    # -------------------------------------------------
    hydrated_shipments = await nc_get_hydrated_shipments_for_order(order_id)
    fulfillment_map = build_order_item_fulfillment_map(hydrated_shipments)
    # -------------------------------------------------
    # 3a. Load backend RMAs (read-only)
    # -------------------------------------------------
    rmas = await nc_get_rmas_by_order_id(order_id)

    logger.error(f"RMA COUNT FETCHED: {len(rmas)}")
    logger.error(f"RMA SAMPLE: {rmas[0] if rmas else 'NONE'}")

    rma_map = build_order_item_rma_map(rmas)

    # -------------------------------------------------
    # 4. Normalize order items with fulfillment context
    # -------------------------------------------------
    normalized_items = []

    for item in order_data.get("items", []):
        order_item_id = item.get("id")
        ordered_qty = item.get("quantity", 0)

        shipments = fulfillment_map.get(order_item_id, [])

        shipped_qty = sum(s.get("quantity", 0) for s in shipments)

        if shipped_qty == 0:
            status = "unshipped"
        elif shipped_qty < ordered_qty:
            status = "partially_shipped"
        else:
            # fully shipped — check delivery
            if shipments and all(s.get("deliveryDate") for s in shipments):
                status = "delivered"
            else:
                status = "shipped"
        # RMA summary for this item
        item_rmas = rma_map.get(order_item_id, [])
        
        normalized_items.append({
            "orderItemId": order_item_id,
            "productId": item.get("product_id"),
            "sku": item.get("sku"),
            "name": item.get("product_name"),
            "orderedQty": ordered_qty,
            "shippedQty": shipped_qty,
            "status": status,
            "shipments": shipments,
            "rmas": item_rmas
        })

    # -------------------------------------------------
    # 5. Order-level shipment rollups (informational)
    # -------------------------------------------------
    shipped_dates = []
    delivery_dates = []
    tracking_numbers = []

    for s in hydrated_shipments:
        if s.get("shippedDate"):
            shipped_dates.append(s.get("shippedDate"))
        if s.get("deliveryDate"):
            delivery_dates.append(s.get("deliveryDate"))
        if s.get("trackingNumber"):
            tracking_numbers.append(s.get("trackingNumber"))

    latest_shipped_date = max(shipped_dates) if shipped_dates else None
    latest_delivery_date = max(delivery_dates) if delivery_dates else None

    # -------------------------------------------------
    # 6. Respond
    # -------------------------------------------------
    return {
        "orderNumber": order_data.get("custom_order_number"),
        "orderDate": order_data.get("created_on"),
        "orderStatus": order_data.get("order_status"),
        "shippingStatus": order_data.get("shipping_status"),
        "paymentMethod": order_data.get("payment_method"),
        "orderTotal": order_data.get("order_total"),

        "latestShippedDate": latest_shipped_date,
        "latestDeliveryDate": latest_delivery_date,
        "trackingNumbers": tracking_numbers,

        "canReturn": order_data.get("is_return_request_allowed", False),
        "canReorder": order_data.get("is_re_order_allowed", False),

        # ✅ Line-level fulfillment (authoritative)
        "items": normalized_items
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

    # OPTIONAL month-based filtering with rollover
    if body.approxOrderDateText:
        try:
            start_date, end_date = resolve_month_range(body.approxOrderDateText)

            filtered = [
                o for o in orders
                if o.get("created_on")
                and start_date <= parse_iso(o.get("created_on")) <= end_date
            ]

            # STRICT month filtering: apply even if empty
            orders = filtered

        except Exception:
            # Parsing failed → ignore filter entirely
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

@app.post("/vf/cart/add")
async def vf_cart_add(body: AddToCartBody):
    sess = require_session_token(body.sessionToken)

    frontend_token = sess["frontend_token"]

    data = await nc_frontend_post(
        f"/api-frontend/ShoppingCart/AddProductToCartFromCatalog/{body.productId}",
        frontend_token,
        params={
            "shoppingCartType": body.shoppingCartType,
            "quantity": body.quantity
        }
    )

    return {
        "ok": True,
        "productId": body.productId,
        "addedQuantity": body.quantity,
        "totalItems": data["model"]["total_products"],
        "subTotal": data["model"]["sub_total_value"]
}

@app.post("/vf/cart/update")
async def vf_cart_update(body: UpdateCartBody):
    try:
        sess = require_session_token(body.sessionToken)
        frontend_token = sess["frontend_token"]

        cart = await nc_get_frontend_json(
            "/api-frontend/ShoppingCart/Cart",
            headers={
                "Authorization": frontend_token,
                "Accept": "application/json"
            }
        )

        cart_items = [
            {
                "cartItemId": i["id"],
                "quantity": i["quantity"]
            }
            for i in cart.get("items", [])
        ]

        target = body.items[0]   # single-item update
        payload = build_updatecart_payload(
            cart_items,
            target.cartItemId,
            target.quantity
        )


        data = await nc_frontend_post_form(
            "/api-frontend/ShoppingCart/UpdateCart",
            frontend_token,
            payload
        )

        return {"ok": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/vf/cart")
async def vf_cart_get(body: CartGetBody):
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    data = await nc_get_frontend_json(
        "/api-frontend/ShoppingCart/Cart",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    model = data

    return {
        "items": [
            {
                "cartItemId": i.get("id"),
                "productId": i.get("product_id"),
                "name": i.get("product_name"),
                "quantity": i.get("quantity"),
                "unitPrice": i.get("unit_price_value"),
                "lineTotal": i.get("unit_price_value", 0) * i.get("quantity", 0)
            }
            for i in model.get("items", [])
        ],
        "totalItems": model.get("total_products"),
        "subTotal": model.get("sub_total_value"),
        "canCheckout": model.get("display_checkout_button", False),
        "isGuest": model.get("current_customer_is_guest", True)
    }

@app.get("/vf/cart")
async def vf_cart_get(sessionToken: str):
    sess = require_session_token(sessionToken)
    frontend_token = sess["frontend_token"]

    data = await nc_get_frontend_json(
        "/api-frontend/ShoppingCart/Cart",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    # 🔴 TEMPORARY: return raw payload exactly as NOP sends it
    return data

@app.post("/vf/wishlist")
async def vf_wishlist_read(body: WishlistReadBody):
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    data = await nc_get_wishlist(frontend_token)

    return {
        "customerGuid": data.get("customer_guid"),
        "items": [
            {
                "productId": i.get("product_id"),
                "name": i.get("product_name")
            }
            for i in data.get("items", [])
        ]
    }

@app.post("/vf/wishlist/sync")
async def vf_wishlist_sync(body: WishlistReadBody):
    sess = require_session_token(body.sessionToken)
    frontend_token = sess["frontend_token"]

    # 1. READ CART
    cart = await nc_get_frontend_json(
        "/api-frontend/ShoppingCart/Cart",
        headers={
            "Authorization": frontend_token,
            "Accept": "application/json"
        }
    )

    cart_products = {
        i["product_id"]
        for i in cart.get("items", [])
    }

    if not cart_products:
        return {"added": 0, "skipped": 0}

    # 2. READ WISHLIST
    wishlist = await nc_get_wishlist(frontend_token)

    customer_guid = wishlist.get("customer_guid")
    wishlist_products = {
        i.get("product_id")
        for i in wishlist.get("items", [])
    }

    # 3. DIFF
    to_add = cart_products - wishlist_products

    # 4. ADD MISSING ITEMS (BULK)
    await nc_update_wishlist(frontend_token, list(to_add))

    added = len(to_add)


    return {
        "added": added,
        "skipped": len(cart_products) - added
    }
