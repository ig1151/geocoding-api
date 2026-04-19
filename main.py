from fastapi import FastAPI, Query, HTTPException, Security, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List
import httpx
import os
import math
import secrets
import sqlite3
import threading
from datetime import datetime

# ---------------------------------------------------------------------------
# SQLite key store (thread-safe, persists across restarts)
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "keys.db")
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _db_lock:
        conn = _get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key        TEXT PRIMARY KEY,
                label      TEXT NOT NULL,
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

def db_add_key(key: str, label: str):
    with _db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT INTO api_keys (key, label, active, created_at) VALUES (?, ?, 1, ?)",
            (key, label, datetime.utcnow().isoformat())
        )
        conn.commit()
        conn.close()

def db_is_valid_key(key: str) -> bool:
    with _db_lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT 1 FROM api_keys WHERE key = ? AND active = 1", (key,)
        ).fetchone()
        conn.close()
        return row is not None

def db_revoke_key(key: str) -> bool:
    with _db_lock:
        conn = _get_conn()
        cur = conn.execute(
            "UPDATE api_keys SET active = 0 WHERE key = ?", (key,)
        )
        conn.commit()
        conn.close()
        return cur.rowcount > 0

def db_list_keys() -> list:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute(
            "SELECT key, label, active, created_at FROM api_keys ORDER BY created_at DESC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

init_db()

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="Geocoding API",
    version="1.0.0",
    description="Multi-provider geocoding API with fallback support.",
)

app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Rate limit exceeded. Max 60 requests/minute per IP."},
    )

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
ADMIN_KEY        = os.getenv("ADMIN_KEY", "")
api_key_header   = APIKeyHeader(name="X-API-Key",   auto_error=False)
admin_key_header = APIKeyHeader(name="X-Admin-Key", auto_error=False)

async def require_api_key(key: str = Security(api_key_header)):
    if not key or not db_is_valid_key(key):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Pass it as X-API-Key header.",
        )

async def require_admin_key(key: str = Security(admin_key_header)):
    if not ADMIN_KEY:
        raise HTTPException(status_code=503, detail="Admin key not configured on this server.")
    if not key or key != ADMIN_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing admin key. Pass it as X-Admin-Key header.",
        )

# ---------------------------------------------------------------------------
# Provider credentials
# ---------------------------------------------------------------------------
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY", "")
MAPBOX_API_KEY   = os.getenv("MAPBOX_API_KEY", "")

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class BatchGeocodeRequest(BaseModel):
    addresses: List[str] = Field(..., min_length=1, max_length=50)

class NormalizeRequest(BaseModel):
    addresses: List[str] = Field(..., min_length=1, max_length=50)

# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------
def haversine_meters(lat1, lng1, lat2, lng2) -> float:
    r = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ---------------------------------------------------------------------------
# Provider functions
# ---------------------------------------------------------------------------
async def geocode_opencage(client, address, limit=1):
    if not OPENCAGE_API_KEY:
        return None, {"provider": "opencage", "error": "OPENCAGE_API_KEY is not configured"}
    try:
        resp = await client.get(
            "https://api.opencagedata.com/geocode/v1/json",
            params={"q": address, "key": OPENCAGE_API_KEY, "limit": limit, "no_annotations": 1},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as e:
        return None, {"provider": "opencage", "error": f"HTTP {e.response.status_code}"}
    except httpx.RequestError as e:
        return None, {"provider": "opencage", "error": f"Network error: {str(e)}"}

    if data.get("status", {}).get("code") != 200:
        return None, {"provider": "opencage", "error": data.get("status", {}).get("message", "Unknown error")}
    results = data.get("results", [])
    if not results:
        return None, {"provider": "opencage", "error": "No results found"}
    return [
        {"provider": "opencage", "address": r.get("formatted"),
         "latitude": r["geometry"]["lat"], "longitude": r["geometry"]["lng"]}
        for r in results
    ], None

async def reverse_opencage(client, lat, lng):
    results, error = await geocode_opencage(client, f"{lat},{lng}", limit=1)
    if results is None:
        return None, error
    r = results[0]
    return {"provider": "opencage", "latitude": lat, "longitude": lng, "address": r["address"]}, None

async def geocode_mapbox(client, address, limit=1):
    if not MAPBOX_API_KEY:
        return None, {"provider": "mapbox", "error": "MAPBOX_API_KEY is not configured"}
    try:
        resp = await client.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json",
            params={"access_token": MAPBOX_API_KEY, "limit": limit},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as e:
        return None, {"provider": "mapbox", "error": f"HTTP {e.response.status_code}"}
    except httpx.RequestError as e:
        return None, {"provider": "mapbox", "error": f"Network error: {str(e)}"}

    features = data.get("features", [])
    if not features:
        return None, {"provider": "mapbox", "error": "No results found"}
    return [
        {"provider": "mapbox", "address": f.get("place_name"),
         "latitude": f["center"][1], "longitude": f["center"][0]}
        for f in features
    ], None

async def reverse_mapbox(client, lat, lng):
    if not MAPBOX_API_KEY:
        return None, {"provider": "mapbox", "error": "MAPBOX_API_KEY is not configured"}
    try:
        resp = await client.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lng},{lat}.json",
            params={"access_token": MAPBOX_API_KEY, "limit": 1},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as e:
        return None, {"provider": "mapbox", "error": f"HTTP {e.response.status_code}"}
    except httpx.RequestError as e:
        return None, {"provider": "mapbox", "error": f"Network error: {str(e)}"}

    features = data.get("features", [])
    if not features:
        return None, {"provider": "mapbox", "error": "No results found"}
    return {"provider": "mapbox", "latitude": lat, "longitude": lng,
            "address": features[0].get("place_name")}, None

async def geocode_with_fallback(client, address, limit=1):
    result, err = await geocode_opencage(client, address, limit=limit)
    if result: return result, None
    result, err2 = await geocode_mapbox(client, address, limit=limit)
    if result: return result, None
    return None, {"error": "All providers failed", "opencage_error": err, "mapbox_error": err2}

async def reverse_with_fallback(client, lat, lng):
    result, err = await reverse_opencage(client, lat, lng)
    if result: return result, None
    result, err2 = await reverse_mapbox(client, lat, lng)
    if result: return result, None
    return None, {"error": "All providers failed", "opencage_error": err, "mapbox_error": err2}

# ---------------------------------------------------------------------------
# Key management routes (admin only)
# ---------------------------------------------------------------------------
@app.post("/v1/keys/generate")
async def generate_key(
    label: str = Query(..., min_length=1, max_length=60),
    _: None = Security(require_admin_key),
):
    """Generate and persist a new API key for a customer."""
    new_key = "gc-" + secrets.token_urlsafe(32)
    db_add_key(new_key, label)
    return {
        "label": label,
        "api_key": new_key,
        "note": "Store this securely — it cannot be retrieved again.",
    }

@app.get("/v1/keys")
async def list_keys(_: None = Security(require_admin_key)):
    """List all API keys (masked) with labels and status."""
    keys = db_list_keys()
    for k in keys:
        k["key"] = k["key"][:8] + "••••••••"
    return {"count": len(keys), "keys": keys}

@app.delete("/v1/keys/revoke")
async def revoke_key(
    key: str = Query(..., description="Full API key to revoke"),
    _: None = Security(require_admin_key),
):
    """Revoke a customer API key immediately."""
    if not db_revoke_key(key):
        raise HTTPException(status_code=404, detail="Key not found.")
    return {"revoked": True, "key": key[:8] + "••••••••"}

# ---------------------------------------------------------------------------
# Public routes
# ---------------------------------------------------------------------------
@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {
        "status": "ok",
        "providers": {
            "opencage_configured": bool(OPENCAGE_API_KEY),
            "mapbox_configured":   bool(MAPBOX_API_KEY),
        },
    }

@app.get("/v1/geocode")
@limiter.limit("60/minute")
async def geocode(
    request: Request,
    address: str = Query(..., min_length=2),
    _: None = Security(require_api_key),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await geocode_with_fallback(client, address, limit=1)
        if result: return result[0]
        raise HTTPException(status_code=502, detail=error)

@app.get("/v1/reverse")
@limiter.limit("60/minute")
async def reverse(
    request: Request,
    lat: float = Query(..., ge=-90,  le=90),
    lng: float = Query(..., ge=-180, le=180),
    _: None = Security(require_api_key),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await reverse_with_fallback(client, lat, lng)
        if result: return result
        raise HTTPException(status_code=502, detail=error)

@app.post("/v1/batch/geocode")
@limiter.limit("10/minute")
async def batch_geocode(
    request: Request,
    payload: BatchGeocodeRequest,
    _: None = Security(require_api_key),
):
    output = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for address in payload.addresses:
            result, error = await geocode_with_fallback(client, address, limit=1)
            output.append(
                {"input": address, "result": result[0]} if result
                else {"input": address, "error": error}
            )
    return {"count": len(output), "results": output}

@app.get("/v1/autocomplete")
@limiter.limit("60/minute")
async def autocomplete(
    request: Request,
    q: str = Query(..., min_length=2),
    limit: int = Query(5, ge=1, le=10),
    _: None = Security(require_api_key),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await geocode_with_fallback(client, q, limit=limit)
        if result: return {"query": q, "suggestions": result}
        raise HTTPException(status_code=502, detail=error)

@app.get("/v1/distance")
@limiter.limit("120/minute")
async def distance(
    request: Request,
    lat1: float = Query(..., ge=-90,  le=90),
    lng1: float = Query(..., ge=-180, le=180),
    lat2: float = Query(..., ge=-90,  le=90),
    lng2: float = Query(..., ge=-180, le=180),
    _: None = Security(require_api_key),
):
    meters = haversine_meters(lat1, lng1, lat2, lng2)
    return {
        "lat1": lat1, "lng1": lng1, "lat2": lat2, "lng2": lng2,
        "distance_meters": round(meters, 2),
        "distance_km":     round(meters / 1000.0, 3),
    }

@app.get("/v1/usage")
async def usage(_: None = Security(require_api_key)):
    return {
        "service": "Geocoding API",
        "providers": {
            "opencage_configured": bool(OPENCAGE_API_KEY),
            "mapbox_configured":   bool(MAPBOX_API_KEY),
        },
        "limits": {
            "batch_max_addresses":    50,
            "autocomplete_max_limit": 10,
        },
    }

@app.post("/v1/normalize")
@limiter.limit("10/minute")
async def normalize(
    request: Request,
    payload: NormalizeRequest,
    _: None = Security(require_api_key),
):
    normalized = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for address in payload.addresses:
            result, error = await geocode_with_fallback(client, address, limit=1)
            if result:
                normalized.append({
                    "input": address,
                    "normalized_address": result[0]["address"],
                    "latitude":  result[0]["latitude"],
                    "longitude": result[0]["longitude"],
                    "provider":  result[0]["provider"],
                })
            else:
                normalized.append({"input": address, "error": error})
    return {"count": len(normalized), "results": normalized}
