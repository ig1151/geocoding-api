from fastapi import FastAPI, Query, Body
from pydantic import BaseModel, Field
from typing import List, Optional
import httpx
import os
import math

app = FastAPI(title="Extended Geocoding API", version="1.0.0")

OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY", "")
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")


class BatchGeocodeRequest(BaseModel):
    addresses: List[str] = Field(..., min_length=1, max_length=50)


class NormalizeRequest(BaseModel):
    addresses: List[str] = Field(..., min_length=1, max_length=50)


def haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@app.get("/health")
def health():
    return {
        "status": "ok",
        "providers": {
            "opencage_configured": bool(OPENCAGE_API_KEY),
            "mapbox_configured": bool(MAPBOX_API_KEY),
        },
    }


async def geocode_opencage(client: httpx.AsyncClient, address: str, limit: int = 1):
    if not OPENCAGE_API_KEY:
        return None, {"provider": "opencage", "error": "OPENCAGE_API_KEY is missing"}

    resp = await client.get(
        "https://api.opencagedata.com/geocode/v1/json",
        params={
            "q": address,
            "key": OPENCAGE_API_KEY,
            "limit": limit,
            "no_annotations": 1,
        },
    )
    data = resp.json()

    if data.get("status", {}).get("code") != 200:
        return None, {
            "provider": "opencage",
            "error": data.get("status", {}).get("message", "UNKNOWN_ERROR"),
            "raw": data,
        }

    results = data.get("results", [])
    if not results:
        return None, {"provider": "opencage", "error": "No results found", "raw": data}

    formatted = []
    for item in results:
        geometry = item.get("geometry", {})
        formatted.append({
            "provider": "opencage",
            "address": item.get("formatted"),
            "latitude": geometry.get("lat"),
            "longitude": geometry.get("lng"),
        })

    return formatted, None


async def reverse_opencage(client: httpx.AsyncClient, lat: float, lng: float):
    results, error = await geocode_opencage(client, f"{lat},{lng}", limit=1)
    if results is None:
        return None, error
    first = results[0]
    return {
        "provider": "opencage",
        "latitude": lat,
        "longitude": lng,
        "address": first["address"],
    }, None


async def geocode_mapbox(client: httpx.AsyncClient, address: str, limit: int = 1):
    if not MAPBOX_API_KEY:
        return None, {"provider": "mapbox", "error": "MAPBOX_API_KEY is missing"}

    resp = await client.get(
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json",
        params={"access_token": MAPBOX_API_KEY, "limit": limit},
    )
    data = resp.json()

    features = data.get("features", [])
    if not features:
        return None, {"provider": "mapbox", "error": "No results found", "raw": data}

    formatted = []
    for item in features:
        lng, lat = item.get("center", [None, None])
        formatted.append({
            "provider": "mapbox",
            "address": item.get("place_name"),
            "latitude": lat,
            "longitude": lng,
        })

    return formatted, None


async def reverse_mapbox(client: httpx.AsyncClient, lat: float, lng: float):
    if not MAPBOX_API_KEY:
        return None, {"provider": "mapbox", "error": "MAPBOX_API_KEY is missing"}

    resp = await client.get(
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lng},{lat}.json",
        params={"access_token": MAPBOX_API_KEY, "limit": 1},
    )
    data = resp.json()
    features = data.get("features", [])
    if not features:
        return None, {"provider": "mapbox", "error": "No results found", "raw": data}

    first = features[0]
    return {
        "provider": "mapbox",
        "latitude": lat,
        "longitude": lng,
        "address": first.get("place_name"),
    }, None


async def geocode_with_fallback(client: httpx.AsyncClient, address: str, limit: int = 1):
    primary, primary_error = await geocode_opencage(client, address, limit=limit)
    if primary is not None:
        return primary, None

    fallback, fallback_error = await geocode_mapbox(client, address, limit=limit)
    if fallback is not None:
        return fallback, None

    return None, {"error": "All providers failed", "primary_error": primary_error, "fallback_error": fallback_error}


async def reverse_with_fallback(client: httpx.AsyncClient, lat: float, lng: float):
    primary, primary_error = await reverse_opencage(client, lat, lng)
    if primary is not None:
        return primary, None

    fallback, fallback_error = await reverse_mapbox(client, lat, lng)
    if fallback is not None:
        return fallback, None

    return None, {"error": "All providers failed", "primary_error": primary_error, "fallback_error": fallback_error}


@app.get("/v1/geocode")
async def geocode(address: str = Query(..., min_length=2)):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await geocode_with_fallback(client, address, limit=1)
        if result is not None:
            return result[0]
        return error


@app.get("/v1/reverse")
async def reverse(lat: float, lng: float):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await reverse_with_fallback(client, lat, lng)
        if result is not None:
            return result
        return error


@app.post("/v1/batch/geocode")
async def batch_geocode(payload: BatchGeocodeRequest):
    output = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for address in payload.addresses:
            result, error = await geocode_with_fallback(client, address, limit=1)
            if result is not None:
                output.append({
                    "input": address,
                    "result": result[0],
                })
            else:
                output.append({
                    "input": address,
                    "error": error,
                })
    return {"count": len(output), "results": output}


@app.get("/v1/autocomplete")
async def autocomplete(q: str = Query(..., min_length=2), limit: int = Query(5, ge=1, le=10)):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await geocode_with_fallback(client, q, limit=limit)
        if result is not None:
            return {"query": q, "suggestions": result}
        return error


@app.get("/v1/distance")
async def distance(lat1: float, lng1: float, lat2: float, lng2: float):
    meters = haversine_meters(lat1, lng1, lat2, lng2)
    return {
        "lat1": lat1,
        "lng1": lng1,
        "lat2": lat2,
        "lng2": lng2,
        "distance_meters": round(meters, 2),
        "distance_km": round(meters / 1000.0, 3),
    }


@app.get("/v1/usage")
async def usage():
    return {
        "service": "Extended Geocoding API",
        "providers": {
            "opencage_configured": bool(OPENCAGE_API_KEY),
            "mapbox_configured": bool(MAPBOX_API_KEY),
        },
        "limits": {
            "batch_max_addresses": 50,
            "autocomplete_max_limit": 10,
        },
    }


@app.post("/v1/normalize")
async def normalize(payload: NormalizeRequest):
    normalized = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for address in payload.addresses:
            result, error = await geocode_with_fallback(client, address, limit=1)
            if result is not None:
                normalized.append({
                    "input": address,
                    "normalized_address": result[0]["address"],
                    "latitude": result[0]["latitude"],
                    "longitude": result[0]["longitude"],
                    "provider": result[0]["provider"],
                })
            else:
                normalized.append({
                    "input": address,
                    "error": error,
                })
    return {"count": len(normalized), "results": normalized}
