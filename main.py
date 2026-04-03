from fastapi import FastAPI, Query
import httpx
import os

app = FastAPI(title="Geocoding API")

OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY", "")
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")


@app.get("/health")
def health():
    return {"status": "ok"}


async def geocode_opencage(client: httpx.AsyncClient, address: str):
    if not OPENCAGE_API_KEY:
        return None, {"error": "OPENCAGE_API_KEY is missing"}

    resp = await client.get(
        "https://api.opencagedata.com/geocode/v1/json",
        params={
            "q": address,
            "key": OPENCAGE_API_KEY,
            "limit": 1,
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
        return None, {
            "provider": "opencage",
            "error": "No results found",
            "raw": data,
        }

    first = results[0]
    geometry = first.get("geometry", {})
    return {
        "provider": "opencage",
        "address": first.get("formatted"),
        "latitude": geometry.get("lat"),
        "longitude": geometry.get("lng"),
    }, None


async def reverse_opencage(client: httpx.AsyncClient, lat: float, lng: float):
    if not OPENCAGE_API_KEY:
        return None, {"error": "OPENCAGE_API_KEY is missing"}

    resp = await client.get(
        "https://api.opencagedata.com/geocode/v1/json",
        params={
            "q": f"{lat},{lng}",
            "key": OPENCAGE_API_KEY,
            "limit": 1,
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
        return None, {
            "provider": "opencage",
            "error": "No results found",
            "raw": data,
        }

    first = results[0]
    return {
        "provider": "opencage",
        "latitude": lat,
        "longitude": lng,
        "address": first.get("formatted"),
    }, None


async def geocode_mapbox(client: httpx.AsyncClient, address: str):
    if not MAPBOX_API_KEY:
        return None, {"error": "MAPBOX_API_KEY is missing"}

    resp = await client.get(
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json",
        params={
            "access_token": MAPBOX_API_KEY,
            "limit": 1,
        },
    )
    data = resp.json()
    features = data.get("features", [])
    if not features:
        return None, {
            "provider": "mapbox",
            "error": "No results found",
            "raw": data,
        }

    first = features[0]
    lng, lat = first.get("center", [None, None])
    return {
        "provider": "mapbox",
        "address": first.get("place_name"),
        "latitude": lat,
        "longitude": lng,
    }, None


async def reverse_mapbox(client: httpx.AsyncClient, lat: float, lng: float):
    if not MAPBOX_API_KEY:
        return None, {"error": "MAPBOX_API_KEY is missing"}

    resp = await client.get(
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lng},{lat}.json",
        params={
            "access_token": MAPBOX_API_KEY,
            "limit": 1,
        },
    )
    data = resp.json()
    features = data.get("features", [])
    if not features:
        return None, {
            "provider": "mapbox",
            "error": "No results found",
            "raw": data,
        }

    first = features[0]
    return {
        "provider": "mapbox",
        "latitude": lat,
        "longitude": lng,
        "address": first.get("place_name"),
    }, None


@app.get("/v1/geocode")
async def geocode(address: str = Query(...)):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await geocode_opencage(client, address)
        if result is not None:
            return result

        fallback_result, fallback_error = await geocode_mapbox(client, address)
        if fallback_result is not None:
            return fallback_result

        return {
            "error": "All providers failed",
            "primary_error": error,
            "fallback_error": fallback_error,
        }


@app.get("/v1/reverse")
async def reverse(lat: float, lng: float):
    async with httpx.AsyncClient(timeout=20.0) as client:
        result, error = await reverse_opencage(client, lat, lng)
        if result is not None:
            return result

        fallback_result, fallback_error = await reverse_mapbox(client, lat, lng)
        if fallback_result is not None:
            return fallback_result

        return {
            "error": "All providers failed",
            "primary_error": error,
            "fallback_error": fallback_error,
        }
