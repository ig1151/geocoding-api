from fastapi import FastAPI, Query
import httpx
import os

app = FastAPI(title="Geocoding API")

OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY", "")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/geocode")
async def geocode(address: str = Query(...)):
    if not OPENCAGE_API_KEY:
        return {"error": "OPENCAGE_API_KEY is missing"}

    async with httpx.AsyncClient() as client:
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
            return {
                "error": data.get("status", {}).get("message", "UNKNOWN_ERROR"),
                "raw": data,
            }

        results = data.get("results", [])
        if not results:
            return {"error": "No results found", "raw": data}

        first = results[0]
        geometry = first.get("geometry", {})
        return {
            "address": first.get("formatted"),
            "latitude": geometry.get("lat"),
            "longitude": geometry.get("lng"),
        }


@app.get("/v1/reverse")
async def reverse(lat: float, lng: float):
    if not OPENCAGE_API_KEY:
        return {"error": "OPENCAGE_API_KEY is missing"}

    async with httpx.AsyncClient() as client:
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
            return {
                "error": data.get("status", {}).get("message", "UNKNOWN_ERROR"),
                "raw": data,
            }

        results = data.get("results", [])
        if not results:
            return {"error": "No results found", "raw": data}

        first = results[0]
        return {
            "latitude": lat,
            "longitude": lng,
            "address": first.get("formatted"),
        }
