from fastapi import FastAPI, Query
import httpx
import os

app = FastAPI(title="Geocoding API")

GEOCODING_API_KEY = os.getenv("GEOCODING_API_KEY", "")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/geocode")
async def geocode(address: str = Query(...)):
    async with httpx.AsyncClient() as client:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        resp = await client.get(url, params={"address": address, "key": GEOCODING_API_KEY})
        data = resp.json()

        if data.get("status") != "OK":
            return {
                "error": data.get("status", "UNKNOWN_ERROR"),
                "message": data.get("error_message"),
                "raw": data,
            }

        location = data["results"][0]["geometry"]["location"]
        return {
            "address": data["results"][0]["formatted_address"],
            "latitude": location["lat"],
            "longitude": location["lng"],
        }


@app.get("/v1/reverse")
async def reverse(lat: float, lng: float):
    async with httpx.AsyncClient() as client:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        resp = await client.get(url, params={"latlng": f"{lat},{lng}", "key": GEOCODING_API_KEY})
        data = resp.json()

        if data.get("status") != "OK":
            return {
                "error": data.get("status", "UNKNOWN_ERROR"),
                "message": data.get("error_message"),
                "raw": data,
            }

        return {
            "latitude": lat,
            "longitude": lng,
            "address": data["results"][0]["formatted_address"],
        }
