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
        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={GEOCODING_API_KEY}"
        resp = await client.get(url)
        data = resp.json()
        if not data.get("results"):
            return {"error": "No results found"}
        location = data["results"][0]["geometry"]["location"]
        return {
            "address": address,
            "latitude": location["lat"],
            "longitude": location["lng"]
        }

@app.get("/v1/reverse")
async def reverse(lat: float, lng: float):
    async with httpx.AsyncClient() as client:
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={GEOCODING_API_KEY}"
        resp = await client.get(url)
        data = resp.json()
        if not data.get("results"):
            return {"error": "No results found"}
        return {
            "latitude": lat,
            "longitude": lng,
            "address": data["results"][0]["formatted_address"]
        }
