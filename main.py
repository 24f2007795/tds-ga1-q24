import requests
import sqlite3
import time
import json
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

app = FastAPI()

# ---------------- CORS + Health ----------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

@app.options("/{path:path}")
async def options_handler(path: str):
    return Response(status_code=200)

@app.get("/")
def health():
    return {"status": "ok"}

# ---------------- DATABASE SETUP ----------------

conn = sqlite3.connect("pipeline.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original TEXT,
    analysis TEXT,
    sentiment TEXT,
    source TEXT,
    timestamp TEXT
)
""")
conn.commit()

# ---------------- AI ENRICHMENT ----------------

def ai_analyze(text: str):
    # Simple deterministic AI simulation
    analysis = f"The generated UUID {text} represents a unique identifier typically used in distributed systems. It ensures uniqueness across systems and supports reliable tracking of resources."

    sentiment = "balanced"

    return analysis, sentiment

# ---------------- FETCH UUID ----------------

def fetch_uuid():
    try:
        response = requests.get("https://httpbin.org/uuid", timeout=5)
        response.raise_for_status()
        return response.json().get("uuid")
    except Exception as e:
        return None

# ---------------- PIPELINE ENDPOINT ----------------

@app.post("/")
async def pipeline(request: Request):

    try:
        body = await request.json()
    except:
        body = {}

    email = body.get("email", "unknown@example.com")
    source = body.get("source", "HTTPBin UUID")

    items = []
    errors = []

    for i in range(3):

        try:
            uuid_value = fetch_uuid()

            if not uuid_value:
                raise Exception("Failed to fetch UUID")

            analysis, sentiment = ai_analyze(uuid_value)

            timestamp = datetime.utcnow().isoformat() + "Z"

            # Store in DB
            cursor.execute("""
                INSERT INTO results (original, analysis, sentiment, source, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (uuid_value, analysis, sentiment, source, timestamp))

            conn.commit()

            items.append({
                "original": uuid_value,
                "analysis": analysis,
                "sentiment": sentiment,
                "stored": True,
                "timestamp": timestamp
            })

        except Exception as e:
            errors.append({
                "item": i,
                "error": str(e)
            })

    # ---------------- Notification ----------------

    try:
        # Simulated notification
        print(f"Notification sent to: 24f2007795@ds.study.iitm.ac.in")
        notification_sent = True
    except:
        notification_sent = False
        errors.append({"notification": "Failed to send notification"})

    processed_at = datetime.utcnow().isoformat() + "Z"

    return {
        "items": items,
        "notificationSent": notification_sent,
        "processedAt": processed_at,
        "errors": errors
    }

