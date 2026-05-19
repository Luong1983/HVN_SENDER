
from fastapi import FastAPI, Request, HTTPException, Query, Header
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from .database import get_db_connection, init_db, get_recent_data, get_data_by_date, get_trend_data, execute_query
from .mqtt_handler import start_mqtt
import time
import logging
import asyncio
import os
from pathlib import Path
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.publish as publish
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta
import io
import csv
from typing import Dict
import zoneinfo
from app import state
import psycopg2


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn")
BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI()
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")

# Matches your folder structure: app/, static/, templates/ are siblings
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

@app.on_event("startup")

async def startup_event():
    # --- RESILIENT STARTUP LOGIC ---
    # Increased to 20 retries to give Postgres plenty of time to boot
    max_retries = 20
    retry_count = 0
    db_connected = False

    while retry_count < max_retries and not db_connected:
        try:
            logger.info(f"🔄 Connecting to DB (Attempt {retry_count + 1}/{max_retries})...")
            init_db()
            db_connected = True
            logger.info("✅ Database is ready and initialized!")
        except Exception as e:
            retry_count += 1
            # This catches the "database system is starting up" error
            logger.warning(f"⚠️ Database not ready yet ({e}). Retrying in 3s...")
            await asyncio.sleep(3) # Non-blocking sleep

    if not db_connected:
        logger.error("❌ CRITICAL: Could not connect to database after 20 attempts.")
        raise SystemExit(1)

    # Only start MQTT once the database is confirmed ready
    init_db()
    start_mqtt()
    logger.info("✅ MQTT Handler started.")

@app.get("/")
async def index(request: Request):
    # FIXED: Using explicit keyword arguments to avoid 'unhashable type' error
    return templates.TemplateResponse(
        request=request, 
        name="index.html", 
        context={}
    )

@app.get("/api/data")
async def data_api(hours: float = 1.0):
    from .database import get_recent_data
    return get_recent_data(hours)


@app.get("/api/trend/{sensor}/{hours}")
async def get_sensor_trend(sensor: str, hours: int = 24):
    data = get_trend_data(sensor, hours)
    return data


@app.post("/api/control/led/{state}")
async def control_led(state: str):
    target_state = state.upper() # 'ON' or 'OFF'
    
    if target_state not in ["ON", "OFF"]:
        raise HTTPException(status_code=400, detail="Invalid state. Use ON or OFF.")
    
    try:
        # 🎯 CRITICAL: This topic must match your ESP32 code exactly!
        publish.single(
            "cmnd/esp32/led", 
            payload=target_state, 
            hostname=MQTT_BROKER
            #port=1883
        )
        print(f"📡 MQTT Published: {target_state} to cmnd/esp32/led")
        return {"status": "success", "sent": target_state}
    
    except Exception as e:
        print(f"❌ MQTT Connection Error: {e}")
        raise HTTPException(status_code=500, detail="MQTT Broker unreachable")

# @app.get("/api/latest-data")
# async def get_latest():
    # res = db.query(Reading).order_by(Reading.id.desc()).first()
    # if not res:
        # return {"error": "No data in database yet", "temp": 0, "hum": 0, "light": 0}
    # return {
        # "temp": res.temperature,
        # "hum": res.humidity,
        # "pres": res.pressure,
        # "ws": res.ws,   # Ensure this matches your database column name
        # "wd": res.wd,
        # "light": res.light,
        # "uv": res.uv,
        # "pm25": res.pm25,
        # "co": res.co,
        # "lat": res.lat,
        # "lng": res.lng,
        # "vote": res.vote
    # }

@app.get("/api/latest-data")
async def get_latest():
    try:
        # 1. Establish connection (matching your Docker service name)
        conn = psycopg2.connect(
            host="db",        
            database="weather_station", 
            user="admin",         
            password="secretpassword"      
        )
        cur = conn.cursor()
        
        # 2. Fetch the most recent entry
        # Note: I'm using 'weather_history' as the table name from your previous code
        query = "SELECT * FROM weather_logs ORDER BY timestamp DESC LIMIT 2"
        cur.execute(query)
        rows = cur.fetchall()
        print(rows)
        
        cur.close()
        conn.close()
        if not rows:
            return {"error": "No data"}
        latest = rows[0]
        vote_value = latest[-1]
        if vote_value is None and len(rows) > 1:
            vote_value = rows[1][-1]
        
        # Default to 0 if still None
        final_vote = vote_value if vote_value is not None else -1  
        return {
            "temp": latest[2],
            "hum": latest[3],
            "ws": latest[4],
            "wd": latest[5],
            "pres": latest[6],
            "light": latest[7],
            "uv": latest[8],
            "pm25": latest[9],
            "lat": latest[10],
            "lng": latest[11], 
            "co": latest[13], # Safety check for column count
            "vote": final_vote
        }

    except Exception as e:
        print(f"❌ API Error: {e}")
        return {"error": str(e)}
        

@app.get("/api/data-by-date")
async def get_weather_by_date(date: str):
    try:
        # 1. Your SQL Query
        query = """
            SELECT *, 
            to_char(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh', 'HH24:MI') as time
            FROM weather_logs 
            WHERE (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = %s
            ORDER BY timestamp ASC;
        """
        
        # 2. Use execute_query and make sure it accepts the 'date' param
        # The (date,) MUST be in parentheses with a comma!
        rows = execute_query(query, (date,))
        
        if rows is None:
            return []
            
        return rows
    except Exception as e:
        print(f"🔥 Python Crashed: {e}")
        return {"error": str(e)}, 500
        
# @app.get("/api/history/{sensor}/{date_str}")
# async def history_api(sensor: str, date_str: str):    
    # data = get_data_by_date(date_str)     
    # return data

# @app.get("/api/history/{date_str}")
# async def history_api(date_str: str):
    # # This matches the /api/history/YYYY-MM-DD call from your logs
    # from .database import get_data_by_date
    # data = get_data_by_date(date_str)
    # return data

# 🎯 1. Specific Sensor History
@app.get("/api/history/sensor/{sensor}/{date_str}")
async def get_sensor_history(sensor: str, date_str: str):
    print(f"📡 Sensor Route -> {sensor} on {date_str}")
    # Ensure get_data_by_date handles the timezone shift and 'AS value' alias!
    return get_data_by_date(sensor, date_str)

# 🎯 2. General/All Sensors History
@app.get("/api/history/general/{date_str}")
async def get_general_history(date_str: str):
    print(f"📡 General Route -> Data for {date_str}")
    # This might return all columns for that date
    return get_data_by_date(None, date_str)

HANOI_TZ = zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")
    
@app.get("/api/export")
def export_sensor_data(
    date: str = Query(...),
    range: str = Query(...),
    cols: str = Query(...),
    x_admin_key: str = Header(None)):
    
    if x_admin_key != state.ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized: Data export is restricted.")
        
    
    global current_location, use_phone_gps
    
    try:
        # 1. Define the Time Window for ALL ranges
        base_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=HANOI_TZ)
        end_time = base_date + timedelta(days=1)
        
        if range == "current":
            # For 'current', we look at the 24h window ending NOW
            end_time = datetime.now(HANOI_TZ)
            start_time = end_time - timedelta(hours=24)
        else:
            # Historical ranges based on the selected date
            end_time = base_date + timedelta(days=1)
            if range == "day":
                start_time = base_date
            elif range == "week":
                start_time = end_time - timedelta(days=7)
            elif range == "month":
                start_time = end_time - timedelta(days=30)
            elif range == "year":
                start_time = end_time - timedelta(days=365)
            elif range == "alltime":
                start_time = datetime(2026,1,1, tzinfo=HANOI_TZ)
                end_time = datetime.now(HANOI_TZ) + timedelta(days=1)
            else:
                raise HTTPException(status_code=400, detail="Invalid range")

        # 2. Logic shared by ALL ranges starts here
        allowed_fields = [
            "temperature", "humidity", "ws", "wd", 
            "pressure", "light", "uv", "pm25", "co", "gps", "vote"
        ]
        
        selected_fields = [c.strip() for c in cols.split(",") if c.strip() in allowed_fields]
        query_cols = ["timestamp"]
        for part in selected_fields:
            if part == "gps":
                query_cols.extend(["lat", "lng"])
            else:
                query_cols.append(part)
        
        column_string = ", ".join(query_cols)
        
        start_time_utc = start_time.astimezone(zoneinfo.ZoneInfo("UTC"))
        end_time_utc = end_time.astimezone(zoneinfo.ZoneInfo("UTC"))
        
        query = f"SELECT {column_string} FROM weather_logs WHERE timestamp >= %s AND timestamp < %s ORDER BY timestamp ASC"
        
        params = (start_time_utc.strftime("%Y-%m-%d %H:%M:%S"), end_time_utc.strftime("%Y-%m-%d %H:%M:%S"))
        rows = execute_query(query, params)

        if not rows:
            raise HTTPException(status_code=404, detail="No data found for this range")
        
        # 3. 🎯 THE FIX: Convert Database UTC timestamps to Hanoi Local Time
        for row in rows:
            if "timestamp" in row and isinstance(row["timestamp"], datetime):
                # If DB returns naive UTC, replace tzinfo; then convert to Hanoi
                if row["timestamp"].tzinfo is None:
                    row["timestamp"] = row["timestamp"].replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                
                # Convert to local time and format as a clean string for the CSV
                row["timestamp"] = row["timestamp"].astimezone(HANOI_TZ).strftime("%Y-%m-%d %H:%M:%S")
            
            # Round GPS for professional CSV output (matches image_6600fb.png)
            if "lat" in row and row["lat"] is not None:
                row["lat"] = round(float(row["lat"]), 4)
            if "lng" in row and row["lng"] is not None:
                row["lng"] = round(float(row["lng"]), 4)
                
        # 3. CSV Construction
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=query_cols)
        
        pretty_names = {
            "timestamp": "Timestamp", "temperature": "Temperature", 
            "humidity": "Humidity", "ws": "Wind Speed", "wd": "Wind Dir",
            "pressure": "Pressure", "light": "Light", "uv": "UV", 
            "pm25": "PM2.5", "lat": "Latitude", "lng": "Longitude", "co": "CO",
            "vote": "Comfort Sensation"  # 🎯 Clean CSV Column Header
        }
        
        header_row = {col: pretty_names.get(col, col.capitalize()) for col in query_cols}
        writer.writerow(header_row) 
        writer.writerows(rows)
        output.seek(0)

        filename = f"hanoi_export_{range}_{date}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# current_location = {"lat": 21.0285, "lng": 105.8542}
# use_phone_gps = False
        
class GPSData(BaseModel):
    lat: float
    lng: float

@app.get("/api/get-location")
async def get_location():
    global current_location
    try:
        # Returning the dictionary directly as JSON
        return current_location
    except Exception as e:
        # This prevents the "Internal Server Error" text
        raise HTTPException(status_code=500, detail=str(e))

    
@app.post("/api/update-location")
async def update_location(data: GPSData, x_admin_key: str = Header(None)):
    if x_admin_key != state.ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized: Only the Owner can sync GPS.")
    state.current_location["lat"] = data.lat
    state.current_location["lng"] = data.lng
    state.use_phone_gps = True 
    print(f"📍 GPS Sync successful: {state.current_location}")
    return {"status": "success"}
    

def save_data_to_db_GPS():
    global current_location
    
    try:
        # 🎯 THE FIX: Pull coordinates from the global variable updated by your phone
        log_lat = current_location["lat"]
        log_lng = current_location["lng"]
        
        # Use the Hanoi Timezone we configured
        now_hanoi = datetime.now(HANOI_TZ).strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO weather_logs (timestamp, lat, lng)
            VALUES (%s, %s, %s)
        """
        values = (now_hanoi, log_lat, log_lng)
        
        execute_query(query, values)
        print(f"✅ Logged at {log_lat}, {log_lng}")
        
    except Exception as e:
        logger.error(f"Database Save Error: {e}")
        


# --- Update this in your main.py ---
@app.post("/api/sensor-data")
async def receive_sensor_data(packet: dict):
    global current_location, use_phone_gps
    
    # 1. 🎯 THE COORDINATE OVERRIDE
    if state.use_phone_gps:
        # Use the high-precision phone coordinates
        packet["lat"] = state.current_location["lat"]
        packet["lng"] = state.current_location["lng"] 
        print(f"📱 [PHONE GPS] Active Sync: {packet['lat']}, {packet['lng']}")
    else:
        # Fallback to the lab defaults if phone hasn't synced yet
        packet["lat"] = packet.get("lat", state.current_location["lat"])
        packet["lng"] = packet.get("lng", state.current_location["lng"])
        print(f"📟 [HARDWARE GPS] Using Default: {packet['lat']}, {packet['lng']}")
        
    # 2. SAVE ALL CLASSROOM PARAMETERS
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Expanded query to include all fields used by your Dashboard and Exporter
        query = """
            INSERT INTO weather_logs (
                timestamp, temperature, humidity, ws, wd, 
                pressure, light, uv, pm25, co, lat, lng, vote
            )
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Safely extract values from the packet sent by ESP32
        values = (
            packet.get("temperature", 0), 
            packet.get("humidity", 0),
            packet.get("ws", 0), 
            packet.get("wd", 0),
            packet.get("pressure", 1013), 
            packet.get("light", 0),
            packet.get("uv", 0), 
            packet.get("pm25", 0),
            packet.get("co", 0),
            packet.get("lat"), 
            packet.get("lng"),
            packet.get("vote")
        )
        
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Database Logging Failed: {e}")

    return {"status": "success", "lat_logged": packet["lat"]}



class ComfortVote(BaseModel):
    vote: int  # 0: Cold, 1: Comfortable, 2: Hot

# 1. End point to merge web votes into the latest ESP32 database entry
@app.post("/api/submit-vote")
async def submit_vote(data: ComfortVote):
    conn = None
    cursor = None
    try:
        # Open an independent database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. First, check if there's at least one row in the database
        cursor.execute("SELECT id FROM weather_logs ORDER BY time DESC LIMIT 1;")
        latest_row = cursor.fetchone()
        
        if not latest_row:
            logger.warning("⚠️ Cannot vote: No sensor logs found in database yet.")
            raise HTTPException(status_code=400, detail="Database is empty. Send sensor data first.")
        
        # Extract the ID safely (supports both dictionary and tuple cursors)
        latest_id = latest_row['id'] if isinstance(latest_row, dict) else latest_row[0]
        
        # 2. Update that specific latest row with the vote
        query = "UPDATE weather_logs SET vote = %s WHERE id = %s"
        cursor.execute(query, (data.vote, latest_id))
        
        # 3. 🚀 CRITICAL: Force PostgreSQL to write the transaction to disk
        conn.commit()
        
        logger.info(f"🗳️ Web vote {data.vote} successfully merged into row ID {latest_id}")
        return {"status": "success", "message": "Vote recorded!"}
        
    except Exception as e:
        if conn:
            conn.rollback() # Undo changes if anything crashed
        logger.error(f"❌ Failed to merge vote: {e}")
        raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# 2. Endpoint to fetch logs where attendees actively submitted feedback
@app.get("/api/votes")
async def get_votes(limit: int = Query(10)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query only the entries where an active vote was registered
        query = """
            SELECT id, 
                   to_char(time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh', 'YYYY-MM-DD HH24:MI:SS') as time, 
                   vote 
            FROM weather_logs 
            WHERE vote IS NOT NULL
            ORDER BY time DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return rows if rows else []
        
    except Exception as e:
        logger.error(f"❌ Error fetching logged votes: {e}")
        raise HTTPException(status_code=500, detail="Database read failed")

from fastapi.responses import HTMLResponse

@app.get("/vote", response_class=HTMLResponse)
async def serve_mobile_vote_page():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Classroom Climate Feedback</title>
        <style>
            body {
                background-color: #0c0c0e;
                color: #ffffff;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
                margin: 0;
                padding: 20px;
                box-sizing: border-box;
            }
            .container {
                text-align: center;
                width: 100%;
                max-width: 400px;
            }
            h1 {
                font-size: 24px;
                margin-bottom: 8px;
                font-weight: 700;
            }
            p {
                color: #888;
                font-size: 14px;
                margin-bottom: 40px;
            }
            .button-stack {
                display: flex;
                flex-direction: column;
                gap: 16px;
                width: 100%;
            }
            .vote-btn {
                border: none;
                padding: 20px;
                border-radius: 16px;
                font-size: 18px;
                font-weight: bold;
                color: white;
                cursor: pointer;
                transition: transform 0.1s active;
                display: flex;
                justify-content: center;
                align-items: center;
                gap: 12px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            }
            .vote-btn:active {
                transform: scale(0.96);
            }
            .cold-btn { background: linear-gradient(135deg, #2980b9, #3498db); }
            .comfy-btn { background: linear-gradient(135deg, #27ae60, #2ecc71); }
            .hot-btn { background: linear-gradient(135deg, #c0392b, #e74c3c); }
            
            #status-overlay {
                display: none;
                position: fixed;
                top: 0; left: 0; right: 0; bottom: 0;
                background: rgba(12, 12, 14, 0.95);
                justify-content: center;
                align-items: center;
                flex-direction: column;
                z-index: 100;
            }
            .success-icon {
                font-size: 64px;
                margin-bottom: 16px;
                animation: pop 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            }
            @keyframes pop {
                0% { transform: scale(0); }
                100% { transform: scale(1); }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Classroom Comfort Poll</h1>
            <p>How does the room temperature feel right now?</p>
            
            <div class="button-stack">
                <button onclick="sendVote(0)" class="vote-btn cold-btn">❄️ Too Cold</button>
                <button onclick="sendVote(1)" class="vote-btn comfy-btn">💚 Comfortable</button>
                <button onclick="sendVote(2)" class="vote-btn hot-btn">🔥 Too Hot</button>
            </div>
        </div>

        <div id="status-overlay">
            <div class="success-icon">🎉</div>
            <h2 id="status-message">Vote Submitted!</h2>
            <p>Thank you for helping optimize our classroom AI.</p>
        </div>

        <script>
            async function sendVote(value) {
                const overlay = document.getElementById('status-overlay');
                const message = document.getElementById('status-message');
                
                try {
                    const response = await fetch('/api/submit-vote', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ vote: value })
                    });
                    
                    if (response.ok) {
                        overlay.style.display = 'flex';
                        // Auto-close the success screen after 2.5 seconds
                        setTimeout(() => {
                            overlay.style.display = 'none';
                        }, 2500);
                    } else {
                        alert("Submission failed. The class session might not be active.");
                    }
                } catch (err) {
                    alert("Network error. Make sure you are connected to the campus WiFi.");
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)
