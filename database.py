
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# DATABASE_URL must use 'db' to match the service name in docker-compose.yml
DATABASE_URL = "postgresql://postgres.bgksinwjsrkjymhmzsks:H%23u%2ENg3YfR8yF%24Y@aws-1-ap-northeast-1.pooler.supabase.com:6543/postgres?pgbouncer=true"


def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # We use TIMESTAMPTZ (Timezone Aware) to handle Hanoi time correctly
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_logs (
            id SERIAL PRIMARY KEY,
            time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            temperature REAL NOT NULL,
            humidity REAL NOT NULL,
            ws REAL DEFAULT 0,
            wd REAL DEFAULT 0,
            pressure REAL DEFAULT 0,
            light REAL DEFAULT 0,
            uv REAL DEFAULT 0,
            pm25 REAL DEFAULT 0,
            lat FLOAT DEFAULT 21.0054,
            lng FLOAT DEFAULT 105.9317,
            vote INT DEFAULT 0
        )
    ''')
    
    # Ensure lat/lng columns exist even if the table was created earlier without them
    cursor.execute("""
        DO $$ 
        BEGIN 
            BEGIN
                ALTER TABLE weather_logs ADD COLUMN lat FLOAT DEFAULT 21.0054;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END;
            BEGIN
                ALTER TABLE weather_logs ADD COLUMN lng FLOAT DEFAULT 105.9317;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END;
        END $$;
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def save_reading(temp, hum, ws=0, wd=0, pres=0, lux=0, uv=0, pm=0, lat=0, lng=0, co=0, vote=None):
    if lat == 0 or lng == 0:
        lat = 21.0054 
        lng = 105.9317
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        INSERT INTO weather_logs (
            temperature, humidity, ws, wd, pressure, light, uv, pm25, co, lat, lng, vote
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(query, (temp, hum, ws, wd, pres, lux, uv, pm, co, lat, lng, vote))
        conn.commit()
    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")
    finally:
        cursor.close()
        conn.close()
    
def check_columns():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_logs LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]
    print(f"🔍 CURRENT DATABASE COLUMNS: {colnames}")
    cursor.close()
    conn.close()
    

def get_recent_data(hours=1):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Fixed: column name is 'time', not 'timestamp'
    query = """
        SELECT to_char(time + INTERVAL '7 hours', 'YYYY-MM-DD"T"HH24:MI:SS') as time, 
               temperature, humidity, ws, wd, pressure, light, uv, pm25, co, lat, lng, vote
        FROM weather_logs 
        ORDER BY id DESC LIMIT 1
    """
    cursor.execute(query)
    rows = cursor.fetchone()
    cursor.close()
    conn.close()
    return [rows] if rows else []

# def get_data_by_date(selected_date):
    # conn = get_db_connection()
    # cursor = conn.cursor()
    # try:
        # query = """
            # SELECT to_char((time AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Ho_Chi_Minh', 'HH24:MI') as time, 
                   # temperature, humidity, ws, wd, pressure, light, uv, pm25, co, lat, lng
            # FROM weather_logs 
            # WHERE ((time AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = %s
            # ORDER BY time ASC
        # """
        # cursor.execute(query, (selected_date,))
        # return cursor.fetchall()
    # finally:
        # cursor.close()
        # conn.close()
        
# def get_data_by_date(target_date):
    # # This query filters specifically for the date you picked
    # query = """
        # SELECT * FROM weather_logs 
        # WHERE (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = %s
        # ORDER BY timestamp ASC;
    # """
    # # %s is replaced by '2026-04-08' etc.
    # return execute_query(query, (target_date,))
    
    
def get_data_by_date(sensor, date_str):
    # If sensor is provided, we alias it to 'value' for the Chart
    column_selection = f"{sensor} AS value" if sensor else "*"
    
    query = f"""
        SELECT 
            timestamp, 
            {sensor} AS value 
        FROM weather_logs 
        WHERE (timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = %s
        ORDER BY timestamp ASC;
    """
    return execute_query(query, (date_str,))
        
def get_trend_data(sensor_name, hours=24):
    conn = get_db_connection()
    cur = conn.cursor()
    
    column_map = {
        "temperature": "temperature",
        "humidity": "humidity",
        "ws": "ws",
        "wd": "wd",
        "pressure": "pressure",
        "light": "light",
        "uv": "uv",
        "pm25": "pm25",     
        "co": "co",
        "lat": "lat",
        "lng": "lng",
        "vote": "vote"
    }
    
    # Standardize the name (remove spaces/lowercase)
    col = column_map.get(sensor_name.strip().lower())
    
    if not col:
        print(f"⚠️ Trend Warning: Sensor '{sensor_name}' not found in map.")
        return []

    try:
        # We use 'as val' so we always know the dictionary key is 'val'
        # query = f"""
            # SELECT (time + INTERVAL '7 hours') as hanoi_time, {col} as val
            # FROM weather_logs 
            # ORDER BY id DESC 
            # LIMIT 200
        # """
        query = f"""
            SELECT 
                to_char(
                    (time + INTERVAL '7 hours'), 
                    'HH24:MI:SS'
                ) as hanoi_time,
                AVG({col}) as val
            FROM weather_logs 
            WHERE time >= (NOW() - INTERVAL '{hours} hours')
            GROUP BY 1
            ORDER BY MIN(time) ASC
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        # KEY FIX: Access using r['key'], NOT r[0]
        data = [
            {
                "time": r['hanoi_time'],#.strftime("%H:%M:%S"), 
                "value": round(r['val'], 2) if r['val'] is not None else 0
            } 
            for r in rows
        ]
        
        # Reverse so the chart goes Left -> Right (Old -> New)
        #data.reverse()
        
        print(f"📈 Trend Success: Fetched {len(data)} averaged points for {sensor_name} over {hours}H")
        return data

    except Exception as e:
        # This will now show you EXACTLY why it failed in the terminal
        print(f"❌ DATABASE TREND ERROR: {e}")
        return []
    finally:
        cur.close()
        conn.close()
        
def execute_query(query, params=None): # Add 'params=None' here
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Pass the params into the execute method
            cur.execute(query, params) 
            
            if query.strip().upper().startswith("SELECT"):
                return cur.fetchall()
            conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
        return None
    finally:
        conn.close()
