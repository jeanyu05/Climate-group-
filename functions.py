import json
import datetime
import sqlite3
import numpy as np
from typing import Tuple


DB = 'climate_data.db' 

# helpers to create DB and table 
def get_conn(db: str = DB) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    return cur, conn

def init_tables(cur, conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Dates (
            date_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS AirQuality (
            air_quality_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            date_id INTEGER NOT NULL,
            aqi INTEGER,
            co REAL,
            no REAL,
            no2 REAL,
            o3 REAL,
            so2 REAL,
            pm2_5 REAL,
            pm10 REAL,
            nh3 REAL,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id),
            UNIQUE(city_id, date_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Weather (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            date_id INTEGER NOT NULL,
            temp_mean REAL,
            rh_mean REAL,
            wind_mean REAL,
            UNIQUE(city_id, date_id),
            FOREIGN KEY (city_id) REFERENCES Cities(city_id),
            FOREIGN KEY (date_id) REFERENCES Dates(date_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS eia_consumption (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL,
            year INTEGER NOT NULL,
            series_id TEXT,
            value REAL,
            units TEXT,
            UNIQUE(state, year, series_id)
        )
    ''')
    conn.commit()

def _normalize_aq_timestamps(data):
    # convert dt epoch to local YYYY-MM-DD HH:MM:SS strings using timezone offsets used earlier
    for city, payload in data.items():
        for rec in payload.get('list', []):
            if 'dt' in rec:
                # keep ISO date with only date part for aggregation
                ts = int(rec['dt'])
                dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                rec['iso'] = dt.isoformat()
    return data

def combine_air_quality_data(data):
    # aggregate daily averages grouped by city and date (YYYY-MM-DD)
    out = {}
    for city, payload in data.items():
        rows = payload.get('list', [])
        for rec in rows:
            iso = rec.get('iso')
            if not iso:
                continue
            date = iso[:10]
            key = f"{city} {date}"
            if key not in out:
                out[key] = {'city': city, 'date': date, 'aqi': [], 'co': [], 'no': [], 'no2': [], 'o3': [], 'so2': [], 'pm2_5': [], 'pm10': [], 'nh3': []}
            main_aqi = rec.get('main', {}).get('aqi')
            comps = rec.get('components', {})
            if main_aqi is not None:
                out[key]['aqi'].append(main_aqi)
            for k in ('co','no','no2','o3','so2','pm2_5','pm10','nh3'):
                if k in comps:
                    out[key][k].append(comps.get(k))

    for k,v in out.items():
        for m in ['aqi','co','no','no2','o3','so2','pm2_5','pm10','nh3']:
            v[m] = float(np.nanmean(v[m])) if len(v[m])>0 else None
    return out
def insert_airquality_from_json(filename: str, db: str = DB, max_inserts: int = 25) -> int:
    with open(filename, 'r') as f:
        raw = json.load(f)
    raw = _normalize_aq_timestamps(raw)
    agg = combine_air_quality_data(raw)
    cur, conn = get_conn(db)
    init_tables(cur, conn)
    inserted = 0
    for key, v in agg.items():
        if inserted >= max_inserts:
            break
        city = v['city']
        date = v['date']
        cur.execute("INSERT OR IGNORE INTO Cities (city_name) VALUES (?)", (city,))
        cur.execute("SELECT city_id FROM Cities WHERE city_name = ?", (city,))
        city_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO Dates (date) VALUES (?)", (date,))
        cur.execute("SELECT date_id FROM Dates WHERE date = ?", (date,))
        date_id = cur.fetchone()[0]
        cur.execute("""
            INSERT OR IGNORE INTO AirQuality (city_id, date_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (city_id, date_id, v['aqi'], v['co'], v['no'], v['no2'], v['o3'], v['so2'], v['pm2_5'], v['pm10'], v['nh3']))
        if cur.rowcount:
            inserted += 1
    conn.commit()
    conn.close()
    return inserted

def insert_openmeteo_json(filename: str, db: str = DB, max_inserts: int = 25) -> int:
    with open(filename, 'r') as f:
        raw = json.load(f)
    cur, conn = get_conn(db)
    init_tables(cur, conn)
    inserted = 0
    for city, payload in raw.items():
        if inserted >= max_inserts:
            break
        daily = payload.get('daily', {})
        times = daily.get('time', [])
        temps = daily.get('temperature_2m_mean', [])
        rhs = daily.get('relative_humidity_2m_mean', [])
        winds = daily.get('wind_speed_10m_mean', [])
        for i, date in enumerate(times):
            if inserted >= max_inserts:
                break
            temp = temps[i] if i < len(temps) else None
            rh = rhs[i] if i < len(rhs) else None
            wind = winds[i] if i < len(winds) else None
            cur.execute("INSERT OR IGNORE INTO Cities (city_name) VALUES (?)", (city,))
            cur.execute("SELECT city_id FROM Cities WHERE city_name = ?", (city,))
            city_id = cur.fetchone()[0]
            cur.execute("INSERT OR IGNORE INTO Dates (date) VALUES (?)", (date,))
            cur.execute("SELECT date_id FROM Dates WHERE date = ?", (date,))
            date_id = cur.fetchone()[0]
            cur.execute("""
                INSERT OR IGNORE INTO Weather (city_id, date_id, temp_mean, rh_mean, wind_mean)
                VALUES (?, ?, ?, ?, ?)
            """, (city_id, date_id, temp, rh, wind))
            if cur.rowcount:
                inserted += 1
    conn.commit()
    conn.close()
    return inserted

def insert_eia_json_to_db(json_path: str = 'EIA_data.json', db: str = DB, max_inserts:int = 25) -> int:
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS eia_consumption (id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL, year INTEGER NOT NULL, series_id TEXT, value REAL, units TEXT, UNIQUE(state, year, series_id))')
    conn.commit()
    inserted = 0
    with open(json_path, 'r') as f:
        raw = json.load(f)
    for state, recs in raw.items():
        if inserted >= max_inserts:
            break
        if not isinstance(recs, list):
            continue
        for item in recs:
            if inserted >= max_inserts:
                break
            year_raw = item.get('period') or item.get('year')
            try:
                year = int(str(year_raw)[:4])
            except Exception:
                continue
            series = item.get('seriesId') or item.get('series_id') or item.get('series')
            try:
                value = float(item.get('value')) if item.get('value') is not None else None
            except Exception:
                value = None
            units = item.get('units') or item.get('unit')
            cur.execute("""INSERT OR IGNORE INTO eia_consumption (state, year, series_id, value, units) VALUES (?, ?, ?, ?, ?)""",
                        (state, year, series, value, units))
            if cur.rowcount:
                inserted += 1
        conn.commit()
        conn.close()
        return inserted
