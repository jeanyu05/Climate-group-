import requests
import json
import sqlite3
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Optional
Stateid_list = ['MI','OH','IN','IL','WI','MN','IA','MO','KY','TN',
                'PA','WV','VA','MD','DE','NY','NJ','CT','MA','RI',
                'VT','NH','ME','SD','ND']


DB_PATH = 'climate_data.db'

def eia_city_data(stateidlst: List[str], api_key: str, start: str = "2013", end: str = "2023"):
    """Fetch EIA SEDS consumption 'TETCB' series for states and write EIA_data.json."""
    all_states = {}
    url = 'https://api.eia.gov/v2/seds/data/'
    for sid in stateidlst:
        payload = {
            "frequency": "annual",
            "data": ["value"],
            "facets": {
                "stateId": [sid],
                "seriesId": ["TETCB"]
            },
            "start": start,
            "end": end,
            "sort": [{"column": "period", "direction": "desc"}],
            "offset": 0,
            "length": 5000
        }
        try:
            resp = requests.post(url, params={"api_key": api_key}, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            all_states[sid] = data.get('response', {}).get('data', [])
            time.sleep(0.15)
        except Exception:
            continue

    with open('EIA_data.json', 'w') as f:
        json.dump(all_states, f, indent=2)

#db helpers and insertions 
def init_eia_table(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS eia_consumption (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT NOT NULL,
        year INTEGER NOT NULL,
        series_id TEXT,
        value REAL,
        units TEXT,
        UNIQUE(state, year, series_id)
    )""")
    conn.commit()
    conn.close()

def insert_eia_from_json(json_path: str = 'EIA_data.json', db_path: str = DB_PATH, max_inserts: int = 25) -> int:
    """Read EIA_data.json and insert up to max_inserts rows into eia_consumption (no duplicates)."""
    init_eia_table(db_path)
    with open(json_path, 'r') as f:
        raw = json.load(f)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    inserted = 0
    for state, records in raw.items():
        if inserted >= max_inserts:
            break
        if not isinstance(records, list):
            continue
        for item in records:
            if inserted >= max_inserts:
                break
            year_raw = item.get('period') or item.get('year')
            try:
                year = int(str(year_raw)[:4])
            except Exception:
                continue
            series_id = item.get('seriesId') or item.get('series_id') or item.get('series')
            try:
                value = float(item.get('value')) if item.get('value') is not None else None
            except Exception:
                value = None
            units = item.get('units') or item.get('unit')
            cur.execute("""
                INSERT OR IGNORE INTO eia_consumption (state, year, series_id, value, units)
                VALUES (?, ?, ?, ?, ?)
            """, (state, year, series_id, value, units))
            if cur.rowcount:
                inserted += 1
    conn.commit()
    conn.close()
    return inserted





