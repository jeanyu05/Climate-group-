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