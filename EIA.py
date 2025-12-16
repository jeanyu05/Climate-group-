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
#analysis and plotting

def load_eia_df(db_path: str = DB_PATH) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT state, year, series_id, value, units FROM eia_consumption", conn)
    conn.close()
    return df

def compute_eia_state_consumption(eia_df: pd.DataFrame, start_year: int = 2013, end_year: int = 2023) -> pd.DataFrame:
    mask = (eia_df['year'] >= start_year) & (eia_df['year'] <= end_year)
    sub = eia_df[mask].copy()
    results = []
    for state, g in sub.groupby('state'):
        g = g.dropna(subset=['value'])
        if g.empty:
            continue
        years = g['year'].astype(int).values
        vals = g['value'].astype(float).values
        avg = float(np.nanmean(vals))
        slope = 0.0
        trend = 'flat'
        if len(years) >= 2 and np.isfinite(vals).all():
            slope, intercept = np.polyfit(years, vals, 1)
            if slope > 1e-6 * max(1, abs(avg)):
                trend = 'rising'
            elif slope < -1e-6 * max(1, abs(avg)):
                trend = 'falling'
        results.append({'state': state, 'avg_consumption': avg, 'slope': slope, 'trend': trend})
    res = pd.DataFrame(results).set_index('state').sort_values('avg_consumption', ascending=False)
    return res

def plot_avg_energy_bar(eia_summary_df: pd.DataFrame, top_n: int = 15, figsize=(10,6), title: Optional[str] = None):
    if title is None:
        title = f'Average Energy Consumption per State ({top_n} highest, 2013-2023)'
    df = eia_summary_df.sort_values('avg_consumption', ascending=False).head(top_n)
    plt.figure(figsize=figsize)
    bars = plt.bar(df.index, df['avg_consumption'], color='C2')
    plt.ylabel('Average Consumption (units as stored)')
    plt.title(title)
    plt.xticks(rotation=45, ha='right')
    for i, bar in enumerate(bars[:5]):
        bar.set_edgecolor('black')
        bar.set_linewidth(1.2)
    plt.tight_layout()
    plt.show()

def plot_top5_states(eia_summary_df: pd.DataFrame, figsize=(8,5)):
    top5 = eia_summary_df.head(5)
    plt.figure(figsize=figsize)
    plt.bar(top5.index, top5['avg_consumption'], color=['#d62728','#ff7f0e','#2ca02c','#1f77b4','#9467bd'])
    plt.title('Top 5 Energy Consuming States (avg 2013-2023)')
    plt.ylabel('Average Consumption')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

def plot_state_trend(state: str, eia_df: pd.DataFrame, start_year: int = 2013, end_year: int = 2023, figsize=(10,5)):
    mask = (eia_df['state'] == state) & (eia_df['year'] >= start_year) & (eia_df['year'] <= end_year)
    df = eia_df[mask].dropna(subset=['value']).copy()
    if df.empty:
        raise ValueError(f'No EIA data for state {state} in range')
    df = df.sort_values('year')
    years = df['year'].astype(int).values
    vals = df['value'].astype(float).values
    plt.figure(figsize=figsize)
    plt.plot(years, vals, marker='o', label='annual value')
    if len(years) >= 2:
        slope, intercept = np.polyfit(years, vals, 1)
        plt.plot(years, intercept + slope * years, linestyle='--', color='gray', label=f'trend (slope={slope:.2f})')
    plt.title(f'Energy Consumption Trend: {state} ({start_year}-{end_year})')
    plt.xlabel('Year')
    plt.ylabel('Consumption')
    plt.grid(alpha=0.2)
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    # Insert up to 25 rows from EIA_data.json into database
    try:
        n = insert_eia_from_json('EIA_data.json', DB_PATH, max_inserts=25)
        print(f'Inserted {n} EIA rows.')
    except FileNotFoundError:
        print('EIA_data.json not found. Run eia_city_data(...) first.')
    # Load and plot if DB has data
    eia_df = load_eia_df(DB_PATH)
    if not eia_df.empty:
        summary = compute_eia_state_consumption(eia_df)
        print(summary.head(5)[['avg_consumption','trend']])
        plot_avg_energy_bar(summary, top_n=12)
        plot_top5_states(summary)
        plot_state_trend(summary.index[0], eia_df)