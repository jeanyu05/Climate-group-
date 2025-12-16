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

