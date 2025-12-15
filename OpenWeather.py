import sqlite3
import json
import requests

OW_LatLong_Dict = {
    'Lansing': (42.7325, -84.5555), 'Columbus': (39.9612, -82.9988),
    'Indianapolis': (39.7684, -86.1581), 'Springfield': (39.7817, -89.6501),
    'Madison': (43.0748, -89.3840), 'Saint Paul': (44.9537, -93.0900),
    'Des Moines': (41.5868, -93.6250), 'Jefferson City': (38.5767, -92.1735),
    'Frankfort': (38.2009, -84.8733), 'Nashville': (36.1627, -86.7816),
    'Harrisburg': (40.2732, -76.8867), 'Charleston': (38.3498, -81.6326),
    'Richmond': (37.5407, -77.4360), 'Annapolis': (38.9784, -76.4922),
    'Dover': (39.1582, -75.5244), 'Albany': (42.6526, -73.7562),
    'Trenton': (40.2204, -74.7643), 'Hartford': (41.7658, -72.6734),
    'Boston': (42.3601, -71.0589), 'Providence': (41.8240, -71.4128),
    'Montpelier': (44.2601, -72.5754), 'Concord': (43.2081, -71.5376),
    'Augusta': (44.3106, -69.7795), 'Pierre': (44.3683, -100.3509),
    'Bismarck': (46.8083, -100.7837)
}

def create_tables():
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS AirQuality (
            air_quality_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            dt INTEGER NOT NULL,
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
            UNIQUE(city_id, dt)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("✓ Tables created successfully")

def get_city_id(city_name, lat, lon):
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    
    try:
        cur.execute('''
            INSERT OR IGNORE INTO Cities (city_name, latitude, longitude)
            VALUES (?, ?, ?)
        ''', (city_name, lat, lon))
        conn.commit()
        
        cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city_name,))
        city_id = cur.fetchone()[0]
        conn.close()
        return city_id
    except Exception as e:
        conn.close()
        print(f"Error with city {city_name}: {e}")
        return None

def get_next_batch_cities(batch_size=25):
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    
    cur.execute('''
        SELECT city_name FROM Cities
        WHERE city_id NOT IN (
            SELECT DISTINCT city_id FROM AirQuality
        )
        LIMIT ?
    ''', (batch_size,))
    
    cities_to_process = [row[0] for row in cur.fetchall()]
    conn.close()
    
    if len(cities_to_process) < batch_size:
        conn = sqlite3.connect('climate_data.db')
        cur = conn.cursor()
        cur.execute('SELECT city_name FROM Cities')
        existing = {row[0] for row in cur.fetchall()}
        conn.close()
        
        available = [city for city in OW_LatLong_Dict.keys() if city not in existing]
        remaining = batch_size - len(cities_to_process)
        cities_to_process.extend(available[:remaining])
    
    return cities_to_process[:batch_size]

def openweather_city_data(latlongtup):
    import numpy as np
    from datetime import datetime
    
    all_cities = {}
    owAPIKey = 'bac9b356ed172f8814a3fcea57ab3e85'

    for k, v in latlongtup.items():
        
        url = f"http://api.openweathermap.org/data/2.5/air_pollution/history"
        params = {'lat': v[0],
                  'lon': v[1],
                  'start': 1764633600,
                  'end': 1764979200,
                  'appid': owAPIKey}
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            
            daily_averages = {}
            
            for reading in data.get('list', []):
                dt = reading.get('dt')
                date = datetime.fromtimestamp(dt).strftime('%Y-%m-%d')
                
                if date not in daily_averages:
                    daily_averages[date] = {
                        'aqi': [], 'co': [], 'no': [], 'no2': [],
                        'o3': [], 'so2': [], 'pm2_5': [], 'pm10': [], 'nh3': []
                    }
                
                daily_averages[date]['aqi'].append(reading.get('main', {}).get('aqi'))
                components = reading.get('components', {})
                daily_averages[date]['co'].append(components.get('co'))
                daily_averages[date]['no'].append(components.get('no'))
                daily_averages[date]['no2'].append(components.get('no2'))
                daily_averages[date]['o3'].append(components.get('o3'))
                daily_averages[date]['so2'].append(components.get('so2'))
                daily_averages[date]['pm2_5'].append(components.get('pm2_5'))
                daily_averages[date]['pm10'].append(components.get('pm10'))
                daily_averages[date]['nh3'].append(components.get('nh3'))
            
            city_averages = {}
            for date, values in daily_averages.items():
                city_averages[date] = {
                    'aqi': round(np.mean([x for x in values['aqi'] if x is not None]), 2),
                    'co': round(np.mean([x for x in values['co'] if x is not None]), 2),
                    'no': round(np.mean([x for x in values['no'] if x is not None]), 2),
                    'no2': round(np.mean([x for x in values['no2'] if x is not None]), 2),
                    'o3': round(np.mean([x for x in values['o3'] if x is not None]), 2),
                    'so2': round(np.mean([x for x in values['so2'] if x is not None]), 2),
                    'pm2_5': round(np.mean([x for x in values['pm2_5'] if x is not None]), 2),
                    'pm10': round(np.mean([x for x in values['pm10'] if x is not None]), 2),
                    'nh3': round(np.mean([x for x in values['nh3'] if x is not None]), 2)
                }
            
            all_cities[k] = city_averages

    with open('Air_quality.json', 'w') as airpolfile:
        json.dump(all_cities, airpolfile, indent=4)
    
    return all_cities

def store_air_quality_to_db(cities_to_process):
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    
    items_stored = 0
    
    cities_dict = {city: OW_LatLong_Dict[city] for city in cities_to_process if city in OW_LatLong_Dict}
    
    all_cities = openweather_city_data(cities_dict)
    
    for city_name, city_data in all_cities.items():
        if city_name not in OW_LatLong_Dict:
            continue
            
        lat, lon = OW_LatLong_Dict[city_name]
        city_id = get_city_id(city_name, lat, lon)
        
        if not city_id:
            continue
        
        if 'list' in city_data:
            for reading in city_data['list']:
                dt = reading.get('dt')
                aqi = reading.get('main', {}).get('aqi')
                components = reading.get('components', {})
                
                try:
                    cur.execute('''
                        INSERT OR IGNORE INTO AirQuality 
                        (city_id, dt, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        city_id, dt, aqi,
                        components.get('co'),
                        components.get('no'),
                        components.get('no2'),
                        components.get('o3'),
                        components.get('so2'),
                        components.get('pm2_5'),
                        components.get('pm10'),
                        components.get('nh3')
                    ))
                    items_stored += 1
                except Exception as e:
                    print(f"Error storing reading for {city_name}: {e}")
            
            print(f"✓ Stored air quality data for {city_name}")
    
    conn.commit()
    conn.close()
    return items_stored

def main():
    print("=" * 60)
    print("OpenWeather Air Quality Data Collection")
    print("=" * 60)
    
    create_tables()
    
    cities = get_next_batch_cities(batch_size=25)
    
    if not cities:
        print("\n✓ All cities have been processed!")
        print("Run this script again to add more data if needed.")
        return
    
    print(f"\nProcessing {len(cities)} cities in this batch:")
    for city in cities:
        print(f"  - {city}")
    
    print(f"\nFetching air quality data from OpenWeather API...")
    items_stored = store_air_quality_to_db(cities)
    
    print(f"\n" + "=" * 60)
    print(f"✓ Successfully stored {items_stored} air quality readings")
    print("=" * 60)
    
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM Cities')
    city_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM AirQuality')
    aq_count = cur.fetchone()[0]
    conn.close()
    
    print(f"\nCurrent Database Stats:")
    print(f"  Cities: {city_count}")
    print(f"  Air Quality Readings: {aq_count}")

if __name__ == "__main__":
    main()
    
    openweather_city_data(OW_LatLong_Dict)