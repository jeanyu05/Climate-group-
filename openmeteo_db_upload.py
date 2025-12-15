import json
import sqlite3

def separate_weather_data(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)

    new_weath = {}

    for k,v in data.items():
        temp_dict = {}
        count = 0
        while count < 4:
            for param_name, stat in v['daily'].items():
                temp_dict[param_name] = stat[count]
            new_weath[k] = temp_dict 
            count += 1
    
    return new_weath

def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    return cur, conn

def setup_city_table(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE NOT NULL
        )
    ''')

    for k in data.keys():
        cur.execute(
            "INSERT OR IGNORE INTO Cities (city_name) VALUES (?)", (k,)
        )

    conn.commit()

def setup_date_tables():
    pass

def setup_open_meteo_data(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS WeatherData (
            weather_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            date TEXT,
            mean_temp REAL,
            max_temp REAL,
            min_temp REAL,
            mean_feels_like_temp REAL,
            max_feels_like_temp REAL,
            min_feels_like_temp REAL,
            snowfall_sum REAL,
            rain_sum REAL,
            precipitation_sum REAL,
            mean_relavite_humidity REAL,
            max_relative_humidity REAL,
            min_relative_humidity REAL,
            mean_wind_speed REAL,
            max_wind_speed REAL,
            min_wind_speed REAL,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id),
            UNIQUE(city_id, date)
            )
    ''')

    count = 0

    for k, v in data.items():

        if count >= 25:
            break

        cur.execute('SELECT city_id FROM Cities where city_name =?', (k,))
        city_name_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO WeatherData
                    (
                    city_id,
                    date,
                    mean_temp,
                    max_temp,
                    min_temp,
                    mean_feels_like_temp,
                    max_feels_like_temp,
                    min_feels_like_temp,
                    snowfall_sum,
                    rain_sum,
                    precipitation_sum,
                    mean_relavite_humidity,
                    max_relative_humidity,
                    min_relative_humidity,
                    mean_wind_speed,
                    max_wind_speed,
                    min_wind_speed
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                    city_name_id,
                    v['time'],
                    v['temperature_2m_mean'],
                    v['temperature_2m_max'],
                    v['temperature_2m_min'],
                    v['apparent_temperature_mean'],
                    v['apparent_temperature_max'],
                    v['apparent_temperature_min'],
                    v['snowfall_sum'],
                    v['rain_sum'],
                    v['precipitation_sum'],
                    v['relative_humidity_2m_mean'],
                    v['relative_humidity_2m_max'],
                    v['relative_humidity_2m_min'],
                    v['wind_speed_10m_mean'],
                    v['wind_speed_10m_max'],
                    v['wind_speed_10m_min'],
                    )
               )
        
        if cur.rowcount == 1:
            count += 1

    conn.commit()

def main():
    data = separate_weather_data('Weather_Data.json')
    cur, conn = setup_climate_database('climate_data.db')
    setup_city_table(cur, conn, data)
    setup_open_meteo_data(cur, conn, data)

main()

        

            



