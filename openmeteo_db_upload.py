import json
import sqlite3

def separate_weather_data(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)

    new_weath = {}
    
    for k,v in data.items():

        for i in range(4):
            temp_dict = {}
        
            for param_name, stat in v['daily'].items():
                if i < len (stat):
                    temp_dict[param_name] = stat[i]

            new_weath[f'{k} {i}'] = temp_dict
    
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

        city_name = k.rsplit(' ', 1)[0]
        cur.execute(
            "INSERT OR IGNORE INTO Cities (city_name) VALUES (?)", (city_name,)
        )

    conn.commit()

def setup_date_tables(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Dates (
                date_id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE NOT NULL
        )
    ''')

    for v in data.values():
        cur.execute(
            "INSERT OR IGNORE INTO Dates (date) VALUES (?)", (v['time'],)
        )

    conn.commit()


    

def setup_open_meteo_data(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS WeatherData (
            weather_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            date_id INTERGER NOT NULL,
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
            UNIQUE(city_id, date_id)
            )
    ''')

    count = 0
    datecount = 0
     
    for k, v in data.items():

        if count >= 25:
            break

        city_name = k.rsplit(' ', 1)[0]

        cur.execute('SELECT city_id FROM Cities where city_name =?', (city_name,))
        city_name_id = cur.fetchone()[0]

        cur.execute('SELECT date_id FROM Dates where date =?', (v['time'],))
        date_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO WeatherData
                    (
                    city_id,
                    date_id,
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
                    date_id,
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
    setup_date_tables(cur, conn, data)
    setup_open_meteo_data(cur, conn, data)

main()

        

            



