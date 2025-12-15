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

def setup_open_meteo_data(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS WeatherData (
            weather_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            date TEXT,
            temperature_2m_max INTEGER,
            co REAL,
            no REAL,
            no2 REAL,
            o3 REAL,
            so2 REAL,
            pm2_5 REAL,
            pm10 REAL,
            nh3 REAL,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id),
            UNIQUE(city_id, date)
            )
    ''')

    count = 0

    for k, v in data.items():

        if count >= 25:
            break

        city_name = k[:-3].strip()
        city_date = v['date'][0][:10]


        cur.execute('SELECT city_id FROM Cities where city_name =?', (city_name,))
        city_name_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO AirQuality
                    (city_id, date, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                    city_name_id,
                    city_date,
                    np.average(np.array(v['aqi'])),
                    np.average(np.array(v['co'])),
                    np.average(np.array(v['no'])),
                    np.average(np.array(v['no2'])),
                    np.average(np.array(v['o3'])),
                    np.average(np.array(v['so2'])),
                    np.average(np.array(v['pm2_5'])),
                    np.average(np.array(v['pm10'])),
                    np.average(np.array(v['nh3'])),
                    )
               )
        
        if cur.rowcount == 1:
            count += 1

def main():
    separate_weather_data('Weather_Data.json')
    cur, conn = setup_climate_database('climate_data.db')

        

            



