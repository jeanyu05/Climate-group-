import json
import datetime
import sqlite3
import numpy as np
    
def update_air_quality_date(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)

    for k,v in data.items():
        for AQDict in v['list']:
            if k in ["Lansing", "Columbus", "Indianapolis", "Harrisburg", "Charleston", 
                        "Richmond", "Annapolis", "Dover", "Albany", "Trenton", "Hartford", 
                        "Boston", "Providence", "Montpelier", "Concord", "Augusta", "Frankfort"]:
                AQDict.update({'dt': str(datetime.datetime.fromtimestamp(AQDict.get('dt'), tz=datetime.timezone(datetime.timedelta(hours=-5)))) })
            elif k in ["Springfield", "Madison", "Saint Paul", "Des Moines", 
                       "Jefferson City", "Nashville", "Pierre", "Bismarck"]:
                AQDict.update({'dt': str(datetime.datetime.fromtimestamp(AQDict.get('dt'), tz=datetime.timezone(datetime.timedelta(hours=-6)))) })

    return data

def combine_air_quality_data(data):
    new_aq = {}
    dayslst = ['02','03','04','05']

    for date in dayslst:
        for k,v in data.items():
            tempcity = {'aqi':[], 
                        'date':[], 
                        'co': [],
                        'no':[],
                        'no2':[],
                        'o3': [],
                        'so2': [],
                        'pm2_5': [],
                        'pm10': [],
                        'nh3': []
                    }
            for AQDict in v['list']:
                if date == AQDict.get('dt')[8:10]:
                    tempcity['aqi'].append(AQDict['main']['aqi'])
                    tempcity['date'].append(AQDict['dt'])
                    tempcity['co'].append(AQDict['components']['co'])
                    tempcity['no'].append(AQDict['components']['no'])
                    tempcity['no2'].append(AQDict['components']['no2'])
                    tempcity['o3'].append(AQDict['components']['o3'])
                    tempcity['so2'].append(AQDict['components']['so2'])
                    tempcity['pm2_5'].append(AQDict['components']['pm2_5'])
                    tempcity['pm10'].append(AQDict['components']['pm10'])
                    tempcity['nh3'].append(AQDict['components']['nh3'])
                    
            new_aq[f"{k} {date}"] = tempcity

    return new_aq

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
            "INSERT OR IGNORE INTO Dates (date) VALUES (?)", (v['date'][0][:10],)
        )

    conn.commit()

def setup_air_quality_data(cur, conn, data):
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

    count = 0

    for k, v in data.items():

        if count >= 25:
            break

        city_name = k[:-3].strip()

        cur.execute('SELECT city_id FROM Cities where city_name =?', (city_name,))
        city_name_id = cur.fetchone()[0]

        cur.execute('SELECT date_id FROM Dates where date =?', (v['date'][0][:10],))
        date_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO AirQuality
                    (city_id, date_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                    city_name_id,
                    date_id,
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

    conn.commit()

def main():
    data = update_air_quality_date("Air_quality.json")
    agg_aq_data = combine_air_quality_data(data)
    cur, conn = setup_climate_database('climate_data.db')
    setup_city_table(cur, conn, agg_aq_data)
    setup_date_tables(cur,conn, agg_aq_data)
    setup_air_quality_data(cur, conn, agg_aq_data)

main()