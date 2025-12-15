import json
import sqlite3

def extract_eia_data(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)

    return data

def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    return cur, conn

def setup_state_table(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS States(
            state_id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_name TEXT UNIQUE NOT NULL
        )
''')
    
    for k in data.keys():
        cur.execute(
            "INSERT OR IGNORE INTO States (state_name) VALUES (?)", (k,)
        )

    conn.commit()

def setup_year_table(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Years(
            year_id INTEGER PRIMARY KEY AUTOINCREMENT,
            year TEXT UNIQUE NOT NULL
        )
''')
    
    for statelst in data.values():
        for energyinfo in statelst:
            cur.execute(
                "INSERT OR IGNORE INTO Years (year) VALUES (?)", (energyinfo["period"],)
            )

    conn.commit()

def setup_state_energy_data(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS EnergyData (
        state_energy_id INTEGER PRIMARY KEY AUTOINCREMENT,
        state_id INTEGER NOT NULL,
        year_id INTEGER NOT NULL,
        energy_value INTEGER  
        )

''')
    
    count = 0

    for k, v in data.items():
        for energyinfo in v:

            if count >= 25:
                break

            cur.execute('SELECT state_id FROM States WHERE state_name =?', (k,))
            state_id = cur.fetchone()[0]

            cur.execute('SELECT year_id FROM Years WHERE year =?', (energyinfo['period'],))
            year_id = cur.fetchone()[0]

            cur.execute('''INSERT OR IGNORE INTO EnergyData
                        (state_id, year_id, energy_value)
                        VALUES (?, ?, ?)''',
                        (
                        state_id,
                        year_id,
                        energyinfo['value'],    
                        )                       
                    )
            
            if cur.rowcount == 1:
                count += 1

        conn.commit()


def main():
    data = extract_eia_data("EIA_data.json")
    cur, conn = setup_climate_database("climate_data.db")
    setup_state_table(cur, conn, data)
    setup_year_table(cur, conn, data)
    setup_state_energy_data(cur, conn, data)

main()
