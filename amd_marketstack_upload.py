import json
import sqlite3

def extract_amd_marketstack(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)

    return data

def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()

    return cur, conn

def setup_amd_market_data(cur, conn, data):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS amdstock (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT, 
        adj_open REAL, 
        adj_high REAL,
        adj_low REAL,
        adj_close REAL,
        adj_volume REAL, 
        date TEXT
        )
                
''')
    
    count = 0
    datalst = data['data']

    for d in datalst:

        if count >= 25:
            break

        cur.execute('''INSERT OR IGNORE INTO amdstock
                    (adj_open, adj_high, adj_low, adj_close, adj_volume, date)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (d['adj_open'],
                     d['adj_high'],
                     d['adj_low'],
                     d['adj_close'],
                     d['adj_volume'],
                     d['date'][:10],)     
                )
        
        if cur.rowcount == 1:
            count += 1

    conn.commit()

def main():
    data = extract_amd_marketstack('amd_marketstack.json')
    cur, conn = setup_climate_database("climate_data.db")
    setup_amd_market_data(cur, conn, data)

main()

    


