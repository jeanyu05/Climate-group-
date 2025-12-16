import sqlite3
import numpy as np


def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    return cur, conn


def openweather_city_harmful_pollutant(cur, city):
    cur.execute('''SELECT 
                AirQualityData.co,
                AirQualityData.no,
                AirQualityData.no2,
                AirQualityData.o3,
                AirQualityData.so2,
                AirQualityData.pm2_5,
                AirQualityData.pm10,
                AirQualityData.nh3,
                Cities.city_name AS City,
                Dates.date as Date
            FROM AirQualityData
            JOIN Cities ON AirQualityData.city_id = Cities.city_id
            JOIN Dates on AirQualityData.date_id = Dates.date_id   
            WHERE City = ?''', (city,)
    )
    
    cityd = {}
    for row in cur:
        cityd[f"{row[8]} {row[9]}"] = {'co': row[0]/10000, 
                'no': row[1]/200, 
                'no2': row[2]/200,
                'o3': row[3]/100,
                'so2': row [4]/350,
                'pm2_5': row[5]/25,
                'pm10': row[6]/50,
                'nh3': row[7]/200}

    polmainlst = []  
    for k, v in cityd.items():
        main_amount = 0
        pollutant_name = ''
        for pollutant, amount in v.items():
            if amount > main_amount:
                pollutant_name = pollutant
                main_amount = amount

        polmainlst.append((k, pollutant_name, main_amount))

    return(polmainlst)
        

def amd_marketstack_movingavg(cur):
    cur.execute('''SELECT
        adj_close
        FROM amdstock 
        ''')
    
    moving_avg =[]
    for row in cur:
        moving_avg.append(row[0])

    return round(np.average(np.array(moving_avg)), 2)
        


def main():
    cur, conn = setup_climate_database('climate_data.db')
    openweather_city_harmful_pollutant(cur, 'Annapolis')
    print(amd_marketstack_movingavg(cur))


main()

                        
                

