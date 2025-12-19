import numpy as np
import pprint as pp

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
        
def eia_state_peak_consumption(cur, stateabbr):
    cur.execute('''SELECT
            States.state_name,
            Years.year,
            EnergyData.energy_value
        FROM EnergyData
        JOIN States ON EnergyData.state_id = States.state_id
        JOIN Years ON EnergyData.year_id = Years.year_id
        WHERE States.state_name = ?
        ''', (stateabbr,)
    )

    highest_year = ''
    highest_energy = 0
    for row in cur:
        year = row[1]
        energy = row[2]

        if energy > highest_energy:
            highest_energy = energy
            highest_year = year

    return (stateabbr, highest_year, highest_energy)

def open_meteo_city_mean_temp_avg(cur, city):
    cur.execute('''SELECT
            WeatherData.mean_temp
        FROM WeatherData
        JOIN Cities ON WeatherData.city_id = Cities.city_id
        WHERE Cities.city_name = ?
        ''', (city,)
    )

    temps = []
    for row in cur:
        temps.append(row[0])

    return round(np.average(np.array(temps)), 2)

def functionoutput(pollutant, moving, statenrg, mean_temp_avg):

    with open("function_output.txt", "w") as ofile:
            ofile.write("Most harmful pollutant in: \n\n")
            pp.pprint(pollutant, stream=ofile,)

            ofile.write("\n\nAMD Stock 100 day moving average\n\n")
            pp.pprint(moving, stream=ofile,)

            ofile.write("\n\nHighest energy consumption for the past ten years in: \n\n")
            pp.pprint(statenrg, stream=ofile,)

            ofile.write("\n\nState mean temperature\n\n")
            pp.pprint(mean_temp_avg, stream=ofile,)

                        
                

