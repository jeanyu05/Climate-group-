import sqlite3
import numpy as np
import matplotlib.pyplot as plt

def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    return cur, conn

def lstavg(lst):
    return (sum(lst)/len(lst))

def amd_trend_line(cur):

    cur.execute('''SELECT
                adj_close,
                date
                FROM amdstock
                ORDER BY date ASC
                ''')
    dates = []
    close = []
    for row in cur:
        dates.append(row[1])
        close.append(row[0])

    fig, ax = plt.subplots()
    ax.plot(dates, close)
    ax.set_xlabel('Dates')
    ax.set_ylabel('Closing Prices')
    ax.set_title('AMD Stock Trendline 12/12/2025 - 07/24/2025')
    

    plt.show()

def pollution_danger_threshold(cur, city):
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

    
    cityname = ''
    cityd={
            'co': [], 
            'no': [], 
            'no2': [],
            'o3': [],
            'so2': [],
            'pm2_5': [],
            'pm10': [],
            'nh3': []
            }
    for row in cur:
            cityname = row[8]
            cityd['co'].append(row[0]/10000) 
            cityd['no'].append(row[1]/200) 
            cityd['no2'].append(row[2]/200)
            cityd['o3'].append(row[3]/100)
            cityd['so2'].append(row [4]/350)
            cityd['pm2_5'].append(row[5]/25)
            cityd['pm10'].append(row[6]/50)
            cityd['nh3'].append(row[7]/200)

    pollutants = (
         'co',
         'no',
         'no2',
         'o3',
         'so2',
         'pm2_5',
         'pm10',
         'nh3'
    )

    weights = {
        'Current_level': np.array([np.average(cityd['co']), np.average(cityd['no']), np.average(cityd['no2']), np.average(cityd['o3']), 
                                   np.average(cityd['so2']), np.average(cityd['pm2_5']), np.average(cityd['pm10']), np.average(cityd['nh3'])]),
        'Threat_level': np.array([1 - np.average(cityd['co']), 1- np.average(cityd['no']),1 - np.average(cityd['no2']),1 - np.average(cityd['o3']), 
                                   1 - np.average(cityd['so2']), 1- np.average(cityd['pm2_5']),1 - np.average(cityd['pm10']),1-  np.average(cityd['nh3'])])
    }

    width = 0.5

    fig, ax = plt.subplots()
    bottom = np.zeros(len(pollutants))

    for c_level, t_level in weights.items():
         p = ax.bar(pollutants, t_level, width, label=c_level, bottom=bottom)
         bottom += t_level

    ax.set_title(f'Pollutants in {cityname} approaching enviorment threat levels')
    ax.legend(loc='upper right')

    plt.show()

def plot_state_consumption(cur):
    cur.execute('''
        SELECT
            States.state_name,
            EnergyData.energy_value
        FROM EnergyData
        JOIN States ON EnergyData.state_id = States.state_id
    ''')

    stated = {}

    for row in cur:
        state = row[0]
        value = float(row[1])

        if state not in stated:
            stated[state] = []

        stated[state].append(value)

    states = []
    avg_energy = []

    for state, values in stated.items():
        states.append(state)
        avg_energy.append(lstavg(values))

    fig, ax = plt.subplots()
    ax.bar(states, avg_energy)
    ax.set_xlabel('State')
    ax.set_ylabel('Average Energy Consumption in Billion Btu')
    ax.set_title('Average Energy Consumption per State')
    ax.tick_params(axis='x', rotation=90)

    plt.tight_layout()
    plt.show()

def plot_city_temperature_averages(cur):
    cur.execute('''
        SELECT
            Cities.city_name,
            WeatherData.mean_temp,
            WeatherData.max_temp,
            WeatherData.min_temp
        FROM WeatherData
        JOIN Cities ON WeatherData.city_id = Cities.city_id
    ''')

    cityd = {}

    for row in cur:
        city = row[0]

        if city not in cityd:
            cityd[city] = {
                'mean': [],
                'max': [],
                'min': []
            }

        cityd[city]['mean'].append(row[1])
        cityd[city]['max'].append(row[2])
        cityd[city]['min'].append(row[3])

    cities = []
    mean_avgs = []
    max_avgs = []
    min_avgs = []

    for city, temps in cityd.items():
        cities.append(city)
        mean_avgs.append(np.average(temps['mean']))
        max_avgs.append(np.average(temps['max']))
        min_avgs.append(np.average(temps['min']))

    x = np.arange(len(cities))
    width = 0.25

    fig, ax = plt.subplots()
    ax.bar(x - width, mean_avgs, width, label='Mean Temp')
    ax.bar(x, max_avgs, width, label='Max Temp')
    ax.bar(x + width, min_avgs, width, label='Min Temp')

    ax.set_xlabel('City')
    ax.set_ylabel('Farenheit')
    ax.set_title('Average Daily Temperatures by City')
    ax.set_xticks(x)
    ax.set_xticklabels(cities, rotation=45)
    ax.legend()

    plt.tight_layout()
    plt.show()
     

def main():
    cur, conn = setup_climate_database('climate_data.db')
    amd_trend_line(cur)
    pollution_danger_threshold(cur, "Annapolis")
    plot_state_consumption(cur)
    plot_city_temperature_averages(cur)


main()

