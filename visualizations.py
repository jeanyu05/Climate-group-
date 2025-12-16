import sqlite3
import numpy as np
import matplotlib
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

def main():
    cur, conn = setup_climate_database('climate_data.db')
    #amd_trend_line(cur)
    pollution_danger_threshold(cur, "Lansing")


main()
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt

DB = 'climate_data.db'

def plot_eia_avg_consumption(db_path: str = DB,
                             start_year: int = 2013,
                             end_year: int = 2023,
                             top_n: int = 10,
                             save_path: Optional[str] = None) -> Optional[str]:
    """
    Load EIA data from the SQLite DB, compute average consumption per state
    between start_year and end_year, and plot a bar chart of the top_n states.
    If save_path is provided the plot is saved and the path returned, otherwise the plot is shown.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT state, year, value FROM eia_consumption", conn)
    conn.close()

    if df.empty:
        raise ValueError("No EIA data found in database.")

    mask = (df['year'] >= start_year) & (df['year'] <= end_year)
    sub = df[mask].copy()
    if sub.empty:
        raise ValueError(f"No EIA rows in range {start_year}-{end_year}.")

    agg = sub.groupby('state')['value'].mean().sort_values(ascending=False).head(top_n)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(agg.index, agg.values, color='C2')
    ax.set_ylabel('Average Consumption')
    ax.set_title(f'Average Energy Consumption per State ({start_year}-{end_year})')
    plt.xticks(rotation=45, ha='right')
    for b in bars[:5]:
        b.set_edgecolor('black')
        b.set_linewidth(1.0)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
        plt.close(fig)
        return save_path
    else:
        plt.show()
        return None

if __name__ == '__main__':
    plot_eia_avg_consumption()


def plot_open_meteo_weather(db_path: str = DB, save_path: Optional[str] = None):
    """
    1. Connects to DB.
    2. JOINs Weather and Cities tables (Rubric Requirement!).
    3. Calculates average temperature and humidity per city.
    4. Writes calculation results to 'open_meteo_stats.txt' (Rubric Requirement!).
    5. Visualizes the data.
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        Cities.city_name, 
        AVG(Weather.temp_mean) as avg_temp,
        AVG(Weather.rh_mean) as avg_humidity
    FROM Weather
    JOIN Cities ON Weather.city_id = Cities.city_id
    GROUP BY Cities.city_name
    ORDER BY avg_temp DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    