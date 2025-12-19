from collectors import *
import sqlite3
import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    eiakey = os.getenv('EIAAPI')
    marketkey = os.getenv('MARKETAPI')
    weatherkey = os.getenv('WEATHERAPI')

    EIA.eia_city_data(EIA.Stateid_list, eiakey)
    openmeteo.openmeteo_city_data(openmeteo.OW_LatLong_Dict)
    marketstack.amd_stock_data(marketkey)
    OpenWeather.openweather_city_data(OpenWeather.OW_LatLong_Dict, weatherkey)

if __name__ == '__main__':
    main()