from collectors import *
from database_uploaders import *
from functions_and_visualizations import *
import sqlite3

def main():
    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()
    
    eiadata = eia_db_upload.extract_eia_data("EIA_data.json")
    eia_db_upload.setup_state_table(cur, conn, eiadata)
    eia_db_upload.setup_year_table(cur, conn, eiadata)
    eia_db_upload.setup_state_energy_data(cur, conn, eiadata)

    meteodata = openmeteo_db_upload.separate_weather_data('Weather_Data.json')
    openmeteo_db_upload.setup_city_table(cur, conn, meteodata)
    openmeteo_db_upload.setup_date_tables(cur, conn, meteodata)
    openmeteo_db_upload.setup_open_meteo_data(cur, conn, meteodata)

    weatherdata = openweather_db_upload.update_air_quality_date("Air_quality.json")
    agg_aq_data = openweather_db_upload.combine_air_quality_data(weatherdata)
    openweather_db_upload.setup_city_table(cur, conn, agg_aq_data)
    openweather_db_upload.setup_date_tables(cur,conn, agg_aq_data)
    openweather_db_upload.setup_air_quality_data(cur, conn, agg_aq_data)

    amddata = amd_marketstack_upload.extract_amd_marketstack('amd_marketstack.json')
    amd_marketstack_upload.setup_amd_market_data(cur, conn, amddata)

if __name__ == '__main__':
    main()