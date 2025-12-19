from functions_and_visualizations import *
import sqlite3

def main():

    conn = sqlite3.connect('climate_data.db')
    cur = conn.cursor()

    harmfuldata = functions.openweather_city_harmful_pollutant(cur, 'Annapolis')
    movingavgdata = functions.amd_marketstack_movingavg(cur)
    statenergy = functions.eia_state_peak_consumption(cur, 'MD')
    meantmpavg = functions.open_meteo_city_mean_temp_avg(cur, 'Annapolis')
    functions.functionoutput(harmfuldata, movingavgdata, statenergy, meantmpavg)

    visualizations.amd_trend_line(cur)
    visualizations.pollution_danger_threshold(cur, "Annapolis")
    visualizations.plot_state_consumption(cur)
    visualizations.plot_city_temperature_averages(cur)
    visualizations.state_energy_trend_line(cur, "MD")

if __name__ == '__main__':
    main()