import requests
import json


OW_LatLong_Dict = {
    'Lansing': (42.7325, -84.5555), 'Columbus': (39.9612, -82.9988),
    'Indianapolis': (39.7684, -86.1581), 'Springfield': (39.7817, -89.6501),
    'Madison': (43.0748, -89.3840), 'Saint Paul': (44.9537, -93.0900),
    'Des Moines': (41.5868, -93.6250), 'Jefferson City': (38.5767, -92.1735),
    'Frankfort': (38.2009, -84.8733), 'Nashville': (36.1627, -86.7816),
    'Harrisburg': (40.2732, -76.8867), 'Charleston': (38.3498, -81.6326),
    'Richmond': (37.5407, -77.4360), 'Annapolis': (38.9784, -76.4922),
    'Dover': (39.1582, -75.5244), 'Albany': (42.6526, -73.7562),
    'Trenton': (40.2204, -74.7643), 'Hartford': (41.7658, -72.6734),
    'Boston': (42.3601, -71.0589), 'Providence': (41.8240, -71.4128),
    'Montpelier': (44.2601, -72.5754), 'Concord': (43.2081, -71.5376),
    'Augusta': (44.3106, -69.7795), 'Pierre': (44.3683, -100.3509),
    'Bismarck': (46.8083, -100.7837)
}


def openmeteo_city_data(citydict):

    all_cities = {}

    for k,v in citydict.items():
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
	            "latitude": v[0],
	            "longitude": v[1],
	            "start_date": "2025-12-02",
	            "end_date": "2025-12-05",
	            "daily": ["temperature_2m_max", "temperature_2m_min", "snowfall_sum", 
                       "rain_sum", "precipitation_sum", "wind_speed_10m_max", 
                       "relative_humidity_2m_min", "relative_humidity_2m_mean", "relative_humidity_2m_max", 
                       "apparent_temperature_max", "apparent_temperature_mean", "apparent_temperature_min", 
                       "temperature_2m_mean", "wind_speed_10m_mean", "wind_speed_10m_min"],
	            "timezone": "auto",
	            "temperature_unit": "fahrenheit",
	            "wind_speed_unit": "mph",
	            "precipitation_unit": "inch",
}
    
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_cities[k] =  data

    with open('Weather_Data.json', 'w') as weatherfile:
        json.dump(all_cities, weatherfile, indent=4)