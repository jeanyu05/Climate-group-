import requests
import json

Stateid_list = ['MI','OH','IN','IL','WI','MN','IA','MO','KY','TN',
                'PA','WV','VA','MD','DE','NY','NJ','CT','MA','RI',
                'VT','NH','ME','SD','ND']

def eia_city_data(stateidlst, Key):

    all_states = {}

    for id in stateidlst:
    
        url = 'https://api.eia.gov/v2/seds/data/'
        
        payload = {
            "frequency": "annual",
            "data": ["value"],
            "facets": {
                "stateId": [id],
                "seriesId": ["TETCB"]
            },
            "start": "2013",
            "end": "2023",
            "sort": [
                {
                    "column": "period",
                    "direction": "desc"
                }
            ],
            "offset": 0,
            "length": 5000
        }
        
        params = {
            "api_key": Key
        }
        
        response = requests.post(url, params=params, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            all_states[id] = data['response']['data']

    with open('EIA_data.json', 'w') as EIA_file:
        json.dump(all_states, EIA_file, indent=4)

