import requests
import json

def amd_stock_data():

    API_KEY = '1f3b83b514e53113dee475ee803802c5'
    url = 'https://api.marketstack.com/v1/eod'
    params = {
            "access_key": API_KEY,
            "symbols": 'AMD',
            "limit": 100,
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()

        with open('amd_marketstack.json', 'w') as marketfile:
            json.dump(data, marketfile, indent=4)

amd_stock_data()