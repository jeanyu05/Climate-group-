import json
import datetime



def load_json_data(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)
        return data
    
def update_air_quality_date(data):

    for k,v in data.items():
        for AQDict in v['list']:
            if k in ["Lansing", "Columbus", "Indianapolis", "Harrisburg", "Charleston", 
                        "Richmond", "Annapolis", "Dover", "Albany", "Trenton", "Hartford", 
                        "Boston", "Providence", "Montpelier", "Concord", "Augusta", "Frankfort"]:
                AQDict.update({'dt': str(datetime.datetime.fromtimestamp(timed.get('dt'), tzinfo= datetime.timezone(datetime.timedelta(hours=-5)))) })
            elif k in ["Springfield", "Madison", "Saint Paul", "Des Moines", 
                       "Jefferson City", "Nashville", "Pierre", "Bismarck"]:
                AQDict.update({'dt': str(datetime.datetime.fromtimestamp(timed.get('dt'), tzinfo = datetime.timezone(datetime.timedelta(hours=-6)))) })

    return None

def combine_air_quality_data(data):
    new_aq = {}
    dayslst = ['02','03','04','05']

    for date in dayslst:
        for k,v in data.items():
            tempcity = {'aqi':[], 
                        'components': 
                            {'co': [],
                             'no':[],
                             'no2':[],
                             'o3': [],
                             'so2': [],
                             'pm2_5': [],
                             'pm10': [],
                             'nh3': []
                            }}
            for AQDict in v['list']:
                if date == AQDict.get('dt')[9:11]:
                    tempcity['aqi'].append(AQDict['aqi'])
                    tempcity['components']['co'].append(AQDict['components']['co'])
                    tempcity['components']['no'].append(AQDict['components']['no'])
                    tempcity['components']['no2'].append(AQDict['components']['no2'])
                    tempcity['components']['o3'].append(AQDict['components']['o3'])
                    tempcity['components']['so2'].append(AQDict['components']['so2'])
                    tempcity['components']['pm2_5'].append(AQDict['components']['pm2_5'])
                    tempcity['components']['pm10'].append(AQDict['components']['pm10'])
                    tempcity['components']['nh3'].append(AQDict['components']['nh3'])
                    
            new_aq[f"{k} 2025-12-{date}"] = tempcity

    return new_aq

    


