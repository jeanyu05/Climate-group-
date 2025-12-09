import json
import datetime
import pyzt


def load_json_data(filename):
    with open(filename, 'r') as ofile:
        data = json.load(ofile)
        return data
    
def updatedate_Air_quality_data(data):

    for k,v in data.items():
        for timed in v['list']:
            if k in ["Lansing", "Columbus", "Indianapolis", "Harrisburg", "Charleston", 
                        "Richmond", "Annapolis", "Dover", "Albany", "Trenton", "Hartford", 
                        "Boston", "Providence", "Montpelier", "Concord", "Augusta", "Frankfort"]:
                timed.update({'dt': datetime.datetime.fromtimestamp(timed.get('dt'), tzinfo= datetime.timezone(datetime.timedelta(hours=-5))) })
            elif k in ["Springfield", "Madison", "Saint Paul", "Des Moines", 
                       "Jefferson City", "Nashville", "Pierre", "Bismarck"]:
                timed.update({'dt': datetime.datetime.fromtimestamp(timed.get('dt'), tzinfo = datetime.timezone(datetime.timedelta(hours=-6))) })

    return None


