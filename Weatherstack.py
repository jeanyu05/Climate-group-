import requests

WS_City_Dict = {'Michigan':'Lansing','Ohio':'Columbus','Indiana':'Indianapolis','Illinois':'Springfield','Wisconsin':'Madison',
             'Minnesota':'Saint Paul','Iowa':'Des Moines','Missouri':'Jefferson City','Kentucky':'Frankfort','Tennessee':'Nashville',
             'Pennsylvania':'Harrisburg','West Virginia':'Charleston','Virginia':'Richmond','Maryland':'Annapolis','Delaware':'Dover',
             'New York':'Albany','New Jersey':'Trenton','Connecticut':'Hartford','Massachusetts':'Boston','Rhode Island':'Providence',
             'Vermont':'Montpelier','New Hampshire':'Concord','Maine':'Augusta','South Dakota':'Pierre','North Dakota':'Bismarck'}


#start and end for weatherstand and open weather have to line up for correlation function
def weatherstack_city_data(citydict):
    WeatherstackAPIKey = ''

    for k,v in citydict.items():
        url = f"https://api.weatherstack.com/historical"
        params = {'access_key': WeatherstackAPIKey,
                  'query': v,
                  'historical_date': '2025-12-02;2025-12-03;2025-12-04;2025-12-05',
                  'hourly': 1,
                  'interval': 24
                  }
    #you can do the query string like lansing;columbus; ect. or just loop through the dict

