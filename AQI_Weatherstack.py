import requests

WS_City_Dict = {'Michigan':'Lansing','Ohio':'Columbus','Indiana':'Indianapolis','Illinois':'Springfield','Wisconsin':'Madison',
             'Minnesota':'Saint Paul','Iowa':'Des Moines','Missouri':'Jefferson City','Kentucky':'Frankfort','Tennessee':'Nashville',
             'Pennsylvania':'Harrisburg','West Virginia':'Charleston','Virginia':'Richmond','Maryland':'Annapolis','Delaware':'Dover',
             'New York':'Albany','New Jersey':'Trenton','Connecticut':'Hartford','Massachusetts':'Boston','Rhode Island':'Providence',
             'Vermont':'Montpelier','New Hampshire':'Concord','Maine':'Augusta','South Dakota':'Pierre','North Dakota':'Bismarck'}


OW_LatLong_Dict = {'Lansing':(42.7325,-84.5555),'Columbus':(39.9612,-82.9988),'Indianapolis':(39.7684,-86.1581),'Springfield':(39.7817,-89.6501),'Madison':(43.0748,-89.3840),
                  'Saint Paul':(44.9537,-93.0900),'Des Moines':(41.5868,-93.6250),'Jefferson City':(38.5767,-92.1735),'Frankfort':(38.2009,-84.8733),'Nashville':(36.1627,-86.7816),
                  'Harrisburg':(40.2732,-76.8867),'Charleston':(38.3498,-81.6326),'Richmond':(37.5407,-77.4360),'Annapolis':(38.9784,-76.4922),'Dover':(39.1582,-75.5244),
                  'Albany':(42.6526,-73.7562),'Trenton':(40.2204,-74.7643),'Hartford':(41.7658,-72.6734),'Boston':(42.3601,-71.0589),'Providence':(41.8240,-71.4128),
                  'Montpelier':(44.2601,-72.5754),'Concord':(43.2081,-71.5376),'Augusta':(44.3106,-69.7795),'Pierre':(44.3683,-100.3509),'Bismarck':(46.8083,-100.7837)}

#start and end for weatherstand and open weather have to line up for correlation function
def weatherstack_city_data(citydict):
    WeatherstackAPIKey = ''

    for k,v in citydict.items():
        url = f"https://api.weatherstack.com/current?access_key={'api key soon'}&query={v}&historical_date={"4 dates not decided yet"}"
    #you can do the query string like lansing;columbus; ect. or just loop through the dict

def openweather_city_data(latlongdict):
    aqicnAPIKey = ''

    for k, v in latlongdict.items():
        
        url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={v[0]}&lon={v[1]}&start={'dont know yet'}&end={'dont know yet'}&appid={'addig soon'}"


        #all data should be downloaded to a file and then the file should eb accessed by sqlite3 for uploading
