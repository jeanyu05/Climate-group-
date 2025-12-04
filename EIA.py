import requests

Stateid_list = ['MI','OH','IN','IL','WI','MN','IA','MO','KY','TN',
                'PA','WV','VA','MD','DE','NY','NJ','CT','MA','RI',
                'VT','NH','ME','SD','ND']

#im like 90% sure this is the right url for the eia api should return the state and their energy usage from 1960-2023
#dont know if we need all that data though, couldnt find the history params to only get a certain amount
def eia_city_data(stateidlst):
    eia_APIKey = ''

    for id in stateidlst:
    
        url = f"https://api.eia.gov/v2/seds/consumption/data?api_key={'ill add api key soon'}&facets[stateid][]={id}" 
    pass


#all data should be downloaded to a file and then the file should eb accessed by sqlite3 for uploading


