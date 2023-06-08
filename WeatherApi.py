import requests
from urllib.parse import urlencode
import json
from requests.exceptions import RequestException
from datetime import date
import io
class WeatherDataAPI:
    def __init__(self,start_date,end_date):
        self.endpoint = "https://archive-api.open-meteo.com/v1/archive?"
        self.start_date=start_date
        self.end_date=end_date
        self.timezone = 'UTC'


    def get_weather_data(self, latitude, longitude, location_name=None):
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "timezone": self.timezone,
            "daily": "weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,precipitation_sum,rain_sum,snowfall_sum,windspeed_10m_max,windgusts_10m_max",

        }
        query_string = urlencode(params, safe=',')
        try:
            response = requests.get(self.endpoint+query_string)
            data=response.json()
            if response.status_code == 200:
                if location_name is not None:
                    data['location_name'] = location_name
                return json.dumps(data)
        except RequestException as e:
            raise ValueError(f"{response.status_code} Code: {response.text}  - Error fetching weather data from API")

    def get_locations(self, city_names):
        locations = {}
        for city in city_names:
            params = {
                "name": city,
                "format": "json"
            }
            query_string = urlencode(params, safe=',')
            response = requests.get("https://geocoding-api.open-meteo.com/v1/search?" + query_string)
            if response.status_code == 200:
                data = response.json()
                if len(data['results']) > 0:
                    city_data = max(data['results'], key=lambda x: x.get('population',0))
                    locations[(city_data['latitude'], city_data['longitude'])] = city_data['name']
        return locations

    def get_all_weather_data(self, city_names,save_each):
        results = []
        locations = self.get_locations(city_names)
        for location in locations:
            lat, lon = location
            city_name = locations[location]
            data = self.get_weather_data(lat, lon, city_name)

            if save_each:
                #data = json.dumps(data)
                current_date = date.today().strftime("%Y-%m-%d")
                file_name = f"data/data_{city_name}_{current_date}.json"
                #data = json.dumps(data)
                with io.open(file_name, "w",newline='\r\n') as file:
                    json.dump(data, file)
            results.append(data)
        return results

    def save_data(self,data):
        current_date = date.today().strftime("%Y-%m-%d")
        file_name = f"data/data_{current_date}.json"

        with open(file_name, "w") as file:
            json.dump(data, file)

