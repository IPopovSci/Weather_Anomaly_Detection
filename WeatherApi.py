import requests
from urllib.parse import urlencode
import json
from requests.exceptions import RequestException
from datetime import date

'''This class provides functionality to retrieve weather data from the Open-Meteo API. \
It allows fetching weather data for specific locations and time periods, \
as well as retrieving data for multiple locations at once. \
The class also includes methods for saving the fetched data.'''


class WeatherDataAPI:
    def __init__(self, start_date, end_date):
        """
        Initializes an instance of the WeatherDataAPI class.

        Parameters:
        - start_date (str): Start date of the data retrieval period in the format 'YYYY-MM-DD'.
        - end_date (str): End date of the data retrieval period in the format 'YYYY-MM-DD'.
            """
        self.endpoint = "https://archive-api.open-meteo.com/v1/archive?"
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = 'UTC'

    def get_weather_data(self, latitude, longitude, location_name=None):
        """
           Retrieves weather data for a specific latitude and longitude and saves it to a file.

           Parameters:
           - latitude (float): Latitude coordinate of the location.
           - longitude (float): Longitude coordinate of the location.

           Returns:
           - None
           """
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
            response = requests.get(self.endpoint + query_string)
            data = response.json()
            if response.status_code == 200:
                if location_name is not None:
                    data['location_name'] = location_name
                return json.dumps(data)
        except RequestException as e:
            raise ValueError(f"{response.status_code} Code: {response.text}  - Error fetching weather data from API")

    def get_locations(self, city_names):
        """
        Retrieves city names based on latitude and longitude coordinates.

        Parameters:
        - city_names (list): List of city names.

        Returns:
        - dict: A dictionary with latitude-longitude tuples as keys and corresponding city names as values.
        """
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
                    city_data = max(data['results'], key=lambda x: x.get('population', 0))
                    locations[(city_data['latitude'], city_data['longitude'])] = city_data['name']
        return locations

    def get_all_weather_data(self, city_names):
        """
        Helper function to request the data for cities in a list.

        Parameters:
        - city_names (list): List of city names.

        Returns:
        - dict: JSON file.
        """
        results = []
        locations = self.get_locations(city_names)
        for location in locations:
            lat, lon = location
            city_name = locations[location]
            data = self.get_weather_data(lat, lon, city_name)
            results.append(data)
        return results

    def save_data(self, data):
        current_date = date.today().strftime("%Y-%m-%d")
        file_name = f"data/data_{current_date}.json"

        with open(file_name, "w") as file:
            json.dump(data, file)
