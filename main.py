from WeatherApi import WeatherDataAPI

data_getter = WeatherDataAPI('1940-01-01','2023-05-20')
data = data_getter.get_all_weather_data(city_names=['Tokyo','Sydney','Cape Town','Rio de Janeiro','Moscow','Toronto','Reykjavik'],save_each=True)
data_getter.save_data(data)