import pandas as pd
import json
import glob

with open('data/data_2023-05-26.json', "r") as file:
    data_json = json.load(file)

'''Function to unroll existing weather JSON file to separate CSV instances by city'''
def json_to_csv():

    data_parsed = []
    for city in data_json:
        data_parsed.append(json.loads(city))

    keys_to_normalize = list(data_parsed[0]['daily'].keys())  # This is ok, because each entry has the same structure
    weather_df = pd.DataFrame()
    n = 0
    for nkey in keys_to_normalize:
        if n != 1:  # Hacky way to add location name without duplicating
            concat_df = pd.json_normalize(data_parsed, record_path=['daily', [f'{nkey}']],
                                          meta=['location_name', 'longitude', 'latitude'])
            n = 1
        else:
            concat_df = pd.json_normalize(data_parsed, record_path=['daily', [f'{nkey}']])
            concat_df.rename(columns={concat_df.columns[0]: f"{nkey}"}, inplace=True)
        weather_df = pd.concat([weather_df, concat_df], axis=1)

    weather_df.rename(columns={0: 'time'}, inplace=True)

    weather_df['time'] = pd.to_datetime(weather_df.loc[:, 'time'], format='%Y-%m-%d')
    weather_df = weather_df.loc[(weather_df['time'].dt.year != 2023) | (weather_df['time'].dt.year != 1940)]
    location_names = ['Tokyo', 'Sydney', 'Cape Town', 'Rio de Janeiro', 'Moscow', 'Toronto', 'Reykjavik']
    for lc in location_names:
        city_data = weather_df[weather_df['location_name'] == lc]
        file_name = f"data/data_{lc}.csv"
        city_data.to_csv(file_name)

'''Removes extraneous data from outlier files'''
def clean_outlier_csv():

    directory = 'data'
    for filename in glob.iglob(f'{directory}/*'):
        if not filename.startswith('data\data_'):
            outlier_df = pd.read_csv(filename)
            print(outlier_df.columns, filename)
            outlier_df = outlier_df[['time', 'location_name']]
            outlier_df.to_csv(filename)

'''Add rankings to mean temperature, to help Tableau viz'''
def add_ranks(column='temperature_2m_mean'):
    directory = 'data'
    for filename in glob.iglob(f'{directory}/*'):
        if filename.startswith('data\data_') and filename.endswith('.csv'):
            city_df = pd.read_csv(filename)
            city_df[column+'_rank']=city_df[column].rank(pct=True)
            city_df.to_csv(filename)

'''I can't believe I have to do this in python
Will create a list of all the dates for outliers across all models without overlap'''
def create_all_outlier_dates():
    directory = 'data'
    outlier_time_df = pd.DataFrame()
    for filename in glob.iglob(f'{directory}/*'):
        if not filename.startswith('data\data_'):
            outlier_df = pd.read_csv(filename)

            outlier_time_df = pd.concat([outlier_df,outlier_time_df],axis=0).drop_duplicates()
    outlier_time_df.to_csv('data\Outlier_dates_no_dups.csv')

