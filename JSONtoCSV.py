
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import json
from datetime import date
import glob
with open('data/data_2023-05-26.json', "r") as file:
    data_json = json.load(file)

def json_to_csv():
#Load every city as a list of dicts
    data_parsed = []
    for city in data_json:
        data_parsed.append(json.loads(city))

    keys_to_normalize = list(data_parsed[0]['daily'].keys()) #This is ok, because each entry has the same structure
    weather_df = pd.DataFrame()
    n=0
    for nkey in keys_to_normalize:
        if n!=1: #Hacky way to add location name without duplicating
            concat_df = pd.json_normalize(data_parsed,record_path=['daily',[f'{nkey}']],meta=['location_name','longitude','latitude'])
            n=1
        else:
            concat_df = pd.json_normalize(data_parsed, record_path=['daily', [f'{nkey}']])
            concat_df.rename(columns={concat_df.columns[0]: f"{nkey}"}, inplace=True)
        weather_df=pd.concat([weather_df,concat_df],axis=1)

    weather_df.rename(columns={0:'time'},inplace=True)


    weather_df['time']=pd.to_datetime(weather_df.loc[:,'time'],format='%Y-%m-%d')
    weather_df=weather_df.loc[ (weather_df['time'].dt.year != 2023) | (weather_df['time'].dt.year != 1940)]
    location_names=['Tokyo','Sydney','Cape Town','Rio de Janeiro','Moscow','Toronto','Reykjavik']
    for lc in location_names:
        city_data=weather_df[weather_df['location_name']==lc]
    #    city_data=city_data[['time','location_name']]
        current_date = date.today().strftime("%Y-%m-%d")
        file_name = f"data/data_{lc}_2023-06-08.csv"
        city_data.to_csv(file_name)

def clean_outlier_csv():
    # import required module


    # assign directory
    directory = 'data'

    # iterate over files in
    # that directory
    for filename in glob.iglob(f'{directory}/*'):
        if not filename.startswith('data\data_'):

            outlier_df=pd.read_csv(filename)
            print(outlier_df.columns,filename)
            outlier_df  = outlier_df[['time','location_name']]
            outlier_df.to_csv(filename)

clean_outlier_csv()