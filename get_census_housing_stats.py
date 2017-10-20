'''

@author: J Randazzo
'''
from sqlalchemy import create_engine
import pandas as pd
import requests
import json

def get_census_housing_content(acs_year):
    url = "https://api.census.gov/data/" + acs_year + "/acs5?get=B25001_001E,B25001_001M,B25002_002E,B25002_002M,B25002_003E,B25002_003M,B25040_001E,B25040_001M,B25040_002E,B25040_002M,B25040_003E,B25040_003M,B25040_004E,B25040_004M,B25040_005E,B25040_005M,B25040_006E,B25040_006M,B25040_007E,B25040_007M,B25040_008E,B25040_008M,B25040_009E,B25040_009M,B25040_010E,B25040_010M&for=zip%20code%20tabulation%20area:*&key=76ec564f7e0e639a3c9736ae63e969d79af1498f"
    census_response = requests.get(url)
    return census_response.text

postgres_engine = create_engine("postgresql://<DB Credentials here>")

acs_year = '2015'
census_json = json.loads(get_census_housing_content(acs_year))
df = pd.DataFrame.from_dict(census_json,orient='columns')

df.insert(0,'year',acs_year)
df = df.iloc[1:]

df.columns=[
'year','total_units','total_units_me','occupied_units','occupied_units_me',
'vacant_units','vacant_units_me','total_fuel','total_fuel_me','total_fuel_utility_gas',
'total_fuel_utility_gas_me','total_fuel_bottled_gas','total_fuel_bottled_gas_me',
'total_fuel_electricity','total_fuel_electricity_me','total_fuel_oil','total_fuel_oil_me',
'total_fuel_coal','total_fuel_coal_me','total_fuel_wood','total_fuel_wood_me','total_fuel_solar',
'total_fuel_solar_me','total_fuel_other','total_fuel_other_me','total_fuel_none','total_fuel_none_me','zip_code']


df.to_sql(name='qsdb_census_housing_characteristics', con=postgres_engine, if_exists = 'append', index=False)
postgres_engine.dispose();

print('----------------Census Data Processing Complete----------------')
