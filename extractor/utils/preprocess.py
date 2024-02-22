import pandas as pd

def convert_str_to_date(df: pd.DataFrame, column: str):
    df[column] = pd.to_datetime(df[column])

def fix_country_and_province_data(df: pd.DataFrame):
    df['Country/Region'] = df['Country/Region'].replace('Korea, South', 'South Korea')
    df.loc[df['Province/State']=='Greenland', 'Country/Region'] = 'Greenland'
    df['Country/Region'] = df['Country/Region'].replace('Mainland China', 'China')
    df = df[df['Province/State'].str.contains('Recovered')!=True]
    df = df[df['Province/State'].str.contains(',')!=True]

def fill_na_values(df: pd.DataFrame, columns: list, fill_with):
    df[columns] = df[columns].fillna(fill_with)

