import pandas as pd

from extractor.utils.validator import validate_dataframes
from extractor.utils.filter import filter_data_without_longlat


def transform_data_dates(urls, expected_columns):
    """
    transforms data to desired format before cleaning and handling missing values.

    :param:
        context: this is where we get out inputs from dagster
    """
  
    confirmed_df = pd.read_csv(urls[0])
    deaths_df = pd.read_csv(urls[1])
    recovery_df = pd.read_csv(urls[2])
    
    if not validate_dataframes([confirmed_df, deaths_df, recovery_df], expected_columns):
        raise Exception('Columns in URL changed. Please review the expected columns')

    dates = confirmed_df.columns[4:]

    conf_df_long = confirmed_df.melt(id_vars=expected_columns, 
                                value_vars=dates, var_name='Date', value_name='Confirmed')

    deaths_df_long = deaths_df.melt(id_vars=expected_columns, 
                                value_vars=dates, var_name='Date', value_name='Deaths')

    recv_df_long = recovery_df.melt(id_vars=expected_columns, 
                                value_vars=dates, var_name='Date', value_name='Recovered')
    
    return conf_df_long, deaths_df_long, recv_df_long
    

def clean_and_merge_dataframe( 
    conf_df_long:pd.DataFrame, 
    deaths_df_long:pd.DataFrame, 
    recv_df_long:pd.DataFrame
):
    """
    filter data of missing values found
    """
   
    conf_df_cleaned = filter_data_without_longlat(conf_df_long)
    deaths_df_cleaned = filter_data_without_longlat(deaths_df_long)
    recv_df_cleaned = filter_data_without_longlat(recv_df_long)

    full_table = pd.merge(left=conf_df_cleaned, right=deaths_df_cleaned, how='left',
                      on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])
    full_table = pd.merge(left=full_table, right=recv_df_cleaned, how='left',
                        on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])

    return full_table