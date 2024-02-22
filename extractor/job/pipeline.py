import pandas as pd 

from dagster import graph, op, Field, Array, String
from extractor.resources.sql_alchemy import SqlAlchemyClientResource
from extractor.job.task import transform_data_dates, clean_and_merge_dataframe
from extractor.utils.preprocess import (
    convert_str_to_date, 
    fill_na_values, 
    fix_country_and_province_data
)

@op(config_schema={
    'file_list': Field(Array(String), description='list of file urls'),
})
def clean_and_merge_data(context):
    """
    transforms data to desired format before cleaning and handling missing values.

    :param:
        context: this is where we get out inputs from dagster
    """
    context.log.info(context.op_config['file_list'])

    urls =context.op_config['file_list']
    expected_columns = ['Province/State', 'Country/Region', 'Lat', 'Long']

    try:
        context.log.info(f'Start Melting Dates as values')
        conf_df_long, deaths_df_long, recv_df_long = transform_data_dates(urls, expected_columns)
        context.log.info(f'Start Melting Dates as values')

        context.log.info(f'Start Cleaning and Merging Sources')
        full_table = clean_and_merge_dataframe(conf_df_long, deaths_df_long, recv_df_long)
        context.log.info(f'Done Cleaning and Merging Sources')

        return full_table
    except Exception as e:
        context.log.error(f'Cannot process Pipeline: {e}')


@op
def preprocess_and_save_values(context, full_table: pd.DataFrame, postgres_io_manager: SqlAlchemyClientResource):
    """
    Initial preprocessing data by converting to proper date format, fill the NaN values by default 0 for 
    the merged cases and fixing the known errors for country and region column. This is a primary step 
    before the transformations to happen in dbt

    """
    context.log.info('preprocessing data after merge')
    convert_str_to_date(full_table, 'Date')
    fix_country_and_province_data(full_table)
    fill_na_values(full_table, ['Province/State'], '')
    fill_na_values(full_table, ['Confirmed', 'Deaths', 'Recovered'], 0)
    full_table['Active'] = full_table['Confirmed'] - full_table['Deaths'] - full_table['Recovered']

    context.log.info('preprocessed data after merge')

    full_table.to_sql('covid_19_hist', postgres_io_manager.create_engine(), if_exists='replace', index=False)
    context.log.info('Loaded transformed data into the database')

@graph
def etl_dag_graph():
    preprocess_and_save_values(clean_and_merge_data())

