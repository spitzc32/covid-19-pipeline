import pandas as pd

def validate_columns(df, expected_columns):
    """
    Validate if a DataFrame has the specified columns.

    :params:
        df: The DataFrame to validate.
        expected_columns: A list of column names to check for.

    returns:
        True if all expected columns are present, False otherwise.
    """
    return pd.Index(expected_columns).isin(df.columns)

def validate_dataframes(dataframes, expected_columns):
    """
    Validate if multiple DataFrames have the same specified columns.

    :params:
        ataframes: A list of DataFrames to validate.
        expected_columns: A list of column names to check for.

    :returns:
        True if all DataFrames have the same expected columns, False otherwise.
    """
    return all(all(validate_columns(df, expected_columns)) for df in dataframes)