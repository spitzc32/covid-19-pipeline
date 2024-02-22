import pandas as pd


def filter_data_without_longlat(df: pd.DataFrame):
    grouped_df = df.groupby("Country/Region")

    countries_with_null = []

    # Iterate over groups
    for country, group in grouped_df:
        # Check if there are any null values in Lat or Long columns for this country
        if group["Lat"].isnull().any() or group["Long"].isnull().any():
            countries_with_null.append(country)
    
    df_cleaned = df[~df["Country/Region"].isin(countries_with_null)]
    return df_cleaned










