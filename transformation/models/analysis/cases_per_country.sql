-- Step 1: Grouped by day, country
SELECT
    "Date" as datetime,
    "Country/Region" AS country,
    MAX("Lat") AS latitude,
    MAX("Long") AS longitude,
    SUM("Confirmed") AS Confirmed,
    SUM("Deaths") AS Deaths,
    SUM("Recovered") AS Recovered,
    SUM("Active") AS Active
FROM covid_19_hist
GROUP BY "Date", "Country/Region"