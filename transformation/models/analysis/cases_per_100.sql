SELECT
    datetime,
    SUM(Confirmed) AS Confirmed,
    SUM(Deaths) AS Deaths,
    SUM(Recovered) AS Recovered,
    SUM(Active) AS Active,
    ROUND(COALESCE((SUM(Deaths) / NULLIF(SUM(Confirmed), 0)) * 100, 0)) AS "Deaths / 100 Cases",
    ROUND(COALESCE((SUM(Recovered) / NULLIF(SUM(Confirmed), 0)) * 100, 0)) AS "Recovered / 100 Cases",
    ROUND(COALESCE((SUM(Deaths) / NULLIF(SUM(Recovered), 0)) * 100, 0)) AS "Deaths / 100 Recovered",
    COUNT(DISTINCT country) AS "No. of countries"
FROM cases_per_country
GROUP BY datetime