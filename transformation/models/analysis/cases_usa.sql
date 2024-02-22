SELECT
   *
FROM
    {{ ref('cases_per_country') }}
WHERE
    country = 'US'
