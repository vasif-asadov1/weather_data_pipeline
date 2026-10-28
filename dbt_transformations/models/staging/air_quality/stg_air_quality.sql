SELECT 
    city,
    country,
    -- Extract just the date part (YYYY-MM-DD) from the hourly timestamp
    CAST(SUBSTR(time, 1, 10) AS DATE) AS record_date,
    CAST(pm10 AS DOUBLE) AS pm10,
    CAST(pm2_5 AS DOUBLE) AS pm2_5,
    CAST(carbon_monoxide AS DOUBLE) AS carbon_monoxide,
    CAST(nitrogen_dioxide AS DOUBLE) AS nitrogen_dioxide,
    CAST(ozone AS DOUBLE) AS ozone
FROM {{ source('bronze_layer', 'air_quality_raw') }}