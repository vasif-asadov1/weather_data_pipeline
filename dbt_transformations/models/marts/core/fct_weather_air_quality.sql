WITH daily_weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

daily_air_quality AS (
    SELECT 
        city,
        record_date,
        MAX(pm10) AS max_pm10,
        MAX(pm2_5) AS max_pm2_5,
        MAX(carbon_monoxide) AS max_carbon_monoxide,
        MAX(nitrogen_dioxide) AS max_nitrogen_dioxide,
        MAX(ozone) AS max_ozone
    FROM {{ ref('stg_air_quality') }}
    GROUP BY city, record_date
)

SELECT 
    w.city,
    w.country,
    w.latitude,
    w.longitude,
    w.record_date,
    w.temperature_2m_max,
    w.temperature_2m_min,
    w.precipitation_sum,
    w.wind_speed_10m_max,
    w.wind_direction_10m_dominant,
    aq.max_pm10,
    aq.max_pm2_5,
    aq.max_carbon_monoxide,
    aq.max_nitrogen_dioxide,
    aq.max_ozone
FROM daily_weather w
LEFT JOIN daily_air_quality aq
    ON w.city = aq.city 
    AND w.record_date = aq.record_date