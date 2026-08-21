{{
    config(
        materialized='table',
        description='Date dimension table for the Gold layer. Covers 2026-07-01 to 2030-12-31.'
    )
}}

WITH date_bounds AS (
    SELECT
        DATE '2026-07-01'  AS min_date,
        DATE '2030-12-31'  AS max_date
),
date_spine AS (
    SELECT date_day
    FROM date_bounds,
    UNNEST(
        GENERATE_DATE_ARRAY(min_date, max_date, INTERVAL 1 DAY)
    ) AS date_day
)

SELECT
    -- Surrogate key
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64)  AS date_id,

    -- The date itself
    date_day                                         AS full_date,

    -- Year
    EXTRACT(YEAR  FROM date_day)                     AS year,

    -- Quarter
    EXTRACT(QUARTER FROM date_day)                   AS quarter_number,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING))  AS quarter_label,

    -- Month
    EXTRACT(MONTH FROM date_day)                     AS month_number,
    FORMAT_DATE('%B', date_day)                      AS month_name,
    FORMAT_DATE('%b', date_day)                      AS month_short_name,
    FORMAT_DATE('%Y-%m', date_day)                   AS year_month,

    -- Week
    EXTRACT(WEEK FROM date_day)                      AS week_of_year,
    EXTRACT(ISOWEEK FROM date_day)                   AS iso_week_of_year,
    EXTRACT(ISOYEAR FROM date_day)                   AS iso_year,

    -- Day
    EXTRACT(DAY FROM date_day)                       AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day)                 AS day_of_week,         -- 1=Sunday, 7=Saturday
    EXTRACT(DAYOFYEAR FROM date_day)                 AS day_of_year,
    FORMAT_DATE('%A', date_day)                      AS day_name,
    FORMAT_DATE('%a', date_day)                      AS day_short_name,

    -- Weekend / Weekday flag
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                              AS is_weekend,

    -- First / last day flags
    CASE
        WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE
        ELSE FALSE
    END                                              AS is_first_day_of_month,

    CASE
        WHEN date_day = LAST_DAY(date_day, MONTH) THEN TRUE
        ELSE FALSE
    END                                              AS is_last_day_of_month,

    CASE
        WHEN date_day = DATE_TRUNC(date_day, QUARTER) THEN TRUE
        ELSE FALSE
    END                                              AS is_first_day_of_quarter,

    CASE
        WHEN date_day = LAST_DAY(date_day, QUARTER) THEN TRUE
        ELSE FALSE
    END                                              AS is_last_day_of_quarter,

    -- Relative date helpers
    DATE_DIFF(CURRENT_DATE(), date_day, DAY)         AS days_ago,
    DATE_DIFF(CURRENT_DATE(), date_day, MONTH)       AS months_ago,
    DATE_DIFF(CURRENT_DATE(), date_day, YEAR)        AS years_ago

FROM date_spine
ORDER BY date_day
