{{
    config(
        materialized='table',
        tags=['marts_model'],
    )
}}

{%- set config_first_historical_date = '2020-01-01' -%}

{%- set py_today = modules.datetime.date.today() -%}
{%- set future_buffer_days = 365 -%}
{%- set py_calculated_end_date = py_today + modules.datetime.timedelta(days=future_buffer_days) -%}

{%- set final_start_date_str = config_first_historical_date -%}
{%- set final_end_date_str = py_calculated_end_date.strftime('%Y-%m-%d') -%}

{%- set py_start_for_check = modules.datetime.datetime.strptime(final_start_date_str, '%Y-%m-%d').date() -%}
{%- set py_end_for_check = modules.datetime.datetime.strptime(final_end_date_str, '%Y-%m-%d').date() -%}

{%- if py_end_for_check <= py_start_for_check -%}
    {%- set py_end_for_check = py_start_for_check + modules.datetime.timedelta(days=1) -%}
    {%- set final_end_date_str = py_end_for_check.strftime('%Y-%m-%d') -%}
{%- endif -%}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ final_start_date_str ~ "' as date)",
        end_date="cast('" ~ final_end_date_str ~ "' as date)"
       )
    }}
)
SELECT
    d.date_day AS full_date,
    toInt32(formatDateTime(d.date_day, '%Y%m%d')) AS date_sk,
    toYear(d.date_day) AS year,
    toMonth(d.date_day) AS month,
    toDayOfMonth(d.date_day) AS day,
    toDayOfWeek(d.date_day) AS day_of_week,
    toDayOfYear(d.date_day) AS day_of_year,
    toWeek(d.date_day) AS week_of_year,
    toQuarter(d.date_day) AS quarter,
    CASE
        WHEN toDayOfWeek(d.date_day) IN (6, 7) THEN true
        ELSE false
    END AS is_weekend
FROM date_spine d