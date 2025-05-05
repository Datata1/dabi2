-- models/marts/dim_date.sql (Nur Quelle potenziell angepasst)
{{
    config(
        materialized='table'
    )
}}

-- Schritt 1: Finde den ersten und letzten Bestelldatum als Jinja-Variablen
{%- set date_query %}
    SELECT
        MIN(order_timestamp::date) as start_date, -- Spalte aus dem neuen stg_orders
        MAX(order_timestamp::date) as end_date
    FROM {{ ref('stg_orders') }} -- Nutzt das neue Staging-Modell
{% endset -%}

{# Restlicher Code bleibt wie gehabt, verwendet DuckDB-konforme Funktionen #}
{# ... (run_query, Logik mit date_spine, Attribut-Extraktion) ... #}
{# Stellen Sie sicher, dass dbt_utils installiert ist: packages.yml -> dbt deps #}

{%- set query_result = run_query(date_query) -%}
{{ log("run_query result: " ~ query_result, info=True) }}
{%- if query_result and query_result.rows -%}
    {%- set date_results = query_result.rows[0] -%}
    {{ log("date_results row: " ~ date_results, info=True) }}
    {%- set start_date = date_results['start_date'] -%}
    {%- set end_date = date_results['end_date'] -%}

    {% if start_date and end_date %}
        WITH date_spine AS (
            {{ dbt_utils.date_spine(
                datepart="day",
                start_date="cast('" ~ start_date ~ "' as date)",
                end_date="cast('" ~ end_date ~ "' as date)"
               )
            }}
        )
        SELECT
            d.date_day AS full_date,
            CAST(strftime(d.date_day, '%Y%m%d') AS INTEGER) AS date_sk,
            EXTRACT(YEAR FROM d.date_day) AS year,
            EXTRACT(MONTH FROM d.date_day) AS month,
            EXTRACT(DAY FROM d.date_day) AS day,
            EXTRACT(dayofweek FROM d.date_day) AS day_of_week,
            EXTRACT(dayofyear FROM d.date_day) AS day_of_year,
            EXTRACT(weekofyear FROM d.date_day) AS week_of_year,
            EXTRACT(QUARTER FROM d.date_day) AS quarter,
            CASE
                WHEN EXTRACT(dayofweek FROM d.date_day) IN (0, 6) THEN true
                ELSE false
            END AS is_weekend
        FROM date_spine d
    {% else %}
        {{ log("WARNUNG: Start- oder Enddatum ist NULL. dim_date wird leer sein.", info=True) }}
        SELECT CAST(NULL AS DATE) AS full_date, CAST(NULL AS INTEGER) AS date_sk, CAST(NULL AS INTEGER) AS year, CAST(NULL AS INTEGER) AS month, CAST(NULL AS INTEGER) AS day, CAST(NULL AS INTEGER) AS day_of_week, CAST(NULL AS INTEGER) AS day_of_year, CAST(NULL AS INTEGER) AS week_of_year, CAST(NULL AS INTEGER) AS quarter, CAST(NULL AS BOOLEAN) AS is_weekend LIMIT 0
    {% endif %}
{% else %}
    {{ log("FEHLER: run_query für Datumsbereich hat keine Ergebnisse geliefert. dim_date wird leer sein.", info=True) }}
    SELECT CAST(NULL AS DATE) AS full_date, CAST(NULL AS INTEGER) AS date_sk, CAST(NULL AS INTEGER) AS year, CAST(NULL AS INTEGER) AS month, CAST(NULL AS INTEGER) AS day, CAST(NULL AS INTEGER) AS day_of_week, CAST(NULL AS INTEGER) AS day_of_year, CAST(NULL AS INTEGER) AS week_of_year, CAST(NULL AS INTEGER) AS quarter, CAST(NULL AS BOOLEAN) AS is_weekend LIMIT 0
{% endif %}