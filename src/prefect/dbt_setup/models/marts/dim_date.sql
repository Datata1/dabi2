-- models/marts/dim_date.sql (Angepasst für DuckDB)
{{
    config(
        materialized='table'
    )
}}

-- Schritt 1: Finde den ersten und letzten Bestelldatum als Jinja-Variablen
{%- set date_query %}
    SELECT
        MIN(order_timestamp::date) as start_date,
        MAX(order_timestamp::date) as end_date
    FROM {{ ref('stg_orders') }}
{% endset -%}

{%- set query_result = run_query(date_query) -%}

-- MEHR LOGGING: Gib das Ergebnis von run_query aus
{{ log("run_query result: " ~ query_result, info=True) }}

{# Prüfe, ob das Ergebnis und die Zeilen existieren #}
{%- if query_result and query_result.rows -%}
    {%- set date_results = query_result.rows[0] -%}
    {{ log("date_results row: " ~ date_results, info=True) }}

    {%- set start_date = date_results['start_date'] -%}
    {%- set end_date = date_results['end_date'] -%}
    {{ log("Extracted start_date: " ~ start_date, info=True) }}
    {{ log("Extracted end_date: " ~ end_date, info=True) }}

    {# Führe den Rest nur aus, wenn Start- und Enddatum gültig sind #}
    {% if start_date and end_date %}

        -- Schritt 2: Generiere alle Tage im Datumsbereich mit dbt_utils.date_spine
        WITH date_spine AS (
            {{ dbt_utils.date_spine(
                datepart="day",
                start_date="cast('" ~ start_date ~ "' as date)",
                end_date="cast('" ~ end_date ~ "' as date)"
               )
            }}
        )

        -- Schritt 3: Extrahiere Datumsattribute und erstelle den Surrogate Key
        SELECT
            d.date_day AS full_date,
            -- Ersetze to_char durch strftime für DuckDB
            CAST(strftime(d.date_day, '%Y%m%d') AS INTEGER) AS date_sk,
            EXTRACT(YEAR FROM d.date_day) AS year,
            EXTRACT(MONTH FROM d.date_day) AS month,
            EXTRACT(DAY FROM d.date_day) AS day,
            -- Ersetze dow durch dayofweek für DuckDB (Sonntag=0)
            EXTRACT(dayofweek FROM d.date_day) AS day_of_week,
            -- Ersetze doy durch dayofyear für DuckDB
            EXTRACT(dayofyear FROM d.date_day) AS day_of_year,
            -- Ersetze week durch weekofyear für DuckDB
            EXTRACT(weekofyear FROM d.date_day) AS week_of_year,
            EXTRACT(QUARTER FROM d.date_day) AS quarter,
            CASE
                -- Verwende dayofweek für DuckDB (Sonntag=0, Samstag=6)
                WHEN EXTRACT(dayofweek FROM d.date_day) IN (0, 6) THEN true
                ELSE false
            END AS is_weekend
        FROM date_spine d

    {% else %}
        -- Fallback, wenn Start- oder Enddatum NULL ist (unverändert)
        {{ log("WARNUNG: Start- oder Enddatum ist NULL. dim_date wird leer sein.", info=True) }}
        SELECT CAST(NULL AS DATE) AS full_date, CAST(NULL AS INTEGER) AS date_sk, CAST(NULL AS INTEGER) AS year, CAST(NULL AS INTEGER) AS month, CAST(NULL AS INTEGER) AS day, CAST(NULL AS INTEGER) AS day_of_week, CAST(NULL AS INTEGER) AS day_of_year, CAST(NULL AS INTEGER) AS week_of_year, CAST(NULL AS INTEGER) AS quarter, CAST(NULL AS BOOLEAN) AS is_weekend LIMIT 0

    {% endif %}

{% else %}
    -- Fallback, wenn run_query fehlschlägt (unverändert)
    {{ log("FEHLER: run_query für Datumsbereich hat keine Ergebnisse geliefert. dim_date wird leer sein.", info=True) }}
    SELECT CAST(NULL AS DATE) AS full_date, CAST(NULL AS INTEGER) AS date_sk, CAST(NULL AS INTEGER) AS year, CAST(NULL AS INTEGER) AS month, CAST(NULL AS INTEGER) AS day, CAST(NULL AS INTEGER) AS day_of_week, CAST(NULL AS INTEGER) AS day_of_year, CAST(NULL AS INTEGER) AS week_of_year, CAST(NULL AS INTEGER) AS quarter, CAST(NULL AS BOOLEAN) AS is_weekend LIMIT 0

{% endif %}