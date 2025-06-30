-- models/marts/f_orders.sql

{{
    config(
        materialized='incremental',
        engine='ReplacingMergeTree()',
        order_by=('order_timestamp', 'order_id'),
        unique_key='order_id'
    )
}}

WITH latest_orders_from_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_timestamp_ms DESC) as rn
    FROM
        {{ ref('stg_orders') }}

    {% if is_incremental() %}
    WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Surrogate Keys
    du.dbt_scd_id AS user_sk,
    toInt32(formatDateTime(toDateTime(lo.order_timestamp), '%Y%m%d')) AS order_date_sk,

    -- Business Key
    lo.order_id,

    -- Fakten & Attribute
    CASE WHEN lo.tip_given IS NULL THEN false ELSE lo.tip_given END AS is_tip_given,
    CASE WHEN lo.op_type = 'd' THEN false ELSE true END AS is_active,

    -- Timestamps
    lo.order_timestamp,
    lo.staging_load_timestamp

FROM
    latest_orders_from_source lo

LEFT JOIN {{ ref('dim_users') }} du
    ON lo.user_id = du.user_id
    AND lo.source_timestamp_ms >= du.dbt_valid_from
    AND lo.source_timestamp_ms < coalesce(du.dbt_valid_to, toDateTime64('9999-12-31 23:59:59.999999', 6))


WHERE
    lo.rn = 1