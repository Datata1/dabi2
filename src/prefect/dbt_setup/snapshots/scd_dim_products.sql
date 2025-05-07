-- snapshots/scd_dim_products.sql (Korrigiert)
{% snapshot dim_products %}

{% set updated_at_column = 'effective_last_updated_ts' %} 


{{
    config(
      target_schema='main',
      unique_key='product_id',
      strategy='timestamp',
      updated_at=updated_at_column, 
      invalidate_hard_deletes=True
    )
}}

{% if var('is_initial_snapshot_load', false) %}

  SELECT
      product_id,
      product_name,
      aisle_id,
      aisle_name,
      department_id,
      department_name,
      CAST(historical_effective_from AS TIMESTAMP WITH TIME ZONE) AS effective_last_updated_ts
  FROM {{ ref('historical_dim_products_source') }}

{% else %}

  SELECT * FROM {{ ref('int_product_snapshot_input') }}

{% endif %}

{% endsnapshot %}