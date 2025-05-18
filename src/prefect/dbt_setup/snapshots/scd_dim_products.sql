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


SELECT
    product_id,
    product_name,
    aisle_id,
    aisle_name,
    department_id,
    department_name,
    effective_last_updated_ts
FROM {{ ref('int_product_snapshot_input') }}



{% endsnapshot %}