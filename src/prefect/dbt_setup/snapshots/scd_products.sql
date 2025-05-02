-- snapshots/scd_products.sql

{% snapshot scd_products %} 

{{
    config(
      target_schema='snapshots',    
      strategy='check',             
      unique_key='product_id',     
      check_cols=['product_name', 'aisle_id', 'aisle', 'department_id', 'department'],
      invalidate_hard_deletes=True, 
      updated_at='updated_at',
    )
}}

SELECT DISTINCT
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    updated_at
FROM {{ ref('stg_order_products') }}

{% endsnapshot %}