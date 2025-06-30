{% snapshot dim_products %}

{{
    config(
      target_schema='default_marts',
      strategy='timestamp',
      unique_key='product_id',
      updated_at='effective_last_updated_ts',
      hard_deletes='invalidate'
    )
}}

SELECT * FROM {{ ref('int_product_snapshot_input') }}

{% endsnapshot %}