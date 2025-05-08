-- snapshots/scd_dim_products.sql (Mit bedingter Strategie)
{% snapshot dim_products %}

{% set snapshot_config = {
      'target_schema': 'main',
      'unique_key': 'product_id',
      'invalidate_hard_deletes': True
    }
%}

{% if var('is_initial_snapshot_load', false) %}
  {% do snapshot_config.update({
        'strategy': 'timestamp',                   
        'updated_at': 'effective_last_updated_ts' 
     })
  %}
{% else %}
  {% do snapshot_config.update({
        'strategy': 'check',                      
        'check_cols': ['product_name', 'aisle_id', 'aisle_name', 'department_id', 'department_name'] 
     })
  %}
{% endif %}

{{
    config(**snapshot_config) 
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

  SELECT
      product_id,
      product_name,
      aisle_id,
      aisle_name,
      department_id,
      department_name,
      effective_last_updated_ts
  FROM {{ ref('int_product_snapshot_input') }}

{% endif %}

{% endsnapshot %}