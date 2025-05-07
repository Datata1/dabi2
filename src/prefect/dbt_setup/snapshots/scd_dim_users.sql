-- snapshots/scd_dim_users.sql (Korrigiert)
{% snapshot dim_users %}

{% set updated_at_column = 'effective_last_updated_ts' %} 


{{
    config(
      target_schema='main',
      unique_key='user_id',
      strategy='timestamp',
      updated_at=updated_at_column, 
      invalidate_hard_deletes=True
    )
}}

{% if var('is_initial_snapshot_load', false) %}

  SELECT
      user_id,
      CAST(historical_effective_from AS TIMESTAMP WITH TIME ZONE) AS effective_last_updated_ts
  FROM {{ ref('historical_dim_users_source') }}

{% else %}


  SELECT * FROM {{ ref('int_user_snapshot_input') }}

{% endif %}

{% endsnapshot %}