{% snapshot dim_users %}

{% set updated_at_column = 'effective_last_updated_ts' %} 


{{
    config(
      target_schema='default_marts',
      unique_key='user_id',
      strategy='timestamp',
      updated_at=updated_at_column, 
      invalidate_hard_deletes=True,
      order_by=('user_id', 'dbt_valid_from')
    )
}}


SELECT * FROM {{ ref('int_user_snapshot_input') }}



{% endsnapshot %}