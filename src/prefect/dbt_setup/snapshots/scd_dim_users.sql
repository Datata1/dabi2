-- snapshots/scd_dim_users.sql
{% snapshot scd_dim_users %}

{{
    config(
      target_schema='snapshots',         
      strategy='timestamp',             
      unique_key='user_id',             
      updated_at='source_timestamp_ms', 
      invalidate_hard_deletes=True,      
    )
}}

-- Wähle die Spalten aus der Zieldimensionstabelle (dim_users) aus,
-- deren Historie du verfolgen möchtest.
-- 'unique_key' und 'updated_at' müssen enthalten sein.
select
    -- Der Business Key
    user_id,

    -- Spalten, deren Änderungen verfolgt werden sollen
    is_active, -- Wichtig, um zu sehen, wann ein User (logisch) gelöscht wurde

    -- Der Zeitstempel, der für die 'timestamp'-Strategie verwendet wird
    source_timestamp_ms

from {{ ref('dim_users') }} -- Quelle ist die finale, inkrementell gebaute User-Dimension

{% endsnapshot %}