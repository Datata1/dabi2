-- snapshots/scd_products.sql (Angepasst für check-Strategie auf dim_products)

{% snapshot scd_products %}

{{
    config(
      target_schema='snapshots',
      strategy='check', 
      unique_key='product_id',
      check_cols=['product_name', 'aisle_id', 'aisle', 'department_id', 'department', 'is_active'],
      invalidate_hard_deletes=True,
      updated_at='source_timestamp_ms',
    )
}}

-- Wähle die Spalten aus der Zieldimension aus, die im Snapshot benötigt werden
-- (unique_key, check_cols, updated_at müssen dabei sein)
SELECT
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    is_active, -- Wichtig, um Deaktivierungen als Änderung zu erkennen!
    source_timestamp_ms -- Wird als 'updated_at' für die Strategie verwendet

FROM {{ ref('dim_products') }} -- Quelle ist die finale Dimensionstabelle!

{% endsnapshot %}