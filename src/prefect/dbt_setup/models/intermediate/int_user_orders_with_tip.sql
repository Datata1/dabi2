WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

tips AS (
    SELECT * FROM {{ ref('stg_tips') }}
)

SELECT
    o.user_id,
    o.order_id,
    o.order_timestamp,
    -- Jede Bestellung aus 'orders' sollte einen Eintrag in 'tips' haben,
    -- AUSSER den letzten Bestellungen pro User (Testdaten).
    -- Daher sollte tip_given hier NULL sein für Testdaten.
    t.tip_given
FROM orders o
LEFT JOIN tips t ON o.order_id = t.order_id
-- Wichtig für Zeitreihenanalyse: Sortierung sicherstellen
-- Die Sortierung für die Zeitreihe selbst muss später pro User erfolgen
ORDER BY o.user_id, o.order_timestamp