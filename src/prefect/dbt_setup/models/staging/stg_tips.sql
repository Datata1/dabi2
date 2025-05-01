SELECT
    order_id::BIGINT AS order_id,
    -- Wandle BOOLEAN direkt in INTEGER um (TRUE->1, FALSE->0, NULL->NULL)
    tip::INTEGER AS tip_given
FROM {{ source('raw_data', 'raw_tips') }}