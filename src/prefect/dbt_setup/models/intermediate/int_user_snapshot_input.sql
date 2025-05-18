SELECT
    p_attr.user_id,  
    to_timestamp(
        GREATEST(
            p_attr.last_changed_ts
        ) / 1000
    ) AS effective_last_updated_ts
FROM {{ ref('int_current_user_names') }} p_attr

