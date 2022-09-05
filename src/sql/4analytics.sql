WITH
cte_groups AS (
    SELECT
        hg.hk_group_id
    FROM
        SERGEI_BARANOVTUTBY__DWH.h_groups hg
    ORDER BY
        hg.registration_dt ASC
    LIMIT 10
)
,
cte_users AS (
    SELECT
        luga.hk_group_id,
        -- distinct - to not calc same users that was add->leave->add->...
        COUNT(DISTINCT luga.hk_user_id) as cnt_added_users
    FROM
        SERGEI_BARANOVTUTBY__DWH.s_auth_history sah
        INNER JOIN SERGEI_BARANOVTUTBY__DWH.l_user_group_activity luga ON (
            luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        )
    WHERE
        luga.hk_group_id IN (
            SELECT hk_group_id FROM cte_groups
        )
        AND sah.event = 'add' -- to not calc left users
    GROUP BY luga.hk_group_id
)
,
cte_messages AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) as cnt_users_in_group_with_messages
    FROM
        SERGEI_BARANOVTUTBY__DWH.l_user_message lum
        INNER JOIN SERGEI_BARANOVTUTBY__DWH.h_users hu ON hu.hk_user_id = lum.hk_user_id
        INNER JOIN SERGEI_BARANOVTUTBY__DWH.l_user_group_activity luga ON (
            luga.hk_user_id = hu.hk_user_id
        )
        INNER JOIN cte_groups cg ON cg.hk_group_id = luga.hk_group_id
    GROUP BY luga.hk_group_id
)
SELECT
    cg.hk_group_id
    , cu.cnt_added_users
    , cm.cnt_users_in_group_with_messages
    , cm.cnt_users_in_group_with_messages / cu.cnt_added_users AS group_conversion
FROM
    cte_groups cg
    INNER JOIN cte_users cu ON cu.hk_group_id = cg.hk_group_id
    INNER JOIN cte_messages cm ON cm.hk_group_id = cg.hk_group_id
ORDER BY group_conversion DESC
;
