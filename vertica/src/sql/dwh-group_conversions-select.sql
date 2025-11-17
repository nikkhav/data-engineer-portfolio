--STV202510067

WITH user_group_log as (
    SELECT
        luga.hk_group_id,
        hg.registration_dt,
        COUNT(DISTINCT hk_user_id) AS cnt_added_users
    FROM STV202510067__DWH.s_auth_history sah
    JOIN STV202510067__DWH.l_user_group_activity luga on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    JOIN STV202510067__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id
    WHERE sah.event = 'add'
    GROUP BY luga.hk_group_id, hg.registration_dt
    ORDER BY hg.registration_dt
    LIMIT 10
), user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV202510067__DWH.l_groups_dialogs AS lgd
    JOIN STV202510067__DWH.l_user_message   AS lum
      ON lgd.hk_message_id = lum.hk_message_id
    GROUP BY lgd.hk_group_id
)

SELECT
    ugl.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ROUND(
        COALESCE(ugm.cnt_users_in_group_with_messages, 0)
        / NULLIF(CAST(ugl.cnt_added_users AS FLOAT), 0),
        4
    ) AS group_conversion
FROM user_group_log ugl
LEFT JOIN user_group_messages ugm on ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC
;
