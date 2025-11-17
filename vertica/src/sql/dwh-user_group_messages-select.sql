-- STV202510067

WITH user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV202510067__DWH.l_groups_dialogs AS lgd
    JOIN STV202510067__DWH.l_user_message   AS lum
      ON lgd.hk_message_id = lum.hk_message_id
    GROUP BY lgd.hk_group_id
)

SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages DESC
LIMIT 10;
