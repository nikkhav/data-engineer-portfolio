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
)

SELECT hk_group_id, registration_dt, cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users DESC
;
