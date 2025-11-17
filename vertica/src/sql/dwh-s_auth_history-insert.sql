--STV202510067

INSERT INTO STV202510067__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

SELECT
    luga.hk_l_user_group_activity AS hk_l_user_group_activity,
    gl.user_id_from AS user_id_from,
    gl.event AS event,
    gl.datetime AS event_dt,
    NOW() AS load_dt,
    's3' AS load_src

FROM STV202510067__STAGING.group_log as gl
LEFT JOIN STV202510067__DWH.h_groups as hg ON gl.group_id = hg.group_id
LEFT JOIN STV202510067__DWH.h_users as hu ON gl.user_id = hu.user_id
LEFT JOIN STV202510067__DWH.l_user_group_activity as luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
;
