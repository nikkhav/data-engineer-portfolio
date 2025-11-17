--STV202510067

INSERT INTO STV202510067__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)

SELECT DISTINCT
    HASH(gl.user_id, gl.group_id) AS hk_l_user_group_activity,
    hu.hk_user_id AS hk_user_id,
    hg.hk_group_id AS hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src

FROM STV202510067__STAGING.group_log as gl
LEFT JOIN STV202510067__DWH.h_users hu ON gl.user_id = hu.user_id
LEFT JOIN STV202510067__DWH.h_groups hg ON gl.group_id = hg.group_id
;
