--STV202510067

CREATE TABLE IF NOT EXISTS STV202510067__DWH.l_user_group_activity (
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user REFERENCES STV202510067__DWH.h_users(hk_user_id),
    hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES STV202510067__DWH.h_groups(hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)

ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);