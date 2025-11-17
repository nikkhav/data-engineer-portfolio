--STV202510067

CREATE TABLE IF NOT EXISTS STV202510067__DWH.s_auth_history (
    hk_l_user_group_activity INT NOT NULL CONSTRAINT fk_s_auth_history_user_group_activity REFERENCES STV202510067__DWH.l_user_group_activity(hk_l_user_group_activity),
    user_id_from INT,
    event VARCHAR(30),
    event_dt DATETIME,
    load_dt DATETIME,
    load_src VARCHAR(20)
)

ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);