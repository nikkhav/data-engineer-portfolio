--STV202510067

CREATE TABLE IF NOT EXISTS STV202510067__STAGING.group_log (
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(30),
    datetime DATETIME
)
ORDER BY group_id, datetime
SEGMENTED BY HASH(group_id) ALL NODES;

-- Вопрос ревьюеру: нужно ли добавлять load_dt и load_src?