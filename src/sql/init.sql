-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__STAGING"."group_log";

-- исходим из того, что SERGEI_BARANOVTUTBY__STAGING.groups и т.п.
-- уже созданы и заполнены в процессе исполнения домашек спринта,
-- см. dag-и и сиквел по ним
CREATE TABLE IF NOT EXISTS "SERGEI_BARANOVTUTBY__STAGING"."group_log" (
    "group_id" INTEGER NOT NULL,
    "user_id" INTEGER NOT NULL,
    "user_id_from" INTEGER DEFAULT NULL,
    "event" VARCHAR(32) NOT NULL, -- 'create'|'add'|'leave'
    "datetime" TIMESTAMP NOT NULL,
    CONSTRAINT group_log_group_id_fkey FOREIGN KEY (group_id) REFERENCES groups(id),
    CONSTRAINT group_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT group_log_user_id_from_fkey FOREIGN KEY (user_id_from) REFERENCES users(id)
)
-- далее всё по аналогии с таблицей __STAGING.groups из примера к домашке
ORDER BY group_id, user_id
PARTITION BY "datetime"::date
-- SEGMENTED BY id ALL NODES -- SEGMENTED в заданиях для staging-слоя мы почему-то не указывали
-- сам себе: см. https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/Projections/AutoProjections.htm
-- , там "Default Segmentation and Sort Order";
-- в спринте см. урок "9. Выбираем параметры сегментации", там раздел "Параметры по умолчанию для сортировки и сегментации"
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2)
;

-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__DWH"."l_user_group_activity";

-- исходим из того, что таблицы под FK уже созданы в рамках заданий спринта
CREATE TABLE "SERGEI_BARANOVTUTBY__DWH"."l_user_group_activity"
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_user
        REFERENCES SERGEI_BARANOVTUTBY__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_group
        REFERENCES SERGEI_BARANOVTUTBY__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
-- далее - по аналогии с DDL на линки в заданиях в спринте
-- (см. урок "6. Разработка аналитического хранилища")
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;

-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__DWH"."s_auth_history";
CREATE TABLE "SERGEI_BARANOVTUTBY__DWH"."s_auth_history"
(
    hk_l_user_group_activity BIGINT NOT NULL
        CONSTRAINT fk_s_auth_history_l_user_group_activity
        REFERENCES SERGEI_BARANOVTUTBY__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INTEGER DEFAULT NULL
        CONSTRAINT fk_s_auth_history_h_users
        REFERENCES SERGEI_BARANOVTUTBY__DWH.h_users (hk_user_id),
    event VARCHAR(32) NOT NULL, -- 'create'|'add'|'leave'
    event_dt DATETIME,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
-- далее - по аналогии с DDL на сателлиты в заданиях в спринте
-- (см. урок "6. Разработка аналитического хранилища")
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;