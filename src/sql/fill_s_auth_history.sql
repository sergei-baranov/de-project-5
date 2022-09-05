-- заполнение таблицы сателлитов s_auth_history;
-- по аналогии с заполнением таблиц-сателлитов в заданиях в спринте
-- (см. урок "6. Разработка аналитического хранилища"),
-- а вообще нет - по шаблону из ТЗ проекта
INSERT INTO SERGEI_BARANOVTUTBY__DWH.s_auth_history
    (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
    luga.hk_l_user_group_activity,
    sgl.user_id_from,
    sgl.event,
    sgl."datetime",
    now() as load_dt,
    's3' as load_src
FROM
    SERGEI_BARANOVTUTBY__STAGING.group_log as sgl
    INNER JOIN SERGEI_BARANOVTUTBY__DWH.h_groups as hg on sgl.group_id = hg.group_id
    INNER JOIN SERGEI_BARANOVTUTBY__DWH.h_users as hu on sgl.user_id = hu.user_id
    INNER JOIN SERGEI_BARANOVTUTBY__DWH.l_user_group_activity as luga ON (
        hg.hk_group_id = luga.hk_group_id
        and hu.hk_user_id = luga.hk_user_id
    )
;