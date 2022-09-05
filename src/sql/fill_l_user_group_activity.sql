-- заполнение таблицы линков l_user_group_activity;
-- по аналогии с заполнением таблиц-линков в заданиях в спринте
-- (см. урок "6. Разработка аналитического хранилища"),
-- и см. шаблон из ТЗ проекта
INSERT INTO SERGEI_BARANOVTUTBY__DWH.l_user_group_activity
    (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
FROM
    SERGEI_BARANOVTUTBY__STAGING.group_log as sgl
    INNER JOIN SERGEI_BARANOVTUTBY__DWH.h_users AS hu ON hu.user_id = sgl.user_id
    INNER JOIN SERGEI_BARANOVTUTBY__DWH.h_groups AS hg ON hg.group_id = sgl.group_id
WHERE
    hash(hu.hk_user_id, hg.hk_group_id)
        NOT IN (SELECT hk_l_user_group_activity FROM SERGEI_BARANOVTUTBY__DWH.l_user_group_activity)
;