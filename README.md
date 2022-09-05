# Проект 5

## DAG

```bash
src/dags/project5.py
```

```python
    # инициализирует переменные, DDL из src/sql/init.sql
    init() >> \
    # стейджинг из s3 в локальный файл
    fetch_group_log() >> \
    # точка контроля, схода и расхода параллельных тасков
    # (если бы отрабатывали все 4 источника)
    print_10_lines_of_each() >> \
    # стейджинг из локального файла в staging-слой бд
    load_group_log() >> \
    # заполнение DDS l_user_group_activity (src/sql/fill_l_user_group_activity.sql)
    fill_l_user_group_activity() >> \
    # заполнение DDS s_auth_history (src/sql/fill_s_auth_history.sql)
    fill_s_auth_history()
```

Всё линейно, так как все параллельные задачи выполнены в рамках спринта.

## Ответ на вопрос бизнеса

```bash
src/sql/4analytics.sql
```