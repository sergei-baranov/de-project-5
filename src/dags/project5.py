from datetime import datetime, timedelta
import string
import pendulum

import pprint

import os

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task

import logging
log = logging.getLogger(__name__)

import boto3

import vertica_python

conn_info = {
    'host': '51.250.75.20',
    'port': '5433',
    'user': '***',
    'password': '***',
    'database': 'dwh',
    'autocommit': False
}


def exec_sql_vertica(sql: string):
    global conn_info;
    conn_info['user'] = Variable.get('baranov_vertica_uname')
    conn_info['password'] = Variable.get('baranov_vertica_password')
    # log.info(pprint.pformat(conn_info))
    with vertica_python.connect(**conn_info) as conn, conn.cursor() as curs:
        curs.execute(sql)
        conn.commit()
        curs.close()
        conn.close()

def load_staged_file_into_stage(key: str):
    sql_statement = f"""
        COPY "SERGEI_BARANOVTUTBY__STAGING"."{key}"
        FROM LOCAL '/data/{key}.csv' DELIMITER ','
        REJECTED DATA AS TABLE "SERGEI_BARANOVTUTBY__STAGING"."{key}_rej"
    """
    exec_sql_vertica(sql_statement)
    log.info("load_staged_file_into_stage for key '" + key + "' is done")


def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
    session = boto3.session.Session()
    saveFilePath = '/data/' + key
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket, # 'sprint6',
        Key=key, # 'groups.csv',
        Filename=saveFilePath
    )
    file_stats = os.stat(saveFilePath)
    bytesSize = file_stats.st_size
    log.info("fetch_s3_file into file '" + saveFilePath + "' is done (" + str(bytesSize) + " Bytes)")


def fill_init_vars():
    """
    заполняет Variable Airflow:
    credentials для conn_info в Variables,
    sql_path (путь к дире с файлами sql-запросов)
    """

    # Fill creds into conn_info:
    # как бы мы знаем, что credintials в коде хранить нельзя,
    # но как бы мы понимаем, что у ревьювера нет моих credintials,
    # и мне их таки надо подсунуть в Variable программно.

    # ответвление по поводу самим же и заполнить переменные,
    # которые читаем
    uname = 'SERGEI_BARANOVTUTBY'
    pwd = '5YlE0nqGcpEVJVq'
    vertica_uname = None
    try:
        vertica_uname = Variable.get('baranov_vertica_uname')
    except Exception as e:
        Variable.set('baranov_vertica_uname', uname)
    vertica_password = None
    try:
        vertica_password = Variable.get('baranov_vertica_password')
    except Exception as e:
        Variable.set('baranov_vertica_password', pwd)

    vertica_uname = Variable.get('baranov_vertica_uname')
    if vertica_uname is None:
         Variable.set('baranov_vertica_uname', uname)
    vertica_password = Variable.get('baranov_vertica_password')
    if vertica_password is None:
         Variable.set('baranov_vertica_password', pwd)

    # нормальная логика
    global conn_info
    conn_info['user'] = Variable.get('baranov_vertica_uname')
    conn_info['password'] = Variable.get('baranov_vertica_password')

    # почему-то не хочет log.info форматировать сама, через *args, форматирую тут и далее явно
    log.info("filled conn_info creds: 'user' len is '{0}', 'password' len is '{1}'".format(
        str(len(conn_info['user'])), str(len(conn_info['password']))
    ))

    # Fill sql_path variable:
    curr_dir_path = os.path.dirname(os.path.abspath(__file__))
    log.info("curr_dir_path is: '" + str(curr_dir_path) + "'")
    sql_path =  os.path.abspath(curr_dir_path + '/../sql')
    log.info("filled sql_path var: '" + str(sql_path) + "'")
    Variable.set('sql_path', sql_path)


# сам себе: docker cp \
# /home/vonbraun/YA_DE/SPRINT8_Аналитические_базы_данных/s6-lessons/sql \
# 829ea8b932b1:/lessons/
def make_init_sql():
    """
    исполняет init.sql, там DDL на создание таблиц IF NOT EXISTS
    """
    sql_path = Variable.get('sql_path');
    with open(sql_path + '/init.sql') as file:
        init_query = file.read()
        exec_sql_vertica(init_query)


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project5():
    bucket_files = [
        # 'dialogs.csv',
        # 'groups.csv',
        # 'users.csv',
        'group_log.csv'
    ]


    @task(task_id='init')
    def init():
        fill_init_vars()
        make_init_sql()


    @task(task_id=f'fetch_group_log')
    def fetch_group_log() -> None:
        fetch_s3_file(bucket='sprint6', key='group_log.csv')

    @task(task_id=f'print_10_lines_of_each')
    def print_10_lines_of_each() -> None:
        num_lines = 10
        for filename in bucket_files:
            print(f"First 10 lines of the {filename}")
            with open(f"/data/{filename}", "r") as f:
                for i in range(num_lines):
                    log.info(f.readline())
            with open(f"/data/{filename}", "r") as f:
                row_count = sum(1 for line in f)
                log.info("\n" + filename + "row_count: " + str(row_count) + "\n")


    @task(task_id=f'load_group_log')
    def load_group_log() -> None:
        exec_sql_vertica('TRUNCATE TABLE "SERGEI_BARANOVTUTBY__STAGING"."group_log"');
        load_staged_file_into_stage('group_log')


    @task(task_id=f'fill_l_user_group_activity')
    def fill_l_user_group_activity() -> None:
        exec_sql_vertica('TRUNCATE TABLE "SERGEI_BARANOVTUTBY__DWH"."l_user_group_activity"');
        sql_path = Variable.get('sql_path');
        with open(sql_path + '/fill_l_user_group_activity.sql') as file:
            sql_query = file.read()
            exec_sql_vertica(sql_query)


    @task(task_id=f'fill_s_auth_history')
    def fill_s_auth_history() -> None:
        exec_sql_vertica('TRUNCATE TABLE "SERGEI_BARANOVTUTBY__DWH"."s_auth_history"');
        sql_path = Variable.get('sql_path');
        with open(sql_path + '/fill_s_auth_history.sql') as file:
            sql_query = file.read()
            exec_sql_vertica(sql_query)


    init() >> fetch_group_log() >> print_10_lines_of_each() >> load_group_log() >> \
    fill_l_user_group_activity() >> fill_s_auth_history()

_ = project5()