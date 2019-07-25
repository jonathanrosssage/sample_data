from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import date, timedelta, datetime

import format_data

DAG_DEFAULT_ARGS = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 1)
}

with DAG('results_to_mysql', start_date = datetime(2019, 01, 01), schedule_interval = "@daily", default_args = DAG_DEFAULT_ARGS, catchup = False) as dag:
        folder_scanner = FileSensor(task_id = "folder_scanner", fs_conn_id = "fs_default", filepath = "(#redacted#)/data.csv", poke_interval = 5)
        format_data = PythonOperator(task_id = "format_data", python_callable = format_data.main)
        upload_to_db = MySqlOperator(task_id = "upload_to_db", sql = "LOAD DATA INFILE '(#redacted#)/processed.csv' INTO TABLE staging FIELDS TERMINATED BY ',' IGNORE 1 ROWS PARTITION (dt='2019-01-01')")
        folder_scanner >> format_data >> upload_to_db
