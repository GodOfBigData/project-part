import datetime
import pandas as pd
from pandas import DataFrame
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.variable import Variable
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

try:
    last_timestamp = Variable.get("last_timestamp")
except Exception:
    last_timestamp = 0

def proccess_data():
    print("-----------------------------------------")
    engine = create_engine('postgresql://airflow:Unlucky369@localhost:5432/iot_integration')
    with engine.connect() as conn:
        tables = list(conn.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';").fetchall()[0])
        if table not in tables[0]:
            engine.execute(f"create table device_info_dds(time_event character varying," 
                        "timestamp_event integer, id_sensor bigint, coordinates point," 
                        "temp_controller integer, id_controler bigint, name_sensor character varying) distributed by (id_sensor);")
        df = pandas.read_sql_query(f"SELECT * FROM device_info_dds WHERE timestamp_event > {last_timestamp}")
        df = df.loc[df["temp_controller"] >= 0]
        timestamp = df["temp_controller"].iloc[-1]
        Variable.set("last_timestamp", timestamp)
        df.to_sql("device_info_dds", engine, schema = "public", if_exists='replace')
        


with DAG("DEVICES",
    schedule_interval="1 * * * *",
    description="2",
    catchup=False,
    start_date=datetime.datetime(2022, 10, 8)) as dag:
        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python_operator import PythonOperator


        start_step = EmptyOperator(task_id="start_step")

        proccess_data = PythonOperator(task_id="proccess_data", python_callable=proccess_data)

        start_step >> proccess_data
