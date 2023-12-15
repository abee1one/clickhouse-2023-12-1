# 1. Postgres sales(price, qty, event_date) to Clickhouse sales
# 2.aggr_sales (event_date, sum_income)
# 3. sum(sum_income) -> print
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from apache.airflow.providers.clickhouse.hooks.ClickhouseHook import ClickhouseHook
from apache.airflow.providers.clickhouse.operators.ClickhouseOperator import ClickhouseOperator

ch_url = 'http://default:password@localhost:8123/test'
pg_url = 'pgconn://root:root@localhost:5432/postgres'
# create table aggr_sales(event_date, sum_income, w_insert_dt) Engine = ReplacingMergeTree(w_insert_dt) ORDER by event_date

def transfer():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn-id', database='etl')
    ch_hook = ClickhouseHook(click_conn_id='click-conn-id', database='etl')
    records = pg_hook.get_records("select * from sales where event_date = {{ds}}")
    ch_hook.run("insert into sales values", records)


with DAG(dag_id='etl_example', start_date=datetime(2023,12,15), catchup=False) as dag:
    PythonOperator(task_id='transfer_task', python_callable=transfer) >> ClickhouseOperator(task_id='click_aggr_task', sql="""
    INSERT INTO aggr_sales
    SELECT event_date, sum(price*qty)  from sales
    WHERE event_date = {{ds}}
    GROUP BY event_date;
    OPTIMIZE TABLE aggr_sales FINAL;
    SELECT sum(sum_income) as all_income from aggr_sales;
    """) >> PythonOperator(
        task_id='print_task',
        python_callable=lambda task_instance: print(task_instance.ecom_pull(task_id='click_aggr_task', key='return_value')))

if __name__ == "__main__":
    dag.test()