from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum


default_args = {
    'owner': 'jazzdung',
    'retries':5,
    'retry_delay': timedelta(minutes=20)
}

with DAG(
    dag_id='data_crawling',
    description='Crawl data',
    start_date=pendulum.yesterday(),
    schedule_interval='0 0 * * 1',
    catchup=True
) as dag:

    task_crawl_shopee_url = BashOperator(
        task_id='crawl_shopee_url',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site shopee --type url --num_page 1'
    )

    task_crawl_lazada_url = BashOperator(
        task_id='crawl_lazada_url',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site lazada --type url --num_page 1'
    )

    task_crawl_shopee_data = BashOperator(
        task_id='crawl_shopee_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site shopee --type info'
    )

    task_crawl_lazada_data = BashOperator(
        task_id='crawl_lazada_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site lazada --type info'
    )

    task_crawl_shopee_url  
    task_crawl_shopee_data 
    task_crawl_lazada_url  
    task_crawl_lazada_data