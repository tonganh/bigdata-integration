from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import date
import pendulum


default_args = {
    'owner': 'jazzdung',
    'retries':5,
    'retry_delay': timedelta(minutes=20)
}

def get_this_week():
    today = date.today()
    today_date = today.strftime("%m%Y")
    week = str(int(today.strftime("%d"))//7 +1)
    return today_date+ "_" +week


with DAG(
    dag_id='data_crawling',
    description='Crawl data',
    start_date=pendulum.yesterday(),
    schedule_interval='0 0 * * 1',
    catchup=True
) as dag:

    task_create_raw_folder = BashOperator(
        task_id='create_today_raw_folder',
        bash_command='HADOOP_USER_NAME=hadoop /home/jazzdung/hadoop/bin/hdfs dfs -mkdir /user/hadoop/raw/{{ params.date }}',
        params = {'date' : get_this_week()}
    )

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

    task_create_raw_folder >> [task_crawl_shopee_url , task_crawl_shopee_data, task_crawl_lazada_url, task_crawl_lazada_data]