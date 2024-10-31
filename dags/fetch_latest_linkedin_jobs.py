from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import csv



default_args = {
    'owner': 'airflow', 
    'depends_on_past': False, 
    'start_date': datetime(2024, 10, 23), 
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

url = "https://linkedin-data-api.p.rapidapi.com/search-jobs?keywords=software Engineer&locationId=102787409&datePosted=past24Hours&sort=mostRelevant"
headers = {
    'x-rapidapi-host': 'linkedin-data-api.p.rapidapi.com',
    'x-rapidapi-key': '360f784b90mshe5340e318c9a403p1bd3b6jsncea002ddc2ce'
}

def fetch_linkedin_jobs():
    response = requests.get(url, headers=headers)
    data = response.json()
    if data['success']:
        return data['data']
    return []




def filter_jobs(ti):
    jobs = ti.xcom_pull(task_ids='fetch_jobs')
    filtered_jobs = [job for job in jobs if 'tangier' in job['location']]
    ti.xcom_push(key='filtered_jobs', value=filtered_jobs)



def write_to_csv(ti):
    filtered_jobs = ti.xcom_pull(key='filtered_jobs', task_ids='filter_jobs')
    if not filtered_jobs:
        print("No jobs found in Tanger.")
        return
    
    csv_file = '/opt/airflow/csv folder/tanger_jobs.csv'
    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['id', 'title', 'company_name', 'company_url', 'location', 'job_url', 'type', 'postAt'])
        writer.writeheader()

        for job in filtered_jobs:
            writer.writerow({
                'id': job['id'],
                'title': job['title'],
                'company_name': job['company']['name'],
                'company_url': job['company']['url'],
                'location': job['location'],
                'job_url': job['url'],
                'type': job['type'],
                'postAt': job['postAt'],
            })

    print(f"Filtered jobs written to {csv_file}")



with DAG('fetch_latest_linkedin_jobs',
        default_args=default_args,
        schedule_interval = '1 * * * *',
        catchup=False) as dag:


    start_DAG = EmptyOperator (
        task_id = 'start_DAG'
    )

    fetch_jobs = PythonOperator(
        task_id='fetch_jobs',
        python_callable=fetch_linkedin_jobs
    )

    start_filter_jobs = PythonOperator(
        task_id='filter_jobs',
        python_callable=filter_jobs
    )

    write_csv = PythonOperator(
        task_id='write_csv',
        python_callable=write_to_csv
    )

    start_DAG >> fetch_jobs >> start_filter_jobs >> write_csv