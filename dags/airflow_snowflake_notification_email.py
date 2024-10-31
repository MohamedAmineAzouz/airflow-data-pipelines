from airflow import DAG
from datetime import timedelta, datetime
import os
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.email import EmailOperator




default_args = {
    'owner': 'airflow', 
    'depends_on_past': False, 
    'start_date': datetime(2024, 10, 15), 
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def log_file_availability(**context):
    file_path = '/opt/airflow/csv folder/ma_city.csv'
    if not os.path.exists('/opt/airflow/csv folder/ma_city.csv'):
        raise FileNotFoundError(f"File not found at {file_path}")
    else:
        print("File is available.")







with DAG('snowflake_with_email_notification',
    default_args=default_args,
    schedule_interval = '@daily',
    catchup=False) as dag:


    is_file_csv_available = PythonOperator(
        task_id='log_file_availability',
        python_callable=log_file_availability
    )


    create_table = SnowflakeOperator(
        task_id = "create_snowflake_table",
        snowflake_conn_id = 'conn_id_snowflake',           
        sql = '''
                CREATE TABLE IF NOT EXISTS city_info(
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    census_2024 numeric NOT NULL,
                    land_Area_sq_mile_2024 numeric NOT NULL	
                )
            '''
    )

    upload_csv_to_stage = SnowflakeOperator(
        task_id="upload_csv_to_stage",
        snowflake_conn_id='conn_id_snowflake',
        sql='''
            PUT 'file:///opt/airflow/csv folder/ma_city.csv' @~/csv_folder AUTO_COMPRESS=TRUE;
        '''
    )


    copy_csv_to_table = SnowflakeOperator(
        task_id="copy_csv_to_table",
        snowflake_conn_id='conn_id_snowflake',
        sql='''
            COPY INTO city_info
            FROM @~/csv_folder/ma_city.csv.gz
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
        '''
    )


    notification_by_email = EmailOperator(    # go to /config/airflow.cfg and configure smtp server line 413
        task_id="task_notification_by_email",
        to="your_emai@gmail.com",   
        subject="Snowflake ETL Pipeline",
        html_content="This is just a test."
    )

    is_file_csv_available >>  create_table >>  upload_csv_to_stage  >> copy_csv_to_table >> notification_by_email