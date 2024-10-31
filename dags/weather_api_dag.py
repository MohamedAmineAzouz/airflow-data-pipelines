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





def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius




def transform_load_data( ti ):
    data = ti.xcom_pull(task_ids="group_a.task_extract_tanger_weather_data")
    
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"city": city,
                        "description": weather_description,
                        "temperature_celsius": temp_celsius,
                        "feels_like_celsius": feels_like_celsius,
                        "minimun_temp_celsius":min_temp_celsius,
                        "maximum_temp_celsius": max_temp_celsius,
                        "pressure": pressure,
                        "humidity": humidity,
                        "wind_speed": wind_speed,
                        "time_of_record": time_of_record,
                        "sunrise_local_time)":sunrise_time,
                        "sunset_local_time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    df_data.to_csv("/opt/airflow/csv folder/current_weather_data.csv", index=False, header=False)


def load_weather():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='current_weather_data.csv'
    )





def save_joined_data_to_csv(ti):
    data = ti.xcom_pull(task_ids="task_join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature_celsius', 'feels_like_celsius', 'minimun_temp_celsius', 'maximum_temp_celsius', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'state', 'census_2020', 'land_area_sq_mile_2020'])
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = 'joined_weather_data_' + dt_string
    df.to_csv(f"/opt/airflow/csv folder/{file_name}.csv", index=False)





default_args = {
    'owner': 'airflow', 
    'depends_on_past': False, 
    'start_date': datetime(2024, 10, 14), 
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}



with DAG('weather_api_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:


    start_pipeline = EmptyOperator (
        task_id = 'start_pipeline'
    )

    join_data = PostgresOperator(
        task_id='join_data',
        postgres_conn_id = "postgres_conn",
        sql= '''SELECT 
            w.city,                    
            description,
            temperature_celsius,
            feels_like_celsius,
            minimun_temp_celsius,
            maximum_temp_celsius,
            pressure,
            humidity,
            wind_speed,
            time_of_record,
            sunrise_local_time,
            sunset_local_time,
            state,
            census_2024,
            land_area_sq_mile_2024                  
            FROM weather_data w
            INNER JOIN city_look_up c
            ON w.city = c.city ;
        '''
    )

    load_joined_data = PythonOperator(
        task_id= 'task_load_joined_data',
        python_callable=save_joined_data_to_csv
    )

    end_pipeline = EmptyOperator(
        task_id = 'task_end_pipeline'
    )
    


    with TaskGroup( group_id='group_a' , tooltip='Extract_from_postgres_and_weatherapi') as group_A:
        create_table_city_look_up = PostgresOperator(
            task_id = 'create_table_city_look',
            postgres_conn_id='postgres_conn',
            sql = """
                CREATE TABLE IF NOT EXISTS city_look_up(
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    census_2024 numeric NOT NULL,
                    land_Area_sq_mile_2024 numeric NOT NULL
                );
            """
        )

        truncate_table_city_look_up = PostgresOperator(
            task_id = "truncate_table_city_look_up" ,
            postgres_conn_id = 'postgres_conn',
            sql = "TRUNCATE TABLE city_look_up ;"
        )

        import_csv_to_postgres = PostgresOperator(
            task_id = 'import_csv_to_postgres',
            postgres_conn_id= 'postgres_conn',
            sql = """
                COPY city_look_up(city, state, census, land_Area_sq_mile_2024)
                FROM '/opt/dags/csv folder/ma_city.csv'
                DELIMITER ',' CSV HEADER;
            """
        )

        create_table_weather_data = PostgresOperator(
            task_id='task_create_table_2',
            postgres_conn_id = "postgres_conn",
            sql= ''' 
                CREATE TABLE IF NOT EXISTS weather_data (
                city TEXT,
                description TEXT,
                temperature_celsius NUMERIC,
                feels_like_celsius NUMERIC,
                minimun_temp_celsius NUMERIC,
                maximum_temp_celsius NUMERIC,
                pressure NUMERIC,
                humidity NUMERIC,
                wind_speed NUMERIC,
                time_of_record TIMESTAMP,
                sunrise_local_time TIMESTAMP,
                sunset_local_time TIMESTAMP                    
            );
            '''
        )

        is_tanger_weather_api_ready = HttpSensor(
            task_id ='task_is_tanger_weather_api_ready',
            http_conn_id='weathermap_api',
            endpoint='/data/2.5/weather?q=tanger&APPID=d3538466106f7852d7f2f5e40eb9e5ab'
        )

        extract_tanger_weather_data = SimpleHttpOperator(
            task_id = 'task_extract_tanger_weather_data',
            http_conn_id = 'weathermap_api',
            endpoint='/data/2.5/weather?q=tanger&APPID=d3538466106f7852d7f2f5e40eb9e5ab',
            method = 'GET',
            response_filter= lambda r: json.loads(r.text),
            log_response=True
        )

        transform_load_tanger_weather_data = PythonOperator(
            task_id= 'transform_load_tanger_weather_data',
            python_callable=transform_load_data
        )

        load_weather_data = PythonOperator(
        task_id= 'task_load_weather_data',
        python_callable=load_weather
        )


        create_table_city_look_up >> truncate_table_city_look_up >> import_csv_to_postgres
        create_table_weather_data >> is_tanger_weather_api_ready >> extract_tanger_weather_data >> transform_load_tanger_weather_data >> load_weather_data
    start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline
