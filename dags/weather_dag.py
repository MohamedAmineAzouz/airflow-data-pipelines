from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd



default_args = {
    'owner': 'airflow', # Indicates the owner of the DAG.
    'depends_on_past': False, #Specifies whether tasks will work based on their historical status.
    'start_date': datetime(2024, 10, 10), #Specifies the start date of the DAG.
    'email': ['myemail@domain.com'], # Specifies the e-mail address to which notification e-mails will be sent.
    'email_on_failure': False, # Specifies whether email notifications will be sent when Task fails.
    'email_on_retry': False, #Specifies whether e-mail notifications will be sent when the task is tried again.
    'retries': 2, #Indicates the number of retries when the task failed.
    'retry_delay': timedelta(minutes=5)  #Indicates the Deceleration time between retries.
}





def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius




def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celsius,
                        "Feels Like (C)": feels_like_celsius,
                        "Minimun Temp (C)":min_temp_celsius,
                        "Maximum Temp (C)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = 'current_weather_data_tanger_' + dt_string
    df_data.to_csv(f"/opt/airflow//csv folder/{file_name}.csv", index=False)











with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:


        #HttpSensor : Execute HTTP GET statement; return False on failure 404 Not Found or response_check returning False.
        is_weather_api_ready = HttpSensor(
            task_id ='is_weather_api_ready',
            http_conn_id='weathermap_api', #The http connection to run the sensor against => go to airflow interface > admin > connections create new connection with the same connection id , the connection type is HTTP and HOST is "https://home.openweathermap.org/"
            endpoint='/data/2.5/weather?q=Tanger&APPID=d3538466106f7852d7f2f5e40eb9e5ab' # The relative part of the full url
        )


        # SimpleHttpOperator : Calls an endpoint on an HTTP system to execute an action.
        extract_weather_data = SimpleHttpOperator(
            task_id = 'extract_weather_data',
            http_conn_id = 'weathermap_api',
            endpoint='/data/2.5/weather?q=Tanger&APPID=d3538466106f7852d7f2f5e40eb9e5ab',
            method = 'GET', #The HTTP method to use, default = “POST”
            response_filter= lambda r: json.loads(r.text), #A function allowing you to manipulate the response text. e.g response_filter=lambda response: json.loads(response.text). 
            log_response=True  #Log the response (default: False)
        )


        #Use the PythonOperator to execute Python callables.
        transform_load_weather_data = PythonOperator(
            task_id= 'transform_load_weather_data',
            python_callable=transform_load_data
        )


        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data