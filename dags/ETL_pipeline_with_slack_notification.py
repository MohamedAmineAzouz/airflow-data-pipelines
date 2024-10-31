from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests
import pandas as pd


api_key = "d3538466106f7852d7f2f5e40eb9e5ab"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 17),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}





def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius





def etl_weather_data():
    combined_df = pd.DataFrame()
    names_of_city = ["tanger", "rabat", "casablanca", "tata", "marrakech", "tetouan", "kenitra"]
    base_url = "https://api.openweathermap.org"    
    for city in names_of_city:
        end_point = "/data/2.5/weather?q=" + city + "&APPID=" + api_key
        full_url = base_url + end_point
        r = requests.get(full_url)
        data = r.json()      
       
        print(data)
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
        combined_df = pd.concat([combined_df, df_data], ignore_index=True)        

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = 'current_weather_data_' + dt_string
    combined_df.to_csv(f"/opt/airflow/csv folder/{file_name}.csv", index=False)







with DAG('weather_dag_with_slack_notification',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:
        

        extract_transform_weather_data = PythonOperator(
            task_id= 'tsk_extract_transform_weather_data',
            python_callable=etl_weather_data
        )


        # You need to go to https://app.slack.com and create an account. Then go to https://api.slack.com/apps and create a new app => From scratch, choose your workspace.
        # Then go to Incoming Webhooks, activate it, and click to add a new webhook to the workspace. Choose your channel and copy the webhook URL.
        # Finally, go to the Airflow UI connections, create a new connection of type "HTTP", and paste the URL into the password field, then click save.
        slack_notification = SlackWebhookOperator(
            task_id = "task_slack_notification",
            slack_webhook_conn_id= "slack_conn_id",
            message = "This is just a test. Please do nothing.",
            channel = "#weatherapi"
        )


        extract_transform_weather_data >> slack_notification