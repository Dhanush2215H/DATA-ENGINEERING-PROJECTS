from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook #helps to connect to the API
from airflow.providers.postgres.hooks.postgres import PostgresHook #helps to connect to the database
from airflow.decorators import task # creates a task in airflow DAG
from airflow.utils.dates import days_ago #helps to set the start date of the DAG


# Latitude and longitude for the location (London)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'  # Connection ID for PostgreSQL
API_CONN_ID = 'open_meteo_api'  # Connection ID for OpenWeatherMap API


defualt_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)  # Start date of the DAG
}

#DAG

with DAG(dag_id='weather_etl_pipline', 
         default_args=defualt_args,
         schedule_interval='@daily',  # Run the DAG daily
         catchup=False) as dag:


    @task()
    def extract_weather_data():
        """
        Extracts weather data from the Open_Meteo using Airfloe connection.
        """

        # using HttpHook to get connection detaisls from Airflow connection
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')


        # Build the API endpoint URL with the specified latitude and longitude
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'