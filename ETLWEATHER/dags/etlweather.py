from datetime import datetime, timezone
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook #helps to connect to the API
from airflow.providers.postgres.hooks.postgres import PostgresHook #helps to connect to the database
from airflow.decorators import task # creates a task in airflow DAG
import requests
import json

# Latitude and longitude for the location (London)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'  # Connection ID for PostgreSQL
API_CONN_ID = 'open_meteo_api'  # Connection ID for OpenWeatherMap API


defualt_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=timezone.utc)
}

#DAG

with DAG(dag_id='weather_etl_pipline', 
         default_args=defualt_args,
         schedule='@daily',  # Run the DAG daily
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

        ## Make the request to the Open_Meteo API
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            # If the request is successful, return the JSON response
            return response.json()
        else:
            # If the request fails, raise an error
            raise ValueError(f"Error fetching weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """
        Transforms the extracted weather data into a format suitable for loading into the database.
        """
        # Extract relevant fields from the weather data
        current_weather = weather_data.get('current_weather')
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """
        Loads the transformed weather data into the PostgreSQL database.
        """
        # Using PostgresHook to get connection details from Airflow connection
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()


        #create the weather table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cursor.execute(create_table_query)

        #Insert the transformed data into the weather table
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))
        conn.commit()
        cursor.close()

        conn.close()

    ## DAG Workflow - ETL pipline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)