import pandas as pd
import pymysql
from pymysql import Error
import json
import requests


def create_connection():
    host = 'localhost'
    user = 'username'
    password = 'password'
    database = 'database'
    
    
    try:
        connection = pymysql.connect(
            host=host,
            user = user,
            password = password,
            database=database
        )
        return connection
    except Error as e:
        return None
    
def data(zip_code):
    try:
        api_key = "0354c29c5e773c46d37727c8a0455d58"
        url = f"http://api.openweathermap.org/data/2.5/weather?zip={zip_code},us&appid={api_key}"
        response = requests.get(url)
        data = json.loads(response.text)

        # forecast_id = data["weather"]["id"]
        # temperature = data["main"]["temp"]
        # humidity = data["main"]["humidity"]
        # min_temp = data["main"]["temp_min"]
        # max_temp = data["main"]["temp_max"]
        # forecast_ts = data["dt"]
        # groupname = "FC Barcelona"
        # sunrise = data["sys"]["sunrise"]
        # sunset = data["sys"]["sunset"]
        # zipcode = zip

        weather_data = {
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "min_temp": data["main"]["temp_min"],
        "max_temp": data["main"]["temp_max"],
        "forecast_ts": datetime.fromtimestamp(data["dt"]),
        "groupname": "FC Barcelona",
        "sunrise": datetime.fromtimestamp(data["sys"]["sunrise"]),
        "sunset": datetime.fromtimestamp(data["sys"]["sunset"]),
        "zipcode": zip_code
        }
        return weather_data
    except Exception as e:
        print(f"Error {e} has occured while retrieving weather data")
        

    
def sql_insert(weather_data, connection):
    try:
        # if connection:
        #     df = pd.DataFram([weather])
        #     cursor = self.db.cursor()
        #     sqlStr = f"INSERT INTO cse191.forecast"
        df = pd.DataFrame([weather_data])
        print("success1")
        df.to_sql("forecast", con=connection, if_exists="append", index=False)
        print("success2")
    except Exception as e:
        print(f"Error {e} occurred while inserting into database.")