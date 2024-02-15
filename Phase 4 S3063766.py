# Author: Lena Moroz
# Student ID: S3063766

#pip install openmeteo-requests
#pip install requests-cache retry-requests numpy pandas

import requests
import sqlite3
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class YourClassName:
    def __init__(connection):
        pass
    def add_store_weather_data(connection):
        try:
            cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
            retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
            openmeteo = openmeteo_requests.Client(session = retry_session)

            locations = [
                {"latitude": 52.52, "longitude": 13.4, "city": "Berlin"},
                {"latitude": 50.45, "longitude": 30.52, "city": "Kyiv"},
                {"latitude": 41.9, "longitude": 12.49, "city": "Rome"},
                {"latitude": 40.41, "longitude": 3.7, "city": "Madrid"}
            ]

            url = "https://archive-api.open-meteo.com/v1/archive"

            start_date = "2017-01-01"
            end_date = "2023-06-01"

            connection = sqlite3.connect("CIS4044-N-SDI-OPENMETEO-PARTIAL.db")
            cursor = connection.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS new_daily_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                date TEXT,
                max_temp REAL,
                min_temp REAL,
                mean_temp REAL,
                precipitation REAL)
            ''')

            df = []

            for location in locations:
                params = {
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                    "start_date": start_date,
                    "end_date": end_date,
                    "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum"],
                    "timezone": "GMT"
                }

                response = requests.get(url, params=params)

                if response.status_code == 200:
                    data = response.json()

                with connection:
                    if response.status_code == 200:
                        data = response.json()
                        #df = pd.DataFrame(data)

                        date = data["daily"]["time"]
                        max_temp = data["daily"]["temperature_2m_max"]
                        min_temp = data["daily"]["temperature_2m_min"]
                        mean_temp = data["daily"]["temperature_2m_mean"]
                        prep = data["daily"]["precipitation_sum"]


                        for [d, t, m, e, p] in zip(date, max_temp, min_temp, mean_temp, prep):
                            df.append((location["city"], d, t, m, e, p))

            cursor.executemany("INSERT INTO new_daily_data ('city', 'date', 'max_temp','min_temp', 'mean_temp', 'precipitation') VALUES(?, ?, ?, ?, ?, ?)", df)
            connection.commit()
            cursor.close()

        except Exception as e:
            print("An error occurred:", str(e))

def retry(session, retries, backoff_factor):
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


if __name__=="__main__":
    yourClass = YourClassName()
    yourClass.add_store_weather_data()
