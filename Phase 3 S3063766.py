# Author: Lena Moroz
# Student ID: S3063766

#pip install openmeteo-requests
#pip install requests-cache retry-requests numpy pandas

import sqlite3
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
plt.ion()
from datetime import timedelta, datetime
import numpy as np
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox, simpledialog
import pandas as pd
import requests
import openmeteo_requests
import requests_cache
from retry_requests import retry

class WeatherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Data App")

        # Database connection
        self.connection = sqlite3.connect('CIS4044-N-SDI-OPENMETEO-PARTIAL.db')

        # Buttons
        self.query_button = ttk.Button(root, text="Perform Your Database Query", command=self.perform_database_query)
        self.query_button.pack(pady=10)

        self.select_countries_button = ttk.Button(root, text="Show All Countries", command=self.select_all_countries)
        self.select_countries_button.pack(pady=10)

        self.select_all_cities_button = ttk.Button(root, text="Show All Cities", command=self.select_all_cities)
        self.select_all_cities_button.pack(pady=10)

        self.select_countries_button = ttk.Button(root, text="Select Weather Data", command=self.select_weather_data_wrapper)
        self.select_countries_button.pack(pady=10)

        self.average_annual_temp_button = ttk.Button(root, text="Average Annual Temperature", command=self.average_annual_temp_wrapper)
        self.average_annual_temp_button.pack(pady=10)

        self.average_seven_day_precip_button = ttk.Button(root, text="Average 7-Day Precipitation", command=self.average_seven_day_precip_wrapper)
        self.average_seven_day_precip_button.pack(pady=10)

        self.average_mean_temp_by_city_button = ttk.Button(root, text="Average Mean Temperature by City", command=self.average_mean_temp_by_city_wrapper)
        self.average_mean_temp_by_city_button.pack(pady=10)

        self.average_annual_precip_by_country_button = ttk.Button(root, text="Average Annual Precipitation by Country", command=self.average_annual_precip_by_country_wrapper)
        self.average_annual_precip_by_country_button.pack(pady=10)

        self.seven_day_precipitation_button = ttk.Button(root, text="Bar Chart for 7-day Precipitation by City", command=self.chart_seven_day_precipitation_by_city_wrapper)
        self.seven_day_precipitation_button.pack(pady=10)

        self.select_weather_data_button = ttk.Button(root, text="Bar Chart for Specified Period", command=self.chart_select_weather_data_wrapper)
        self.select_weather_data_button.pack(pady=10)

        self.average_annual_precipitation_button = ttk.Button(root, text="Bar Chart of Average Annual Precipitation by Country", command=self.chart_average_annual_precipitation_wrapper)
        self.average_annual_precipitation_button.pack(pady=10)

        self.fetch_weather_data_button = ttk.Button(root, text="Grouped Bar Charts of the min/max/mean temperature",command=self.chart_fetch_weather_data_wrapper)
        self.fetch_weather_data_button.pack(pady=10)

        self.month_minmax_by_city_button = ttk.Button(root, text="Multi-line Chart for Daily Min/Max Temp by City",command=self.chart_month_minmax_by_city_wrapper)
        self.month_minmax_by_city_button.pack(pady=10)

        self.select_weather_data_city_button = ttk.Button(root, text="Scatter Plot for Mean Temp against Rainfall for City",command=self.chart_select_weather_data_city_wrapper)
        self.select_weather_data_city_button.pack(pady=10)

        self.add_store_weather_data_button = ttk.Button(root, text="Fetch and Store New Data",command=self.add_store_weather_data)
        self.add_store_weather_data_button.pack(pady=10)
        
        self.delete_data_button = ttk.Button(root, text="Delete All Data", command=self.delete_data)
        self.delete_data_button.pack(pady=10)

        self.quit_button = ttk.Button(root, text="Quit", command=root.destroy)
        self.quit_button.pack(pady=10)

    def perform_database_query(self):
        try:
            user_query = simpledialog.askstring("Database Query", "Enter your SQL query:")

            if user_query:
                cursor = self.connection.cursor()

                cursor.execute(user_query)
            
                results = cursor.fetchall()

                if not results:
                    print("No results found.")

                else:
                    column_names = [description[0] for description in cursor.description]
                    print("\t".join(column_names))

                    for row in results:
                        print("\t".join(map(str, row)))

                cursor.close()

                messagebox.showinfo("Database Query", "Query executed successfully. Check console for results.")

        except Exception as e:
            print(f"Error performing database query: {e}")
            messagebox.showerror("Error", f"Error performing database query. Check console for details.\n{e}")
            
    def select_all_countries(self):
        try:
            self.connection.row_factory = sqlite3.Row
            query = "SELECT * FROM [countries]"
            cursor = self.connection.cursor()
            results = cursor.execute(query).fetchall()

            for row in results:
                print( f"ID: {row['id']}, name: {row['name']}, timezone: {row['timezone']}" )

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)


    def select_weather_data(self, date_from, date_to):
        try:
            self.connection.row_factory = sqlite3.Row
            query = f"SELECT * from [daily_weather_entries] where date >= '{date_from}' and date <= '{date_to}'"

            cursor = self.connection.cursor()
            results = cursor.execute(query)

            count = 0
            for row in results:
                count += 1
                print( f"ID: {row['id']}, date: {row['date']}, min temperature(°C): {row['min_temp']}, max temperature(°C): {row['max_temp']}, mean temperature(°C): {row['mean_temp']}, precipitation(mm): {row['precipitation']}" )
        
            print(f"Found {count} entries")

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)
        
    def select_weather_data_wrapper(self):

        date_from = simpledialog.askstring("Select Weather Data", "Enter start date (YYYY-MM-DD):")
        date_to = simpledialog.askstring("Select Weather Data", "Enter end date (YYYY-MM-DD):")

        if date_from and date_to:
            self.select_weather_data(date_from, date_to)

    def select_all_cities(self):
        try:
            self.connection.row_factory = sqlite3.Row
            query = "SELECT * FROM [cities]"
            cursor = self.connection.cursor()
            results = cursor.execute(query)

            for row in results:
                print(f"ID: {row['id']}, name: {row['name']}, longitude: {row['longitude']}, latitude: {row['latitude']}, country ID: {row['country_id']}")

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)
            
    def average_annual_temperature(self, city_id, year):
        try:
            self.connection.row_factory = sqlite3.Row
            query = f"SELECT avg([mean_temp]) FROM [daily_weather_entries] where city_id = {city_id} and date >= '{year}-01-01' and date <= '{year}-12-31'"
            cursor = self.connection.cursor()
            results = cursor.execute(query)

            count = 0
            for row in results:
                count += 1
                avrg_temp = round(row[0], 2)
                print(f"Average annual temperature(°C) for city {city_id} in {year}: {avrg_temp}")

            print(f"Found {count} entries")
            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)

    def average_annual_temp_wrapper(self):
        city_id = simpledialog.askinteger("Average Annual Temperature", "Enter city ID:")
        year = simpledialog.askinteger("Average Annual Temperature", "Enter year:")

        if city_id and year:
            self.average_annual_temperature(city_id, year)

    def average_seven_day_precipitation(self, city_id, start_date):
        try:
            self.connection.row_factory = sqlite3.Row
            end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=7)
            end_date = end_date_obj.strftime("%Y-%m-%d")
            query = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
            cursor = self.connection.cursor()
            results = cursor.execute(query)

            count = 0
            for row in results:
                count += 1
                avrg_precipitation = round(row[0], 2)
                print(f"Average seven-day precipitation (mm) for city {city_id} starting from {start_date}: {avrg_precipitation}")

            print(f"Found {count} entries")
            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)

    def average_seven_day_precip_wrapper(self):
        city_id = simpledialog.askinteger("Average 7-Day Precipitation", "Enter city ID:")
        start_date = simpledialog.askstring("Average 7-Day Precipitation", "Enter start date (YYYY-MM-DD):")

        if city_id and start_date:
            self.average_seven_day_precipitation(city_id, start_date)

    def average_mean_temp_by_city(self, date_from, date_to):
        try:
            self.connection.row_factory = sqlite3.Row
            query = f"SELECT avg([mean_temp]) FROM [daily_weather_entries] where date >= '{date_from}' and date <= '{date_to}' GROUP BY city_id"
            cursor = self.connection.cursor()
            results = cursor.execute(query)

            count = 0
            for row in results:
                count += 1
                avg_mean_temp = round(row[0], 2)
                print(f"Average mean temperature by city(°C): {avg_mean_temp}")

            print(f"Found {count} entries")
            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)

    def average_mean_temp_by_city_wrapper(self):
        date_from = simpledialog.askstring("Average Mean Temperature by City", "Enter start date (YYYY-MM-DD):")
        date_to = simpledialog.askstring("Average Mean Temperature by City", "Enter end date (YYYY-MM-DD):")

        if date_from and date_to:
            self.average_mean_temp_by_city(date_from, date_to)

    def average_annual_precipitation_by_country(self, year):
        try:
            self.connection.row_factory = sqlite3.Row
            query_countries = f"SELECT id FROM [countries]"
            cursor_countries = self.connection.cursor()
            results_countries = cursor_countries.execute(query_countries)

            for row_countries in results_countries:
                country_id = row_countries[0]

                query_cities = f"SELECT id FROM [cities] where country_id = {country_id}"
                cursor_cities = self.connection.cursor()
                results_cities = cursor_cities.execute(query_cities)

                city_list = []
                for row_cities in results_cities:
                    city_list.append(f"city_id= {row_cities[0]}")

                cities_condition = ' or '.join(city_list)
                query_weather = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where {cities_condition} and date >= '{year}-01-01' and date <= '{year}-12-31'"

                cursor_weather = self.connection.cursor()
                results_weather = cursor_weather.execute(query_weather)

                for row_weather in results_weather:
                    annual_precipitation = round(row_weather[0], 2)
                    print(f"Country: {country_id}, average annual precipitation (mm): {annual_precipitation}")

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            print(ex)

    def average_annual_precip_by_country_wrapper(self):
        year = simpledialog.askstring("Input", "Enter the year:")
        if year:
            try:
                year = int(year)
                self.average_annual_precipitation_by_country(year)
            except ValueError:
                messagebox.showerror("Error", "Invalid year. Please enter a valid integer.")

    def chart_seven_day_precipitation_by_city(self, city_id, start_date):
        try:
            self.connection.row_factory = sqlite3.Row
            dict1 = []

            # Define the query
            end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)
            end_date = end_date_obj.strftime("%Y-%m-%d")
            query = f"SELECT [city_id], [date], [precipitation] FROM [daily_weather_entries] where city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
            cursor = self.connection.cursor()
            results = cursor.execute(query)
            results = cursor.fetchall()

            count = 0
            for row in results:
                count += 1
                dict1.append({'city_id': row['city_id'], 'date': row['date'], 'precipitation': row['precipitation']})

            print(f"Found {count} entries")

            plt.bar([entry['date'] for entry in dict1], [entry['precipitation'] for entry in dict1], color='yellow')

            plt.xlabel('Date')
            plt.ylabel('Precipitation(mm)')
            plt.title(f'7-day Precipitation of City №{city_id}')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.grid(True, linestyle='--')
            plt.savefig(f"Precipitation_for_city_{city_id}_from_{start_date}.png")
            plt.show()
            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_seven_day_precipitation_by_city_wrapper(self):
        city_id = simpledialog.askinteger("7-day Precipitation by City", "Enter City ID:")
        start_date = simpledialog.askstring("7-day Precipitation by City", "Enter start date (YYYY-MM-DD):")

        if city_id is not None and start_date:
            self.chart_seven_day_precipitation_by_city(city_id, start_date)

    def chart_select_weather_data(self, city_id, date_from, date_to):
        try:
            self.connection.row_factory = sqlite3.Row
            dict2 = []

            query = f"SELECT [min_temp],[max_temp],[mean_temp],[date], [city_id] from [daily_weather_entries] where city_id = '{city_id}' and date >= '{date_from}' and date <= '{date_to}'"
            cursor = self.connection.cursor()

            results = cursor.execute(query)

            count = 0
            for row in results:
                count += 1
                dict2.append({'city_id': row['city_id'], 'date': row['date'], 'mean_temp': row['mean_temp']})

            print(f"Found {count} entries")

            plt.figure(figsize=(10, 6))
            plt.bar([entry['date'] for entry in dict2], [entry['mean_temp'] for entry in dict2], color='purple', alpha=0.7)

            plt.xlabel('Date')
            plt.ylabel('Mean Temperature(°C)')
            plt.title(f'Mean Temperature of City №{city_id} from {date_from} to {date_to}')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.grid(True, linestyle='--')

            average_mean_temp = sum(entry['mean_temp'] for entry in dict2) / len(dict2)
            plt.axhline(y=average_mean_temp, color='red', linestyle='--', label='Average Mean Temperature')
            plt.legend()
            plt.savefig(f"Mean_temp_for_city_{city_id}_from_{date_from}_to_{date_to}.png")
            plt.show()

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_select_weather_data_wrapper(self):
        city_id = simpledialog.askinteger("Select Weather Data", "Enter City ID:")
        date_from = simpledialog.askstring("Select Weather Data", "Enter start date (YYYY-MM-DD):")
        date_to = simpledialog.askstring("Select Weather Data", "Enter end date (YYYY-MM-DD):")

        if city_id is not None and date_from and date_to:
            self.chart_select_weather_data(city_id, date_from, date_to)

    def chart_average_annual_precipitation_by_country(self, year):
        try:
            self.connection.row_factory = sqlite3.Row
            dict3 = []

            query1 = f"SELECT id FROM [countries]"

            cursor = self.connection.cursor()

            results = cursor.execute(query1)
            results = cursor.fetchall()

            for row in results:
                country_id = row[0]
                query2 = f"SELECT id FROM [cities] where country_id = {country_id}"

                cursor_cities = self.connection.cursor()

                results_cities = cursor_cities.execute(query2)
                results_cities = cursor_cities.fetchall()

                cities = []
                for row_ in results_cities:
                    cities.append(row_[0])

                cities_condition = ' OR '.join(f"city_id = {city}" for city in cities)

                query3 = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where {cities_condition} and date >= '{year}-01-01' and date <= '{year}-12-31'"

                cursor_countries = self.connection.cursor()

                results_countries = cursor_countries.execute(query3)
                results_countries = cursor_countries.fetchall()

                for _row_ in results_countries:
                    dict3.append({'country_id': country_id, 'average_annual_precipitation': _row_[0]})
                    print(f"Country: {country_id}, average annual precipitation: {_row_[0]}")

            plt.figure(figsize=(5, 5))
            countries = ['{}'.format(entry['country_id']) for entry in dict3]
            average_annual_precipitation = [entry['average_annual_precipitation'] for entry in dict3]

            plt.bar(countries, average_annual_precipitation, color='green', alpha=0.7, width=0.3)

            plt.xlabel('Country')
            plt.ylabel('Average Precipitation (mm)')
            plt.title(f'Average Yearly Precipitation by Country for {year}')
            plt.tight_layout()
            plt.grid(True, linestyle='--')

            plt.savefig(f"Bar_chart_average_yearly_precipitation_for_country{country_id}.png")

            plt.show()

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_average_annual_precipitation_wrapper(self):
        year = simpledialog.askinteger("Average Annual Precipitation by Country", "Enter year:")

        if year:
            self.chart_average_annual_precipitation_by_country(year)

    def chart_fetch_weather_data(self, city_1, city_2, city_3, city_4, date):
        try:
            self.connection.row_factory = sqlite3.Row
            dict4 = []

            query = f"SELECT city_id, date, min_temp, max_temp, mean_temp FROM [daily_weather_entries] WHERE city_id in " \
                    f"({city_1},{city_2},{city_3},{city_4}) and date = '{date}'"
            cursor = self.connection.cursor()

            cursor.execute(query)
            results = cursor.fetchall()

            count = 0
            for row in results:
                count += 1
                # TODO: Make in one request
                id = row['city_id']
                query = f"SELECT name from cities WHERE id = {id}"
                cursor.execute(query)
                result_city_name = cursor.fetchall()

                dict4.append({
                    'city_id': str(result_city_name[0][0]),
                    'date': row['date'],
                    'min_temp': row['min_temp'],
                    'max_temp': row['max_temp'],
                    'mean_temp': row['mean_temp']})

            print(dict4)

            bar_width = 0.2
            index = np.arange(len(dict4))

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.set(title="Temperature Data for Different Cities", xlabel="City ID", ylabel="Temperature(°C)")

            ax.bar(index - bar_width, [entry['min_temp'] for entry in dict4], bar_width, label='Min Temperature')
            ax.bar(index + bar_width, [entry['mean_temp'] for entry in dict4], bar_width, label='Mean Temperature')
            ax.bar(index, [entry['max_temp'] for entry in dict4], bar_width, label='Max Temperature')

            ax.set_xticks(index)
            ax.grid(linestyle='--')
            ax.set_xticklabels([entry['city_id'] for entry in dict4])
            ax.legend()

            fig.savefig(f"Grouped_bar_charts.png")
            plt.show()

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_fetch_weather_data_wrapper(self):
        city_1 = simpledialog.askinteger("Fetch Weather Data", "Enter City 1 ID:")
        city_2 = simpledialog.askinteger("Fetch Weather Data", "Enter City 2 ID:")
        city_3 = simpledialog.askinteger("Fetch Weather Data", "Enter City 3 ID:")
        city_4 = simpledialog.askinteger("Fetch Weather Data", "Enter City 4 ID:")
        date = simpledialog.askstring("Fetch Weather Data", "Enter Date (YYYY-MM-DD):")

        if city_1 and city_2 and city_3 and city_4 and date:
            self.chart_fetch_weather_data(city_1, city_2, city_3, city_4, date)        

    def chart_month_minmax_by_city(self, city_id, start_date):
        try:
            self.connection.row_factory = sqlite3.Row
            dict5 = []

            # Define the query
            end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=30)
            end_date = end_date_obj.strftime("%Y-%m-%d")
            query = f"SELECT city_id, date, min_temp, max_temp FROM [daily_weather_entries] " \
                    f"WHERE city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
            cursor = self.connection.cursor()
            results = cursor.execute(query)
            results = cursor.fetchall()

            count = 0
            for row in results:
                count += 1
                dict5.append({'city_id': row['city_id'], 'date': row['date'],
                              'min_temp': row['min_temp'], 'max_temp': row['max_temp']})

            print(f"Found {count} entries")

            fig, ax = plt.subplots()
            fig.set_figwidth(12)

            ax.plot([entry["date"] for entry in dict5], [entry["min_temp"] for entry in dict5], label='Min Temperature',
                    color='b')
            ax.plot([entry["date"] for entry in dict5], [entry["max_temp"] for entry in dict5], label='Max Temperature',
                    color='r')

            ax.set_xlabel('Date')
            ax.set_ylabel('Temperature (°C)')
            ax.set_title(f'Daily Min/Max Temperature for City {city_id} from {start_date}')

            ax.legend()
            ax.tick_params(axis='x', labelrotation=90, labelsize=8)
            ax.grid()
            fig.tight_layout()
            fig.savefig(f"Multi-line_chart_of_city_{city_id}_min_max_temperature.png")

            plt.show()

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_month_minmax_by_city_wrapper(self):
        city_id = simpledialog.askinteger("Month Min/Max by City", "Enter City ID:")
        start_date = simpledialog.askstring("Month Min/Max by City", "Enter Start Date (YYYY-MM-DD):")

        if city_id and start_date:
            self.chart_month_minmax_by_city(city_id, start_date)

    def chart_select_weather_data_city(self, city_id):
        try:
            self.connection.row_factory = sqlite3.Row

            dict6 = []

            query = f"SELECT * FROM [daily_weather_entries] WHERE city_id = {city_id}"
            cursor = self.connection.cursor()

            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                dict6.append({'city_id': row['city_id'], 'date': row['date'],
                              'mean_temp': row['mean_temp'], 'precipitation': row['precipitation']})

            plt.figure()
            plt.scatter([entry["mean_temp"] for entry in dict6], [entry["precipitation"] for entry in dict6],
                        label=f'City {city_id}')
            plt.xlabel('Mean Temperature(°C)')
            plt.ylabel('Rainfall(mm)')
            plt.title(f'Scatter Plot for City {city_id}')
            plt.legend()

            plt.savefig(f"Scatter_plot_for_city_{city_id}.png")

            plt.show()

            self.connection.commit()

        except sqlite3.OperationalError as ex:
            messagebox.showerror("Error", f"Database error: {str(ex)}")

    def chart_select_weather_data_city_wrapper(self):
        city_id = simpledialog.askinteger("Select Weather Data for City", "Enter City ID:")

        if city_id:
            self.chart_select_weather_data_city(city_id)

    def add_store_weather_data(self):
        try:
            cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)

            locations = [
                {"latitude": 52.52, "longitude": 13.4, "city": "Berlin"},
                {"latitude": 50.45, "longitude": 30.52, "city": "Kyiv"},
                {"latitude": 41.9, "longitude": 12.49, "city": "Rome"},
                {"latitude": 40.41, "longitude": 3.7, "city": "Madrid"}
            ]

            url = "https://archive-api.open-meteo.com/v1/archive"

            start_date = "2017-01-01"
            end_date = "2023-06-01"

            self.connection = sqlite3.connect("CIS4044-N-SDI-OPENMETEO-PARTIAL.db")
            cursor = self.connection.cursor()

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

                with self.connection:
                    if response.status_code == 200:
                        data = response.json()
                        # df = pd.DataFrame(data)
                        # df.to_json(location["city"] + "_csv_file_path.json", orient='records', lines=True)

                        date = data["daily"]["time"]
                        max_temp = data["daily"]["temperature_2m_max"]
                        min_temp = data["daily"]["temperature_2m_min"]
                        mean_temp = data["daily"]["temperature_2m_mean"]
                        prep = data["daily"]["precipitation_sum"]

                        for [d, t, m, e, p] in zip(date, max_temp, min_temp, mean_temp, prep):
                            df.append((location["city"], d, t, m, e, p))

            cursor.executemany("INSERT INTO new_daily_data ('city', 'date', 'max_temp','min_temp', 'mean_temp', 'precipitation') VALUES(?, ?, ?, ?, ?, ?)", df)
            self.connection.commit()
            cursor.close()

            messagebox.showinfo("Add Store Weather Data", "New Weather Data added successfully")

        except sqlite3.Error as e:
            messagebox.showerror("Error", f"Error deleting data: {str(e)}")
    
    def delete_data(self):
        try:
            cursor = self.connection.cursor()

            table_names = ['cities', 'countries', 'daily_weather_entries', 'sqlite_sequence']

            for table_name in table_names:
                delete_query = f"DELETE FROM {table_name}"

                cursor.execute(delete_query)

            self.connection.commit()

            messagebox.showinfo("Delete All Data", "Data Deleted Successfully")

        except sqlite3.Error as e:
            messagebox.showerror("Error", f"Error deleting data: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = WeatherApp(root)
    root.mainloop()
