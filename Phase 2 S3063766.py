# Author: Lena Moroz
# Student ID: S3063766

import sqlite3
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
plt.ion()
from datetime import timedelta, datetime
import numpy as np


# Bar chart to show the 7-day precipitation for a specific town/city

def seven_day_precipitation_by_city (connection, city_id, start_date):
    try:
        connection.row_factory = sqlite3.Row
        dict1 = []

        # Define the query
        end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)
        end_date = end_date_obj.strftime("%Y-%m-%d")
        query = f"SELECT [city_id], [date], [precipitation] FROM [daily_weather_entries] where city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
        print(query)

        cursor = connection.cursor()
        results = cursor.execute(query)
        results = cursor.fetchall()

        count = 0
        for row in results:
            count += 1
            dict1.append({'city_id': row['city_id'], 'date': row['date'],  'precipitation': row['precipitation']})

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
        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

# Bar chart for a specified period for a specified set of towns/cities
def select_weather_data(connection, city_id, date_from, date_to):
    try:
        connection.row_factory = sqlite3.Row
        dict2 = []
        
        query = f"SELECT [min_temp],[max_temp],[mean_temp],[date], [city_id] from [daily_weather_entries] where city_id = '{city_id}' and date >= '{date_from}' and date <= '{date_to}'"
        print(query)

        cursor = connection.cursor()

        results = cursor.execute(query)
        #results = cursor.fetchall()

        count = 0
        for row in results:
            count += 1
            dict2.append({'city_id': row['city_id'], 'date':row['date'], 'mean_temp':row['mean_temp']})
        
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

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

# Bar chart that shows the average yearly precipitation by country
def average_annual_precipitation_by_country(connection, year):
    try:
        connection.row_factory = sqlite3.Row
        dict3 = []

        query1 = f"SELECT id FROM [countries]"

        cursor = connection.cursor()

        results = cursor.execute(query1)
        results = cursor.fetchall()

        for row in results:
            country_id = row[0]
            query2 = f"SELECT id FROM [cities] where country_id = {country_id}"

            cursor_cities = connection.cursor()

            results_cities = cursor_cities.execute(query2)
            results_cities = cursor_cities.fetchall()

            cities = []
            for row_ in results_cities:
                cities.append(row_[0])

            cities_condition = ' OR '.join(f"city_id = {city}" for city in cities)

            query3 = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where {cities_condition} and date >= '{year}-01-01' and date <= '{year}-12-31'"

            cursor_countries = connection.cursor()

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

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

# Grouped bar charts for displaying the min/max/mean temperature and 
# precipitation values for selected cities or countries.
def fetch_weather_data (connection, city_1, city_2, city_3, city_4, date):
    try:
        connection.row_factory = sqlite3.Row
        dict4=[]
        
        query = f"SELECT city_id, date, min_temp, max_temp, mean_temp FROM [daily_weather_entries] WHERE city_id in ({city_1},{city_2},{city_3},{city_4}) and date = '{date}'"
        cursor = connection.cursor()

        cursor.execute(query)
        results = cursor.fetchall()

        count = 0
        for row in results:
            count += 1
            dict4.append({
                'city_id': row['city_id'],
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
        ax.grid(linestyle = '--')
        ax.set_xticklabels([entry['city_id'] for entry in dict4])
        ax.legend()

        fig.savefig(f"Grouped_bar_charts.png")
        plt.show()
    
        connection.commit()
        
    except sqlite3.OperationalError as ex:
        print(ex)

# Multi-line chart to show the daily minimum and maximum temperature for a
# given month for a specific city.

def month_minmax_by_city (connection, city_id, start_date):
    try:
        connection.row_factory = sqlite3.Row
        dict5 = []

        # Define the query
        end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=30)
        end_date = end_date_obj.strftime("%Y-%m-%d")
        query = f"SELECT city_id, date, min_temp,max_temp FROM [daily_weather_entries] where city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
        print(query)

        cursor = connection.cursor()
        results = cursor.execute(query)
        results = cursor.fetchall()

        count = 0
        for row in results:
            count += 1
            dict5.append({'city_id': row['city_id'], 'date': row['date'],  'min_temp': row['min_temp'], 'max_temp': row['max_temp']})

        print(f"Found {count} entries")
        
        fig, ax = plt.subplots()
        fig.set_figwidth(12)

        ax.plot([entry["date"] for entry in dict5], [entry ["min_temp"] for entry in dict5], label='Min Temperature', color = 'b')
        ax.plot([entry["date"] for entry in dict5], [entry["max_temp"] for entry in dict5], label='Max Temperature', color = 'r')

        ax.set_xlabel('Date')
        ax.set_ylabel('Temperature (°C)')
        ax.set_title(f'Daily Minimum and Maximum Temperature for city {city_id} from {start_date}')

        ax.legend()
        ax.tick_params(axis = 'x', labelrotation = 90, labelsize =8)
        ax.grid()
        fig.tight_layout()
        fig.savefig (f"Multi-line_chart_of_city_№{city_id}_min_max_temperature.png")

        plt.show()

        connection.commit()
        

    except sqlite3.OperationalError as ex:
        print(ex)


# Scatter plot chart for mean temperature against rainfall

def select_weather_data_city(connection, city_id):
    try:
        connection.row_factory = sqlite3.Row

        dict6 = []

        query = f"SELECT * FROM [daily_weather_entries] WHERE city_id = {city_id}"
        cursor = connection.cursor()

        cursor.execute(query)

        results = cursor.fetchall()

        for row in results:
            
            dict6.append({'city_id': row['city_id'], 'date': row['date'],  'mean_temp': row['mean_temp'],'precipitation': row['precipitation']})

        plt.figure()
        plt.scatter([entry["mean_temp"] for entry in dict6], [entry ["precipitation"] for entry in dict6], label=f'City {city_id}')
        plt.xlabel('Mean Temperature(°C)')
        plt.ylabel('Rainfall(mm)')
        plt.title(f'Scatter Plot for City {city_id}')
        plt.legend()

        plt.savefig(f"Scatter_plot_for_city_{city_id}.png")

        plt.show()

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)
 
def main():

    with sqlite3.connect("CIS4044-N-SDI-OPENMETEO-PARTIAL.db") as connection:
        
        # Bar chart to show the 7-day precipitation for a specific town/city
        city_id = 3
        start_date = "2021-12-05"
        seven_day_precipitation_by_city (connection, city_id, start_date)

        # Bar chart for a specified period for a specified set of towns/cities
        date_from="2021-12-01"
        date_to= "2021-12-15"
        city_id = 1
        select_weather_data(connection, city_id, date_from, date_to)

        # Bar chart that shows the average yearly precipitation by country
        year = 2021
        average_annual_precipitation_by_country(connection, year)

        # Grouped bar charts for displaying the min/max/mean temperature andprecipitation values for selected cities or countries.  
        date = "2021-06-01"
        city_1 = 1
        city_2 = 2
        city_3 = 3
        city_4 = 4
        fetch_weather_data (connection, city_1, city_2, city_3, city_4, date)

        # Multi-line chart to show the daily minimum and maximum temperature for a given month for a specific city.
        start_date = "2020-12-01"
        city_id = 2
        month_minmax_by_city (connection, city_id, start_date)

        # Scatter plot chart for mean temperature against rainfall for a town/city
        city_id=1
        select_weather_data_city(connection, city_id)

if __name__ == "__main__":
    # Create a SQLite3 connection and call the various functions
    # above, printing the results to the terminal.
    main()
