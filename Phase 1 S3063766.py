# Author: Lena Moroz
# Student ID: S3063766

import sqlite3
from datetime import timedelta, datetime

# Phase 1 - Starter
# 
# Note: Display all real/float numbers to 2 decimal places.

'''
Satisfactory
'''

def select_all_countries(connection):
    # Queries the database and selects all the countries
    # stored in the countries table of the database.
    # The returned results are then printed to the
    # console.
    try:
        connection.row_factory = sqlite3.Row

        # Define the query
        query = "SELECT * FROM [countries]"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        for row in results:
            print( f"{row['id']},  {row['name']},  {row['timezone']}" )

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

def select_weather_data(connection, date_from, date_to):
    try:
        connection.row_factory = sqlite3.Row
        
        # Define the query
        query = f"SELECT * from [daily_weather_entries] where date >= '{date_from}' and date <= '{date_to}'"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        #results = cursor.fetchall()

        # Iterate over the results and display the results.
        count = 0
        for row in results:
            count += 1
            print( f"ID: {row['id']}, date: {row['date']},  min temperature(°C): {row['min_temp']}, max temperature(°C): {row['max_temp']}, mean temperature(°C): {row['mean_temp']}, precipitation (mm): {row['precipitation']}" )
        
        print(f"Found {count} entries")

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

def select_all_cities(connection):
    try:
        connection.row_factory = sqlite3.Row

        # Define the query
        query = f"SELECT * FROM [cities]"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        for row in results:
            print( f"ID: {row['id']}, name: {row['name']}, longitude: {row['longitude']}, latitude: {row['latitude']}, country ID: {row['country_id']}" )

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

'''
Good
'''
def average_annual_temperature(connection, city_id, year):
    try:
        connection.row_factory = sqlite3.Row
        
        # Define the query
        query = f"SELECT avg([mean_temp]) FROM [daily_weather_entries] where city_id = {city_id} and date >= '{year}-01-01' and date <= '{year}-12-31'"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        count = 0
        for row in results:
            count += 1
            avrg_temp = round(row[0],2)
            print(f"Average annual temperature(°C): {avrg_temp}" )
        
        print(f"Found {count} entries")

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

def average_seven_day_precipitation(connection, city_id, start_date):
    try:
        connection.row_factory = sqlite3.Row

        # Define the query
        end_date_obj = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=7)
        end_date = end_date_obj.strftime("%Y-%m-%d")
        query = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where city_id = {city_id} and date >= '{start_date}' and date <= '{end_date}'"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        count = 0
        for row in results:
            count += 1
            avrg_precipitation = round(row[0],2)
            print(f"Average seven day precipitation (mm): {avrg_precipitation}")

        print(f"Found {count} entries")

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

'''
Very good
'''
def average_mean_temp_by_city(connection, date_from, date_to):
    try:
        connection.row_factory = sqlite3.Row

        # Define the query
        query = f"SELECT avg([mean_temp]) FROM [daily_weather_entries] where date >= '{date_from}' and date <= '{date_to}' GROUP BY city_id"
        print(query)

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        count = 0
        for row in results:
            count += 1
            avg_mean_temp = round(row[0], 2)
            print(f"Average mean temperature by city(°C): {avg_mean_temp}")

        print(f"Found {count} entries")

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

def average_annual_precipitation_by_country(connection, year):
    try:
        connection.row_factory = sqlite3.Row

        # Define the query
        query = f"SELECT id FROM [countries]"

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)
        results = cursor.fetchall()

        # Iterate over the results and display the results.
        for row in results:
            country_id = row[0]
            query = f"SELECT id FROM [cities] where country_id = {country_id}"

            # Get a cursor object from the database connection
            # that will be used to execute database query.
            cursor_cities = connection.cursor()

            # Execute the query via the cursor object.
            results_cities = cursor_cities.execute(query)
            results_cities = cursor_cities.fetchall()

            list = []
            for row_ in results_cities:
                list.append(f"city_id= {row_[0]}")

            cities = ' or '.join(list)
            query = f"SELECT avg([precipitation]) FROM [daily_weather_entries] where {cities} and date >= '{year}-01-01' and date <= '{year}-12-31'"

            # Get a cursor object from the database connection
            # that will be used to execute database query.
            cursor_countries = connection.cursor()

            # Execute the query via the cursor object.
            results_countries = cursor_countries.execute(query)
            results_countries = cursor_countries.fetchall()

            for _row_ in results_countries:
                annual_precipitation = round(_row_[0], 2)
                print(f"Country: {country_id}, average annual precipitation (mm): {annual_precipitation}")

        connection.commit()

    except sqlite3.OperationalError as ex:
        print(ex)

    pass

'''
Excellent

You have gone beyond the basic requirements for this aspect.

'''
def get_date(message):
    date  = input(message)
    return date


def main():

    with sqlite3.connect("CIS4044-N-SDI-OPENMETEO-PARTIAL.db") as connection:
        select_all_countries(connection)
        select_weather_data(connection, "2020-05-05", "2020-06-13")
        select_all_cities(connection)
        average_annual_temperature(connection, 1, "2020")
        average_seven_day_precipitation(connection, 1, "2020-01-01")
        average_mean_temp_by_city(connection, "2020-01-01", "2020-12-01")
        average_annual_precipitation_by_country(connection, "2020")


if __name__ == "__main__":
    # Create a SQLite3 connection and call the various functions
    # above, printing the results to the terminal.
    main()
