import requests
import psycopg2
import os
from .exceptions import APIRequestError, MissingCountryIDError

# Function to extract data from the World Bank API
def extract_data(api_url, per_page=50):
    all_data = []
    page = 1
    while True:
        response = requests.get(f"{api_url}&page={page}&per_page={per_page}")
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data[1])
            if data[0]['pages'] <= page:
                break
            page += 1
        else:
            raise APIRequestError(f"Failed to fetch data from page {page}. Status code: {response.status_code}")
    return all_data


# Function to create the required tables in the PostgreSQL database
def create_tables(cursor):
    # Create country table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS country (
                        id SERIAL PRIMARY KEY,
                        name TEXT UNIQUE,
                        iso3_code TEXT UNIQUE
                        )''')

    # Create gdp table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS gdp (
                        id SERIAL PRIMARY KEY,
                        country_id INTEGER,
                        year INTEGER,
                        value REAL,
                        FOREIGN KEY (country_id) REFERENCES country(id)
                        )''')


# Function to preload country data into the PostgreSQL database
def preload_country_data(cursor, data):
    country_data = set((entry['country']['value'], entry['countryiso3code']) for entry in data)
    cursor.executemany("INSERT INTO country (name, iso3_code) VALUES (%s, %s) ON CONFLICT DO NOTHING", country_data)


# Function to load data into the PostgreSQL database
def load_data(cursor, data):
    # Retrieve country IDs and ISO3 codes
    cursor.execute("SELECT id, iso3_code FROM country")
    country_mapping = {iso3_code: country_id for country_id, iso3_code in cursor.fetchall()}
    print(country_mapping)

    # Prepare and insert data into gdp table
    for entry in data:
        country_iso3_code = entry['countryiso3code']
        country_id = country_mapping.get(country_iso3_code)
        if country_id is not None:
            year = int(entry['date'])
            value = float(entry['value']) if entry['value'] else None
            if value is None:
                value = 0  # Replace missing value with zero
            cursor.execute("INSERT INTO gdp (country_id, year, value) VALUES (%s, %s, %s)", (country_id, year, value))
        else:
            raise MissingCountryIDError(f"Country ID not found for ISO3 code: {country_iso3_code}")


# Main function to orchestrate ETL process
def main():
    api_url = "https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json"
    db_params = {
        "host": os.environ.get("DB_HOST"),
        "database": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }

    # Extract data from the World Bank API
    try:
        data = extract_data(api_url)
    except APIRequestError as e:
        print(e)
        return
    
    # Load data into database
    conn = psycopg2.connect(**db_params)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS gdp CASCADE") # REMOVER ANTES DE ENTREGAR EL CODIGO
            create_tables(cursor)
            preload_country_data(cursor, data)
            load_data(cursor, data)

    conn.close()


if __name__ == "__main__":
    main()

    