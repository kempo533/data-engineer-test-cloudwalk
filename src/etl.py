import requests
import psycopg2
from .sql_queries import Queries
from .exceptions import APIRequestError, MissingCountryIDError


def extract_data(api_url: str, per_page: int = 50) -> list:
    """
    Extracts data from the World Bank API.

    Args:
        api_url (str): The URL of the API endpoint.
        per_page (int, optional): Number of entries per page. Defaults to 50.

    Returns:
        list: A list containing all extracted data.

    Raises:
        APIRequestError: If the API request fails.
    """
    all_data = []
    page = 1
    while True:
        response = requests.get(f"{api_url}&page={page}&per_page={per_page}")
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data[1])
            if data[0]["pages"] <= page:
                break
            page += 1
        else:
            raise APIRequestError(
                f"Failed to fetch data from page {page}. Status code: {response.status_code}"
            )
    return all_data


def create_tables(cursor: psycopg2.extensions.cursor) -> None:
    """
    Creates the required tables in the PostgreSQL database if they don't exist.

    Args:
        cursor: Cursor object for executing SQL queries.
    """
    cursor.execute(Queries.CREATE_COUNTRY_TABLE)
    cursor.execute(Queries.CREATE_GDP_TABLE)


def preload_country_data(cursor: psycopg2.extensions.cursor, data: list) -> None:
    """
    Preloads country data into the PostgreSQL database.

    Args:
        cursor: Cursor object for executing SQL queries.
        data (list): List of data entries.
    """
    country_data = set(
        (entry["country"]["value"], entry["countryiso3code"]) for entry in data
    )
    cursor.executemany(Queries.INSERT_COUNTRY_DATA, country_data)


def load_data(cursor: psycopg2.extensions.cursor, data: list) -> None:
    """
    Loads data into the PostgreSQL database.

    Args:
        cursor: Cursor object for executing SQL queries.
        data (list): List of data entries.

    Raises:
        MissingCountryIDError: If country ID is missing.
    """
    # Retrieve country IDs and ISO3 codes
    cursor.execute("SELECT id, iso3_code FROM country")
    country_mapping = {
        iso3_code: country_id for country_id, iso3_code in cursor.fetchall()
    }

    # Prepare and insert data into gdp table
    for entry in data:
        country_iso3_code = entry["countryiso3code"]
        country_id = country_mapping.get(country_iso3_code)
        if country_id is not None:
            year = int(entry["date"])
            value = float(entry["value"]) if entry["value"] else None
            if value is None:
                value = 0  # Replace missing value with zero
            cursor.execute(Queries.INSERT__GDP_DATA, (country_id, year, value))
        else:
            raise MissingCountryIDError(
                f"Country ID not found for ISO3 code: {country_iso3_code}"
            )
