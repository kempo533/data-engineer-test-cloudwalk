import psycopg2
import os
from src.etl import extract_data, create_tables, preload_country_data, load_data
from src.sql_queries import Queries
from src.exceptions import APIRequestError


# Main function to orchestrate ETL process
def etl(conn: psycopg2.extensions.connection, api_url: str):
    """
    Orchestrates the ETL process.

    Args:
        conn: psycopg2 connection object.
        api_url: URL of the World Bank API.
    """
    # Extract data from the World Bank API
    try:
        data = extract_data(api_url)
    except APIRequestError as e:
        print(e)
        return

    # Load data into database
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS gdp CASCADE")
            create_tables(cursor)
            preload_country_data(cursor, data)
            load_data(cursor, data)


def generate_pivoted_report(conn: psycopg2.extensions.connection) -> list:
    """
    Generates a pivoted report from the database.

    Args:
        conn: psycopg2 connection object.

    Returns:
        list: List containing the pivoted report data.
    """
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(Queries.SELECT_PIVOT_REPORT)
            pivoted_report = cursor.fetchall()

    return pivoted_report


def write_report_to_file(data: list, filename: str) -> None:
    """
    Writes the report data to a file.

    Args:
        data: List containing the report data.
        filename: Name of the file to write the data to.
    """
    filepath = os.path.join("/app/reports", filename)
    with open(filepath, "w") as file:
        file.write(
            "| {:<5} | {:<15} | {:<10} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8} |\n".format(
                "ID", "Name", "ISO3 Code", "2019", "2020", "2021", "2022", "2023"
            )
        )
        file.write("-" * 95 + "\n")  # Line separator

        for row in data:
            file.write(
                "| {:<5} | {:<15} | {:<10} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8} |\n".format(
                    *row
                )
            )

    return filepath


if __name__ == "__main__":
    api_url = "https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json"
    db_params = {
        "host": os.environ.get("DB_HOST"),
        "database": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }
    conn = psycopg2.connect(**db_params)

    etl(conn, api_url)
    report = generate_pivoted_report(conn)
    write_report_to_file(report, "pivoted_report.txt")
