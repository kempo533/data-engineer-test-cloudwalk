import pytest
from unittest.mock import MagicMock, patch, call
from ..etl import extract_data, create_tables, preload_country_data, load_data
from ..exceptions import APIRequestError, MissingCountryIDError

# Mock data for testing
@pytest.fixture
def mock_api_data():
    return [
        {'page': 1, 'pages': 1, 'per_page': 50, 'total': 3, 'sourceid': '2', 'lastupdated': '2024-03-28'},
        [
            {'indicator': {'id': 'NY.GDP.MKTP.CD', 'value': 'GDP (current US$)'}, 'country': {'id': 'AR', 'value': 'Argentina'}, 'countryiso3code': 'ARG', 'date': '2023', 'value': None, 'unit': '', 'obs_status': '', 'decimal': 0},
            {'indicator': {'id': 'NY.GDP.MKTP.CD', 'value': 'GDP (current US$)'}, 'country': {'id': 'BR', 'value': 'Brazil'}, 'countryiso3code': 'BRA', 'date': '2022', 'value': 631133384439.944, 'unit': '', 'obs_status': '', 'decimal': 0},
            {'indicator': {'id': 'NY.GDP.MKTP.CD', 'value': 'GDP (current US$)'}, 'country': {'id': 'CL', 'value': 'Chile'}, 'countryiso3code': 'CHL', 'date': '2021', 'value': 487902572164.348, 'unit': '', 'obs_status': '', 'decimal': 0},
        ]
    ]


# Mock connection and cursor
@pytest.fixture
def mock_db_connection():
    with patch('psycopg2.connect', return_value=MagicMock()) as mock_conn, \
         patch.object(mock_conn, 'cursor', return_value=MagicMock()) as mock_cursor:
        yield mock_cursor
        

@patch('requests.get')
def test_extract_data(mock_get, mock_api_data):
    # Mocking requests.get to return status code 200
    mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_api_data)
    result = extract_data("https://example.com/api")
    assert isinstance(result, list) # Ensure a list is returned
    assert len(result) == 3  # Ensure correct number of records extracted

    # Mocking requests.get to return status code 404
    mock_get.return_value = MagicMock(status_code=404)
    with pytest.raises(APIRequestError): # Ensure an APIRequestError exception is raised
        extract_data("https://example.com/api")


def test_create_tables(mock_db_connection):
    mock_cursor = mock_db_connection
    create_tables(mock_cursor)

    print(mock_cursor.execute.call_args_list)

    # Verify execute statements are being called with the correct arguments
    mock_cursor.execute.assert_any_call('''CREATE TABLE IF NOT EXISTS country (
                        id SERIAL PRIMARY KEY,
                        name TEXT UNIQUE,
                        iso3_code TEXT UNIQUE
                        )''')

    mock_cursor.execute.assert_any_call('''CREATE TABLE IF NOT EXISTS gdp (
                        id SERIAL PRIMARY KEY,
                        country_id INTEGER,
                        year INTEGER,
                        value REAL,
                        FOREIGN KEY (country_id) REFERENCES country(id)
                        )''')


def test_preload_country_data(mock_db_connection, mock_api_data):
    mock_cursor = mock_db_connection
    preload_country_data(mock_cursor, mock_api_data[1])

    # Assert that execute was called with the expected SQL statement
    expected_data = set([('Argentina', 'ARG'), ('Brazil', 'BRA'), ('Chile', 'CHL')])
    assert mock_cursor.executemany.call_args == call(
        "INSERT INTO country (name, iso3_code) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        expected_data
    )


def test_load_data(mock_db_connection, mock_api_data):
    mock_cursor = mock_db_connection

    # Test for non-missing country ID
    mock_cursor.fetchall.return_value = [('1', "ARG"), ('2', "BRA"), ('3', "CHL")]
    load_data(mock_cursor, mock_api_data[1])

    # Verify that execute is called with the correct SQL command and arguments for each record
    mock_cursor.execute.assert_has_calls([
        call("INSERT INTO gdp (country_id, year, value) VALUES (%s, %s, %s)",
             ('1', 2023, 0)), # Expecting None to be replaced with 0
        call("INSERT INTO gdp (country_id, year, value) VALUES (%s, %s, %s)",
             ('2', 2022, 631133384439.944)),
        call("INSERT INTO gdp (country_id, year, value) VALUES (%s, %s, %s)",
             ('3', 2021, 487902572164.348)),  
    ], any_order=True)

    # Test for missing country ID and expect a MissingCountryIDError exception
    mock_cursor.fetchall.return_value = [('1', 'USA')]

    # Verify that load_data raises a MissingCountryIDError
    with pytest.raises(MissingCountryIDError):
        load_data(mock_cursor, mock_api_data[1])

