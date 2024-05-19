class Queries:
    CREATE_COUNTRY_TABLE = """CREATE TABLE IF NOT EXISTS country (
                        id SERIAL PRIMARY KEY,
                        name TEXT UNIQUE,
                        iso3_code TEXT UNIQUE
                        )"""

    CREATE_GDP_TABLE = """CREATE TABLE IF NOT EXISTS gdp (
                        id SERIAL PRIMARY KEY,
                        country_id INTEGER,
                        year INTEGER,
                        value REAL,
                        FOREIGN KEY (country_id) REFERENCES country(id)
                        )"""

    INSERT_COUNTRY_DATA = (
        "INSERT INTO country (name, iso3_code) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    )

    INSERT__GDP_DATA = "INSERT INTO gdp (country_id, year, value) VALUES (%s, %s, %s)"

    SELECT_PIVOT_REPORT = """SELECT
                                c.id,
                                c.name,
                                c.iso3_code,
                                CASE WHEN MAX(CASE WHEN g.year = 2019 THEN g.value END) = 0 THEN 'No data' ELSE TO_CHAR(MAX(CASE WHEN g.year = 2019 THEN g.value END) / 1000000000, 'FM999999999.99') END AS "2019",
                                CASE WHEN MAX(CASE WHEN g.year = 2020 THEN g.value END) = 0 THEN 'No data' ELSE TO_CHAR(MAX(CASE WHEN g.year = 2020 THEN g.value END) / 1000000000, 'FM999999999.99') END AS "2020",
                                CASE WHEN MAX(CASE WHEN g.year = 2021 THEN g.value END) = 0 THEN 'No data' ELSE TO_CHAR(MAX(CASE WHEN g.year = 2021 THEN g.value END) / 1000000000, 'FM999999999.99') END AS "2021",
                                CASE WHEN MAX(CASE WHEN g.year = 2022 THEN g.value END) = 0 THEN 'No data' ELSE TO_CHAR(MAX(CASE WHEN g.year = 2022 THEN g.value END) / 1000000000, 'FM999999999.99') END AS "2022",
                                CASE WHEN MAX(CASE WHEN g.year = 2023 THEN g.value END) = 0 THEN 'No data' ELSE TO_CHAR(MAX(CASE WHEN g.year = 2023 THEN g.value END) / 1000000000, 'FM999999999.99') END AS "2023"
                            FROM
                                country c
                            LEFT JOIN
                                gdp g ON c.id = g.country_id
                            GROUP BY
                                c.id,
                                c.name,
                                c.iso3_code
                            ORDER BY
                                c.id;"""
