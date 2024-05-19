# Cloudwalk Data Ingestion Pipeline Challenge

## Overview
This project is a submission for the Cloudwalk Data Ingestion Pipeline Challenge. The objective is to develop a data ingestion pipeline using Python to extract Gross Domestic Product (GDP) data of South American countries from the World Bank API, load it into a SQL database, and produce a pivoted report of the last 5 years for each country.

## Installation
### Prerequisites
- Docker
- Docker Compose

### Installation Steps
1. Clone the repository:
```
git clone https://github.com/kempo533/data-engineer-test-cloudwalk.git
cd data-engineer-test-cloudwalk
```

2. Build and run the Docker containers:
```
docker-compose up
```

This will build and start the Docker containers, orchestrating the entire data ingestion pipeline.

## Assumptions and Design Decisions
### Database
We use a PostgreSQL database to store the extracted GDP data. It consists of two tables: `country` and `gdp`.

### ETL Process
We implement an Extract, Transform, Load (ETL) process using pure Python scripts.

### Queries
SQL queries are defined to generate reports from the stored GDP data, including retrieving the latest GDP data for each country and pivoting it to create a report with columns for each year.

### Database Configuration
Database connection parameters are configured in the code to connect to the SQL database where the GDP data is stored.

### Environment variables
For convenience of executing this project, the .env file, which contains sensitive information like database user and password, has been commited and pushed to the remote repository. This, in practice, is a critical flaw in security, but since it's just a demo, ease of execution was priorized.

### Containerization
Docker is used to containerize the application components, including the database, ETL scripts, and dependencies. Docker Compose is used to define and manage the containerized services.

### Apache Airflow
Although Apache Airflow was encouraged, it was purposely not used in this project. This decision was made to highlight expertise in Python coding, as requested by the challenge guidelines. By avoiding Airflow, the emphasis was on demonstrating strong Python skills and the ability to create a data pipeline without relying on additional tools. This choice reflects a commitment to meeting the challenge requirements while showcasing proficiency in Python development.

### No Pandas
Pandas or any other dataframe libraries are not used in the implementation of the ETL process. Pure Python is relied upon for data manipulation and processing.

### Tests
Unit testing is implemented to ensure the correctness of the application before execution.

### Linting
We used `black` to ensure appropiate linting compliant with PEP8.

### Results presentation
The pivoted report requested as the output of this project is generated inside the Docker container and saved as a human-readable table format file. You can find this file at {project_root}/reports/pivoted_report.py, thanks to the use of volumes.

## Contact
- **Name:** Nicholas Kemp
- **Email:** kempo533@gmail.com
- **LinkedIn:** [in/nicholaskemps](https://linkedin.com/in/nicholaskemps)