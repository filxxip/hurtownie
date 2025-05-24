FROM apache/airflow:2.9.0-python3.9

USER root

# System dependencies
RUN apt-get update && apt-get install -y gcc g++ gnupg unixodbc-dev curl

# Microsoft SQL Server ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Copy requirements
COPY requirements.txt /requirements.txt

# 👇 Run pip install as airflow user inside build stage
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install psycopg2-binary

