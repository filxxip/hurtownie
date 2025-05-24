from sqlalchemy import create_engine
import urllib

def get_engine():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=sqlserver;DATABASE=master;"
        "UID=sa;PWD=YourStrong@Passw0rd;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}")
