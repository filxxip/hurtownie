import pandas as pd
import os
from sqlalchemy import inspect
from utils.db import get_engine

base_path = "/opt/airflow/data"

def check_schema_match(df: pd.DataFrame, table_name: str, engine) -> bool:
    inspector = inspect(engine)
    db_columns = [col["name"] for col in inspector.get_columns(table_name)]
    df_columns = list(df.columns)
    return db_columns == df_columns

def load_peaks():
    path = os.path.join(base_path, "peaks_clean.pkl")
    df = pd.read_pickle(path)
    engine = get_engine()

    if check_schema_match(df, "Peak", engine):
        df.to_sql(name="Peak", con=engine, if_exists="append", index=False)
    else:
        raise ValueError(
            f"Schema mismatch for table 'Peak'.\n"
            f"Expected columns in DB: {[col['name'] for col in inspect(engine).get_columns('Peak')]}\n"
            f"Columns in Pickle: {list(df.columns)}"
        )

def load_members():
    path = os.path.join(base_path, "members_clean.pkl")
    df = pd.read_pickle(path)
    engine = get_engine()

    if check_schema_match(df, "Member", engine):
        df.to_sql(name="Member", con=engine, if_exists="append", index=False)
    else:
        raise ValueError(
            f"Schema mismatch for table 'Member'.\n"
            f"Expected columns in DB: {[col['name'] for col in inspect(engine).get_columns('Member')]}\n"
            f"Columns in Pickle: {list(df.columns)}"
        )

def load_exped():
    path = os.path.join(base_path, "exped_clean.pkl")
    df = pd.read_pickle(path)
    engine = get_engine()

    if check_schema_match(df, "Expedition", engine):
        df.to_sql(name="Expedition", con=engine, if_exists="append", index=False)
    else:
        raise ValueError(
            f"Schema mismatch for table 'Expedition'.\n"
            f"Expected columns in DB: {[col['name'] for col in inspect(engine).get_columns('Expedition')]}\n"
            f"Columns in Pickle: {list(df.columns)}"
        )
