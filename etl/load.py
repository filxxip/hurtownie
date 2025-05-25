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

def load_pickle_to_table(pickle_name: str, table_name: str):
    path = os.path.join(base_path, pickle_name)
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    df = pd.read_pickle(path)
    engine = get_engine()

    db_columns = [col["name"] for col in inspect(engine).get_columns(table_name)]
    
    if sorted(db_columns) != sorted(df.columns):
        raise ValueError(
            f"Schema mismatch for table '{table_name}'.\n"
            f"Expected columns in DB: {db_columns}\n"
            f"Columns in Pickle: {list(df.columns)}"
        )

    df = df[db_columns]

    df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    print(f"Loaded {pickle_name} → {table_name}")

# === Concrete loaders ===

def load_peaks():
    load_pickle_to_table("peaks_clean.pkl", "Peak")

def load_members():
    load_pickle_to_table("members_clean.pkl", "Member")

def load_exped():
    load_pickle_to_table("exped_clean.pkl", "Expedition")

def load_country_stats_economy():
    load_pickle_to_table("pwt_clean.pkl", "CountryStatsEconomy")
