import os

def cleanup_pickles():
    base_path = "/opt/airflow/data"
    pkl_files = [
        "peaks_raw.pkl", "peaks_clean.pkl",
        "members_raw.pkl", "members_clean.pkl",
        "exped_raw.pkl", "exped_clean.pkl",
        "pwt_transformed.pkl", "pwt_clean.pkl"
    ]

    for file in pkl_files:
        path = os.path.join(base_path, file)
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted {file}")

from sqlalchemy import text
from utils.db import get_engine

def cleanup_tmp_tables():
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS tmp_role_mapping"))
            print("Dropped tmp_role_mapping")
    except Exception as e:
        print(f"Failed to drop tmp_role_mapping: {e}")
