import pandas as pd
import os
from utils.db import get_engine

def load_data():
    engine = get_engine()
    base_path = "/opt/airflow/data"

    table_map = {
        "peaks_clean.pkl": "Peak",
        "exped_clean.pkl": "Expedition",
        "members_clean.pkl": "Member"
    }

    for filename, table_name in table_map.items():
        df = pd.read_pickle(os.path.join(base_path, filename))
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
        )
