import pandas as pd
import os

def extract_data():
    base_path = "/opt/airflow/data"
    peaks = pd.read_pickle(os.path.join(base_path, "peaks.pkl"))
    members = pd.read_pickle(os.path.join(base_path, "members.pkl"))
    exped = pd.read_pickle(os.path.join(base_path, "exped.pkl"))
    return peaks, members, exped
