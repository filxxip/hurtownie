import pandas as pd
import os
from utils.db import get_engine

base_path = "/opt/airflow/data"

def extract_peaks():
    csv_path = os.path.join(base_path, "peaks.csv")
    pkl_path = os.path.join(base_path, "peaks_raw.pkl")
    if not os.path.exists(csv_path):
        raise FileNotFoundError("Missing peaks.csv")
    df = pd.read_csv(csv_path)
    df.to_pickle(pkl_path)
    print("Extracted peaks.csv → peaks_raw.pkl")

def extract_members():
    csv_path = os.path.join(base_path, "members.csv")
    pkl_path = os.path.join(base_path, "members_raw.pkl")
    if not os.path.exists(csv_path):
        raise FileNotFoundError("Missing members.csv")
    df = pd.read_csv(csv_path)
    df.to_pickle(pkl_path)
    print("Extracted members.csv → members_raw.pkl")

def extract_exped():
    csv_path = os.path.join(base_path, "exped.csv")
    pkl_path = os.path.join(base_path, "exped_raw.pkl")
    if not os.path.exists(csv_path):
        raise FileNotFoundError("Missing exped.csv")
    df = pd.read_csv(csv_path)
    df.to_pickle(pkl_path)
    print("Extracted exped.csv → exped_raw.pkl")

