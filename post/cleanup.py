import os

def cleanup_pickles():
    base_path = "/opt/airflow/data"
    pkl_files = [
        "peaks_raw.pkl", "peaks_clean.pkl",
        "members_raw.pkl", "members_clean.pkl",
        "exped_raw.pkl", "exped_clean.pkl"
    ]

    for file in pkl_files:
        path = os.path.join(base_path, file)
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted {file}")
