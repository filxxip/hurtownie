import pandas as pd
import os

base_path = "/opt/airflow/data"

def extract_csv_to_pickle(filename: str):
    csv_path = os.path.join(base_path, f"{filename}.csv")
    pkl_path = os.path.join(base_path, f"{filename}_raw.pkl")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing {filename}.csv")
    df = pd.read_csv(csv_path)
    df.to_pickle(pkl_path)
    print(f"Extracted {filename}.csv → {filename}_raw.pkl")

def extract_excel_to_pickle(xlsx_filename: str, sheet: str, output_pkl: str, column_map: dict):
    xlsx_path = os.path.join(base_path, xlsx_filename)
    pkl_path = os.path.join(base_path, output_pkl)
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Missing {xlsx_filename}")

    df = pd.read_excel(xlsx_path, sheet_name=sheet)
    df = df[list(column_map.keys())].copy()
    df.rename(columns=column_map, inplace=True)
    df.dropna(inplace=True)

    if "population" in df.columns:
        df["population"] = df["population"].astype(int)

    df.to_pickle(pkl_path)
    print(f"Extracted {xlsx_filename} ({sheet}) → {output_pkl}")

# Concrete extractors

def extract_peaks():
    extract_csv_to_pickle("peaks")

def extract_members():
    extract_csv_to_pickle("members")

def extract_exped():
    extract_csv_to_pickle("exped")

def extract_country_stats_economy():
    extract_excel_to_pickle(
        xlsx_filename="pwt1001.xlsx",
        sheet="Data",
        output_pkl="pwt_clean.pkl",
        column_map={
            "countrycode": "country_code",
            "year": "year",
            "cgdpo": "gdp_per_capita",
            "pop": "population",
            "hc": "human_capital_index",
            "country": "country_name"
        }
    )
