import os
import pandas as pd

base_path = "/opt/airflow/data"

def transform_peaks():
    peaks = pd.read_pickle(os.path.join(base_path, "peaks_raw.pkl"))
    peaks_clean = peaks.rename(columns={
        "PEAKID": "peak_id",
        "PKNAME": "name",
        "LOCATION": "location",
        "HEIGHTM": "height_m",
        "HIMAL": "region_himal",
        "REGION": "region_sub",
        "TREKKING": "is_trekking",
        "PYEAR": "first_ascent_year"
    })[[
        "peak_id", "name", "location", "height_m",
        "region_himal", "region_sub", "is_trekking", "first_ascent_year"
    ]]
    peaks_clean.to_pickle(os.path.join(base_path, "peaks_clean.pkl"))

def transform_exped():
    exped = pd.read_pickle(os.path.join(base_path, "exped_raw.pkl"))
    exped_clean = exped.rename(columns={
        "EXPID": "expedition_id",
        "PEAKID": "peak_id",
        "YEAR": "year",
        "SEASON": "season",
        "SUCCESS1": "success1",
        "SUCCESS2": "success2",
        "SUCCESS3": "success3",
        "SUCCESS4": "success4",
        "BCDATE": "basecamp_date",
        "SMTDATE": "summit_date",
        "TERMDATE": "term_date",
        "HIGHPOINT": "highpoint"
    })[[
        "expedition_id", "peak_id", "year", "season",
        "success1", "success2", "success3", "success4",
        "basecamp_date", "summit_date", "term_date", "highpoint"
    ]]
    exped_clean.to_pickle(os.path.join(base_path, "exped_clean.pkl"))

def transform_members():
    members = pd.read_pickle(os.path.join(base_path, "members_raw.pkl"))
    members = members.dropna(subset=["EXPID", "MEMBID"])
    members_clean = members.rename(columns={
        "EXPID": "expedition_id",
        "MEMBID": "member_id",
        "PEAKID": "peak_id",
        "FNAME": "first_name",
        "LNAME": "last_name",
        "SEX": "sex",
        "YOB": "year_of_birth",
        "CITIZEN": "nationality",
        "STATUS": "role",
        "MSUCCESS": "is_summited",
        "MSOLO": "is_solo",
        "MO2USED": "oxygen_used",
        "DEATH": "death"
    })[[
        "expedition_id", "member_id", "peak_id",
        "first_name", "last_name", "sex", "year_of_birth",
        "nationality", "role", "is_summited", "is_solo",
        "oxygen_used", "death"
    ]]
    members_clean.to_pickle(os.path.join(base_path, "members_clean.pkl"))

def transform_country_stats_economy():
    stats = pd.read_pickle(os.path.join(base_path, "pwt_clean.pkl"))

    stats_clean = stats.rename(columns={
        "country_code": "country_code",
        "contry_name": "country_name",
        "year": "year",
        "gdp_per_capita": "gdp_per_capita",
        "population": "population",
        "human_capital_index": "human_capital_index"
    })[
        ["country_code", "country_name", "year", "gdp_per_capita", "population", "human_capital_index"]
    ]

    def assign_hci_bucket(df_year):
        df_year = df_year.sort_values("human_capital_index", ascending=False).reset_index(drop=True)
        n = len(df_year)
        top = int(n * 0.3)
        mid = int(n * 0.6)

        bucket = []
        for i in range(n):
            if i < top:
                bucket.append("wysokie")
            elif i < mid:
                bucket.append("średnie")
            else:
                bucket.append("niskie")
        df_year["hci_bucket"] = bucket
        return df_year

    stats_with_buckets = stats_clean.groupby("year", group_keys=False).apply(assign_hci_bucket)

    stats_with_buckets.to_pickle(os.path.join(base_path, "pwt_transformed.pkl"))
    print("Transformed pwt_clean.pkl → pwt_transformed.pkl with HCI buckets")
