
import itertools
from utils.db import get_engine
import pandas as pd


def load_dim_roles():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT DISTINCT role AS role_name
        FROM Member
        WHERE role IS NOT NULL
    """, con=engine)
    df.to_sql("dim_role", con=engine, if_exists="append", index=False)
    print("Loaded dim_role")


def load_dim_time():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT basecamp_date AS date FROM Expedition WHERE basecamp_date IS NOT NULL
        UNION ALL
        SELECT summit_date FROM Expedition WHERE summit_date IS NOT NULL
        UNION ALL
        SELECT term_date FROM Expedition WHERE term_date IS NOT NULL
    """, con=engine)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.drop_duplicates(subset=["date"])

    df["year"] = df["date"].dt.year
    df["season"] = df["date"].dt.month // 3 + 1
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    df = df[["year", "season", "month", "day"]]
    df.to_sql("dim_time", con=engine, if_exists="append", index=False)
    print("Loaded dim_time")


def load_dim_members():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            first_name,
            last_name,
            sex,
            year_of_birth,
            nationality
        FROM Member
    """, con=engine)

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df.index.name = "member_id"

    df.to_sql("dim_member", con=engine, if_exists="append")
    print("✅ Loaded dim_member")


def load_dim_peak():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM Peak", con=engine)
    df.to_sql("dim_peak", con=engine, if_exists="append", index=False)
    print("✅ Loaded dim_peak")


def load_dim_expedition():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            expedition_id,
            peak_id,
            season,
            success1,
            success2,
            success3,
            success4,
            basecamp_date,
            summit_date,
            term_date,
            highpoint
        FROM Expedition
    """, con=engine)

    counts = df["expedition_id"].value_counts()
    valid_ids = counts[counts == 1].index
    df = df[df["expedition_id"].isin(valid_ids)]

    df.to_sql("dim_expedition", con=engine, if_exists="append", index=False)
    print("✅ Loaded dim_expedition (without duplicated expedition_ids)")


def load_dim_member_expedition_details():
    engine = get_engine()
    bool_combinations = list(itertools.product([0, 1], repeat=4))
    df = pd.DataFrame(bool_combinations, columns=["success", "is_solo", "oxygen_used", "death"])
    df = df.reset_index().rename(columns={"index": "member_ex_details_id"})
    df.to_sql("dim_member_expedition_details", con=engine, if_exists="append", index=False)
    print("✅ Loaded dim_member_expedition_details")

def load_fact_member_expedition():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            dm.member_id,
            m.expedition_id,
            de.peak_id,
            t_base.time_id AS basecamp_time_id,
            t_summit.time_id AS summit_time_id,
            t_term.time_id AS term_time_id,
            NULL AS country_eco_id,
            dr.role_id,
            dmed.member_ex_details_id
        FROM Member m
        INNER JOIN dim_expedition de ON m.expedition_id = de.expedition_id
        INNER JOIN dim_member dm ON
            m.first_name = dm.first_name AND
            m.last_name = dm.last_name AND
            m.sex = dm.sex AND
            m.year_of_birth = dm.year_of_birth AND
            m.nationality = dm.nationality
        LEFT JOIN dim_time t_base
            ON t_base.year = YEAR(de.basecamp_date)
            AND t_base.month = MONTH(de.basecamp_date)
            AND t_base.day = DAY(de.basecamp_date)
        LEFT JOIN dim_time t_summit
            ON t_summit.year = YEAR(de.summit_date)
            AND t_summit.month = MONTH(de.summit_date)
            AND t_summit.day = DAY(de.summit_date)
        LEFT JOIN dim_time t_term
            ON t_term.year = YEAR(de.term_date)
            AND t_term.month = MONTH(de.term_date)
            AND t_term.day = DAY(de.term_date)
        INNER JOIN dim_role dr ON dr.role_name = m.role
        INNER JOIN dim_member_expedition_details dmed 
            ON dmed.success = m.is_summited AND dmed.is_solo = m.is_solo AND dmed.oxygen_used = m.oxygen_used AND dmed.death = m.death
    """, con=engine)

    df = df.reset_index(drop=True)
    df.index.name = "fact_id"

    df.to_sql("fact_member_expedition", con=engine, if_exists="append")
    print("✅ Loaded fact_member_expedition")

