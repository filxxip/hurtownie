import pandas as pd
from sqlalchemy import text
from rapidfuzz import process, fuzz
from utils.db import get_engine  # Upewnij się, że ta funkcja działa

CANONICAL_ROLES = [
    "climber", "leader", "guide", "doctor", "cook",
    "media", "support", "manager", "research", "transport", "other"
]

def categorize_role(name: str) -> str:
    name = name.strip().lower()
    best_match = process.extractOne(name, CANONICAL_ROLES, scorer=fuzz.partial_ratio)
    if best_match and best_match[1] >= 70:
        return best_match[0]
    return "other"

def clean_roles():
    engine = get_engine()

    df = pd.read_sql("SELECT DISTINCT role AS raw_role FROM Member WHERE role IS NOT NULL", con=engine)
    df["role_name"] = df["raw_role"].apply(categorize_role)
    df = df[["role_name"]].drop_duplicates().reset_index(drop=True)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_role"))

    df.to_sql("dim_role", con=engine, if_exists="append", index=False)
    print("Cleaned and reinserted dim_role")

    df_map = pd.read_sql("SELECT DISTINCT role AS raw_role FROM Member WHERE role IS NOT NULL", con=engine)
    df_map["role_name"] = df_map["raw_role"].apply(categorize_role)

    dim_roles = pd.read_sql("SELECT role_id, role_name FROM dim_role", con=engine)
    merged = df_map.merge(dim_roles, on="role_name", how="left")
    merged[["raw_role", "role_id"]].to_sql("tmp_role_mapping", con=engine, if_exists="replace", index=False)
    print("Created tmp_role_mapping (raw → role_id)")

def cleanup_dim_member_nationalities():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("""
            UPDATE dim_member
            SET nationality = LEFT(nationality, CHARINDEX('/', nationality + '/') - 1)
        """)
    print("Cleaned dim_member nationalities to keep only the first value before '/'")

def cleanup_dim_member_nulls():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("""
            UPDATE dim_member
            SET
                first_name = ISNULL(first_name, 'Unknown'),
                last_name = ISNULL(last_name, 'Unknown'),
                sex = ISNULL(sex, 'Unknown'),
                year_of_birth = ISNULL(year_of_birth, 9999),
                nationality = ISNULL(nationality, 'Unknown')
        """)
    print("Replaced NULLs in dim_member with fallback values")

def cleanup_dim_expedition_nulls():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("""
            UPDATE dim_expedition
            SET
                year = ISNULL(year, 9999)
        """)
    print("Replaced NULLs in dim_expedition with fallback values")

def cleanup_dim_roles_empty_strings():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("""
            DELETE FROM dim_role
            WHERE TRIM(role_name) = ''
        """)
    print("Removed empty-string roles from dim_role")
