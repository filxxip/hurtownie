from utils.db import get_engine
import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import text

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

    # Load raw role names from Member table
    df = pd.read_sql("SELECT DISTINCT role AS raw_role FROM Member WHERE role IS NOT NULL", con=engine)
    df["role_name"] = df["raw_role"].apply(categorize_role)
    df = df[["role_name"]].drop_duplicates().reset_index(drop=True)

    # Clean old data
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_role"))

    # Insert cleaned data
    df.to_sql("dim_role", con=engine, if_exists="append", index=False)
    print("✅ Cleaned and reinserted dim_role")

    # Create mapping table for later use in fact table join
    df_map = pd.read_sql("SELECT DISTINCT role AS raw_role FROM Member WHERE role IS NOT NULL", con=engine)
    df_map["role_name"] = df_map["raw_role"].apply(categorize_role)

    dim_roles = pd.read_sql("SELECT role_id, role_name FROM dim_role", con=engine)
    merged = df_map.merge(dim_roles, on="role_name", how="left")
    merged[["raw_role", "role_id"]].to_sql("tmp_role_mapping", con=engine, if_exists="replace", index=False)
    print("✅ Created tmp_role_mapping (raw → role_id)")
