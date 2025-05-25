from utils.db import get_engine
from sqlalchemy import text

def drop_tables():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS Peak")
        conn.execute("DROP TABLE IF EXISTS Member")
        conn.execute("DROP TABLE IF EXISTS Expedition")

def drop_datawarehouse_tables():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
        DROP TABLE IF EXISTS fact_member_expedition;
        DROP TABLE IF EXISTS dim_member_expedition_details;
        DROP TABLE IF EXISTS dim_role;
        DROP TABLE IF EXISTS dim_expedition;
        DROP TABLE IF EXISTS dim_member;
        DROP TABLE IF EXISTS dim_time;
        DROP TABLE IF EXISTS dim_peak;
        """))
        print("🧹 Dropped all data warehouse tables.")
