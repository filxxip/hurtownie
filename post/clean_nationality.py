from utils.db import get_engine


def cleanup_dim_member_nationalities():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("""
            UPDATE dim_member
            SET nationality = LEFT(nationality, CHARINDEX('/', nationality + '/') - 1)
        """)
    print("Updated dim_member nationalities to keep only the first value before '/'")