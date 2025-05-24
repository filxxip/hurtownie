from utils.db import get_engine

def create_staging_tables():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stg_peaks' AND xtype='U')
        CREATE TABLE stg_peaks (
            peak_id VARCHAR(10),
            name VARCHAR(100),
            location VARCHAR(100),
            height_m INT,
            region_himal INT,
            region_sub INT,
            is_trekking BIT,
            first_ascent_year INT
        );
        """)

        conn.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stg_members' AND xtype='U')
        CREATE TABLE stg_members (
            expedition_id VARCHAR(20),
            member_id INT,
            peak_id VARCHAR(10),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            sex CHAR(1),
            year_of_birth INT,
            nationality VARCHAR(50),
            role VARCHAR(50),
            is_summited BIT,
            is_solo BIT,
            oxygen_used BIT,
            death BIT
        );
        """)

        conn.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stg_exped' AND xtype='U')
        CREATE TABLE stg_exped (
            expedition_id VARCHAR(20),
            peak_id VARCHAR(10),
            year INT,
            season INT,
            success1 BIT,
            success2 BIT,
            success3 BIT,
            success4 BIT,
            basecamp_date VARCHAR(20),
            summit_date VARCHAR(20),
            term_date VARCHAR(20),
            highpoint INT
        );
        """)
