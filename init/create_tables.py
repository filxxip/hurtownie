from utils.db import get_engine
from sqlalchemy import text

def create_required_tables():
    engine = get_engine()
    with engine.connect() as conn:
        print("Creating table: Peak")
        conn.execute(text("""
            CREATE TABLE Peak (
                peak_id VARCHAR(10),
                name VARCHAR(100),
                location VARCHAR(100),
                height_m INT,
                region_himal INT,
                region_sub INT,
                is_trekking BIT,
                first_ascent_year INT
            )
        """))

        print("Creating table: Member")
        conn.execute(text("""
            CREATE TABLE Member (
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
            )
        """))

        print("Creating table: Expedition")
        conn.execute(text("""
            CREATE TABLE Expedition (
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
            )
        """))

        conn.execute(text("""
        CREATE TABLE CountryStatsEconomy (
            country_code VARCHAR(3),
            country_name VARCHAR(100),
            year INT,
            gdp_per_capita FLOAT,
            population INT,
            human_capital_index FLOAT
        )
        """))


def create_datawarehouse_tables():
    engine = get_engine()
    with engine.connect() as conn:

        # === DIM PEAK ===
        conn.execute(text("""
        CREATE TABLE dim_peak (
            peak_id VARCHAR(10) PRIMARY KEY,
            name VARCHAR(100),
            location VARCHAR(100),
            height_m INT,
            region_himal INT,
            region_sub INT,
            is_trekking BIT,
            first_ascent_year INT
        )
        """))

        # === DIM TIME ===
        conn.execute(text("""
        CREATE TABLE dim_time (
            time_id INT IDENTITY(1,1) PRIMARY KEY,
            year INT,
            season INT,
            month INT,
            day INT,
            CONSTRAINT uc_dim_time UNIQUE(year, season, month, day)
        )
        """))

        # === DIM MEMBER ===
        conn.execute(text("""
        CREATE TABLE dim_member (
            member_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            sex CHAR(1),
            year_of_birth INT,
            nationality VARCHAR(50)
        )
        """))

        # === DIM ROLE ===
        conn.execute(text("""
        CREATE TABLE dim_role (
            role_id INT IDENTITY(1,1) PRIMARY KEY,
            role_name VARCHAR(50) UNIQUE
        )
        """))

        # === DIM EXPEDITION ===
        conn.execute(text("""
        CREATE TABLE dim_expedition (
            expedition_id VARCHAR(20) PRIMARY KEY,
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
        )
        """))

        # === DIM MEMBER EXPEDITION DETAILS ===
        conn.execute(text("""
        CREATE TABLE dim_member_expedition_details (
            member_ex_details_id INT PRIMARY KEY,
            success BIT,
            is_solo BIT,
            oxygen_used BIT,
            death BIT
        )
        """))

        # === DIM COUNTRY ECONOMY ===
        conn.execute(text("""
        CREATE TABLE dim_country_economy (
            country_eco_id INT IDENTITY(1,1) PRIMARY KEY,
            country_code VARCHAR(3),
            country_name VARCHAR(100),
            year INT,
            gdp_per_capita FLOAT,
            population INT,
            human_capital_index FLOAT
        )
        """))

        # === FACT MEMBER EXPEDITION ===
        conn.execute(text("""
        CREATE TABLE fact_member_expedition (
            fact_id INT PRIMARY KEY,
            member_id INT,
            expedition_id VARCHAR(20),
            peak_id VARCHAR(10),
            basecamp_time_id INT,
            summit_time_id INT,
            term_time_id INT,
            country_eco_id INT,
            role_id INT,
            member_ex_details_id INT,
            FOREIGN KEY (member_id) REFERENCES dim_member(member_id),
            FOREIGN KEY (expedition_id) REFERENCES dim_expedition(expedition_id),
            FOREIGN KEY (peak_id) REFERENCES dim_peak(peak_id),
            FOREIGN KEY (basecamp_time_id) REFERENCES dim_time(time_id),
            FOREIGN KEY (summit_time_id) REFERENCES dim_time(time_id),
            FOREIGN KEY (term_time_id) REFERENCES dim_time(time_id),
            FOREIGN KEY (role_id) REFERENCES dim_role(role_id),
            FOREIGN KEY (member_ex_details_id) REFERENCES dim_member_expedition_details(member_ex_details_id),
            FOREIGN KEY (country_eco_id) REFERENCES dim_country_economy(country_eco_id) -- 🔥 dodane
        )

        """))

        print("Data warehouse tables created.")
