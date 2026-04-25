from sqlalchemy import create_engine, text
from data_generation import generate_all

DB_URI_ROOT = "mysql+mysqlconnector://root:@localhost/"
DB_URI_DB = "mysql+mysqlconnector://root:@localhost/moviesdb_sql"


def setup_db():
    engine = create_engine(DB_URI_ROOT)

    with engine.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS moviesdb_sql"))
        conn.commit()

    return create_engine(DB_URI_DB)


def setup_tables(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS watch_sessions")
        conn.exec_driver_sql("DROP TABLE IF EXISTS customers")

    customers_table = """
    CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    contact_number VARCHAR(50) NULL,
    email VARCHAR(100) NULL
    );
    """

    watch_sessions_table = """
    CREATE TABLE watch_sessions (
    session_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    watch_date DATE,
    is_rewatch BOOLEAN,
    movie_title VARCHAR(150),
    genre VARCHAR(50),
    mood1 VARCHAR(50) NULL,
    mood2 VARCHAR(50) NULL,
    platform VARCHAR(50),
    runtime INT DEFAULT 0,
    pause_min1 INT DEFAULT 0,
    pause_min2 INT DEFAULT 0,
    pause_min3 INT DEFAULT 0,
    pause_min4 INT DEFAULT 0,
    pause_min5 INT DEFAULT 0,
    pause_min6 INT DEFAULT 0,
    pause_min7 INT DEFAULT 0,
    CONSTRAINT fk_customer
      FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
      ON DELETE CASCADE);
    """

    with engine.connect() as conn:
        conn.execute(text(customers_table))
        conn.execute(text(watch_sessions_table))
        conn.commit()


def load_data(engine, customers_df, sessions_df):
    customers_df.to_sql("customers", engine, if_exists="append", index=False)

    sessions_df.to_sql(
        "watch_sessions",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000
    )

    with engine.connect() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS customers_base")
        conn.exec_driver_sql("DROP TABLE IF EXISTS watch_sessions_base")
        conn.execute(text("CREATE TABLE customers_base AS SELECT * FROM customers"))
        conn.execute(text("CREATE TABLE watch_sessions_base AS SELECT * FROM watch_sessions"))
        conn.commit()


def run_pipeline():
    engine = setup_db()
    setup_tables(engine)

    customers_df, sessions_df = generate_all(15000, 100000)
    load_data(engine, customers_df, sessions_df)

    return engine


if __name__ == "__main__":
    run_pipeline()
