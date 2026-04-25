from sqlalchemy import create_engine, text
from data_generation import generate_all, split_list_column
import logging
from pymongo import MongoClient
import pandas as pd

logging.basicConfig(level=logging.INFO)


SQL_DB_URI_ROOT = "mysql+mysqlconnector://root:@localhost/"
SQL_DB_URI_DB = "mysql+mysqlconnector://root:@localhost/moviesdb_sql"


def setup_sql_db():
    engine = create_engine(SQL_DB_URI_ROOT)

    with engine.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS moviesdb_sql"))
        conn.commit()

    return create_engine(SQL_DB_URI_DB)


def setup_sql_tables(engine):
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


def load_sql_data(engine, customers_df, sessions_df):
    customers_df.to_sql("customers", engine, if_exists="append", index=False)
    # verifying successful imports with table counts
    with engine.connect() as conn:
        customers_result = conn.execute(text("SELECT COUNT(*) FROM customers"))
        logging.info(f"Customers data imported into moviesdb_sql, count in customers table: {customers_result.scalar()}")

    sessions_df.to_sql(
        "watch_sessions",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000
    )

    with engine.connect() as conn:
        sessions_result = conn.execute(text("SELECT COUNT(*) FROM watch_sessions"))
        logging.info(f"Watch session data imported into moviesdb_sql, count in watch_sessions table: {sessions_result.scalar()}")

    # Creating base tables to re-update after our update tests
    with engine.connect() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS customers_base")
        conn.exec_driver_sql("DROP TABLE IF EXISTS watch_sessions_base")
        conn.execute(text("CREATE TABLE customers_base AS SELECT * FROM customers"))
        conn.execute(text("CREATE TABLE watch_sessions_base AS SELECT * FROM watch_sessions"))
        conn.commit()
    

def load_mongo_data(db = None, customers_df = None, sessions_df = None):
    customers = db["customers"]
    watch_sessions = db["watch_sessions"]

    # dropping collections if already existing
    customers.drop()
    watch_sessions.drop()

    #
    customers_records = customers_df.to_dict(orient = "records")
    customers.insert_many(customers_records)
    cust_count = customers.count_documents({})
    logging.info(f"Customers loaded into moviesdb_mongo. Count in customers collection: {cust_count}")

    watch_sessions_records = sessions_df.to_dict(orient = "records")
    watch_sessions.insert_many(watch_sessions_records)
    sess_count = watch_sessions.count_documents({})
    logging.info(f"Customers loaded into moviesdb_mongo. Count in watch_sessions collection: {sess_count}")


    return None



def run_pipeline():

    engine = setup_sql_db()
    setup_sql_tables(engine)
    client = MongoClient("mongodb://localhost:27017/")
    db = client["moviesdb_mongo"]
    
    customers_df, sessions_df = generate_all(15000, 100000)
    logging.info("Movie Data Generated, preparing to import into databases")
    # last second cleaning before import, also splitting out moods and pausetime parameters for SQL schema
    sessions_df["watch_date"] = pd.to_datetime(sessions_df["watch_date"])
    sessions_df_split = split_list_column(sessions_df, "mood")
    sessions_df_split = split_list_column(sessions_df_split, "pause_min", fill_value=0)

    load_sql_data(engine, customers_df, sessions_df_split)
    load_mongo_data(db, customers_df, sessions_df)

    return engine


if __name__ == "__main__":
    run_pipeline()
