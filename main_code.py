# python code
# ------------------------------------------------------------
# One bundle of code that generates fake data (consistent due to seeding)
# Then sets up MYSQL d
# b, and imports data straight from python into DB
# NOTE: MAKE SURE THAT MYSQL IS RUNNING ON COMPUTER, AND ADJUST CREDENTIALS WHERE NECESSARY
# Then runs query tests
# ------------------------------------------------------------

from faker import Faker
import pandas as pd
import random
import logging
import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import time


logging.basicConfig(level=logging.INFO)


# ----------------- settings (adjust if needed) -----------------
SEED = 2025

if SEED is None:
    random.seed(SEED)

customer_size = 15000
watch_session_size  = 100000

logging.info(f"Customer Size: {customer_size}")
logging.info(f"Watch Session Size: {watch_session_size}")

# ---------------------------------------------------------------

fake = Faker()

# Specialties and statuses used in the assignment
platforms = [
    "HBO Max", "Apple TV", "Netflix",
    "Prime Video", "Peacock", "Tubi"
]
genres = [
    "Action", "Comedy", "Documentary", 
    "Drama", "Sci-Fi", "Thriller"
]

moods = [
    "excited", "bored", "intrigued",
    "shocked", "frightened", "disgusted"
]

# Helper: sometimes return None (so CSV cells are empty)
def maybe(value, p_missing=0.1):
    """Return value or None with probability p_missing."""
    return value if random.random() > p_missing else None

# ----------------- SQL setting up and DB creation -----------------

#Update engine w/ your own credentials
engine = create_engine(
    "mysql+mysqlconnector://root:@localhost/"
)

# DB creation
with engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS moviesdb_sql"))
    conn.commit()

# Accessing moviesdb_sql and creating tables w/ requested schema
engine = create_engine(
    "mysql+mysqlconnector://root:@localhost/moviesdb_sql"
)

with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS watch_sessions")
    conn.exec_driver_sql("DROP TABLE IF EXISTS customers")

customers_table = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    contact_number VARCHAR(50) NULL,
    email VARCHAR(100) NULL
);
"""

watch_sessions_table = """
CREATE TABLE IF NOT EXISTS watch_sessions (
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
      ON DELETE CASCADE
);
"""

with engine.connect() as conn:
    conn.execute(text(customers_table))
    conn.execute(text(watch_sessions_table))
    conn.commit()

# --------------------------- Customers Generation --------------------------
cust_case_1 = random.randint(100, 250)
cust_case_2 = random.randint(100, 250)
cust_case_3 = random.randint(10, 50)
remaining_cust = customer_size - (cust_case_1 + cust_case_2 + cust_case_3)

customers = []

# First case to account for: age > 60, no email
for n in range(cust_case_1):
    customers.append({
        "customer_id": len(customers) + 1,
        "name": fake.name(),
        "age": random.randint(60, 80),
        "contact_number": maybe(fake.phone_number()),
        "email": None    # all missing
    })

# Second case to account for: missing phone but has email
for n in range(cust_case_2):
    customers.append({
        "customer_id": len(customers) + 1,
        "name": fake.name(),
        "age": random.randint(18, 80),
        "contact_number": None,
        "email": fake.email()    # all missing
    })

# Last case to account for: <20 (or >50) w/ at least one of email or phone
for n in range(cust_case_3):
    customers.append({
        "customer_id": len(customers) + 1,
        "name": fake.name(),
        "age": random.randint(18, 80),
        "contact_number": fake.phone_number(),
        "email": fake.email()
    })

# generate rest of customer base
for n in range(remaining_cust):
    customers.append({
        "customer_id": len(customers) + 1,
        "name": fake.name(),
        "age": random.randint(18, 80),
        "contact_number": maybe(fake.phone_number(), p_missing = 0.2),
        "email": maybe(fake.email(), p_missing = 0.2)
    })


customers_df = pd.DataFrame(customers)
logging.info(f"Total # of customers generated: {len(customers_df)}")
logging.info(f"Unique customer_ids generated: {len(customers_df.customer_id.unique())}")


# --------------------------- Watch Sessions Generation --------------------------

def mood_selector(expected_moods = 2, manual_set = None):
    """Returns list of moods with defined length expected_moods"""
    moods_list = []

    if manual_set is not None:
        moods_list.append(manual_set)

    for _ in range(expected_moods):
        moods_list.append(random.choice(moods))
    return(moods_list)

def pause_selector(runtime, pause_counts = 8):

    """Returns list of pausetimes within runtime with counts pause_counts"""

    pause_list = []
    for _ in range(pause_counts):
        pause_list.append(random.randint(1, runtime))

    return(pause_list)

def split_list_column(df, col, fill_value=None):
    """
        Splits a list-type column into multiple columns.
    """
    # ensure lists
    s = df[col].apply(lambda x: x if isinstance(x, list) else [])
        
    max_cols = s.map(len).max()

    # expand into columns
    expanded = s.apply(
        lambda x: pd.Series((x + [fill_value] * max_cols)[:max_cols])
    )

    expanded.columns = [f"{col}{i+1}" for i in range(max_cols)]

    return df.drop(columns=[col]).join(expanded)

customer_ids = [c["customer_id"] for c in customers]

sessions_case1 = random.randint(1, 1000)
sessions_case2 = random.randint(1, 1000)
sessions_case3 = random.randint(1, 1000)

remaining_sessions = watch_session_size - (sessions_case1 + sessions_case2 + sessions_case3)

sessions = []

# Generating first special case: sessions w/ no mood

for n in range(sessions_case1):

    runtime = random.randint(80, 180)

    sessions.append({
        "session_id": len(sessions) + 1,
        "customer_id": random.choice(customer_ids),
        "watch_date": fake.date_between_dates(date_start = datetime.datetime(2025, 1, 1), date_end = datetime.datetime(2025, 12, 31)),
        "is_rewatch": fake.boolean(chance_of_getting_true= 25),
        "movie_title": fake.word(),
        "genre": random.choice(genres),
        "platform": random.choice(platforms),
        "runtime": runtime,
        "mood": mood_selector(expected_moods = 0),
        "pause_min" : pause_selector(runtime, pause_counts = random.randint(0, 7))
    })

# Generating second special case: sessions w/ action genre

for n in range(sessions_case2):

    runtime = random.randint(80, 180)

    sessions.append({
        "session_id": len(sessions) + 1,
        "customer_id": random.choice(customer_ids),
        "watch_date": fake.date_between_dates(date_start = datetime.datetime(2025, 1, 1), date_end = datetime.datetime(2025, 12, 31)),
        "is_rewatch": fake.boolean(chance_of_getting_true= 25),
        "movie_title": fake.word(),
        "genre": "Action",
        "platform": random.choice(platforms),
        "runtime": runtime,
        "mood": mood_selector(expected_moods = random.randint(0, 2)),
        "pause_min" : pause_selector(runtime, pause_counts = random.randint(0, 7))
    })

# Generating third case 


for n in range(sessions_case3):

    runtime = random.randint(80, 180)

    sessions.append({
        "session_id": len(sessions) + 1,
        "customer_id": random.choice(customer_ids),
        "watch_date": fake.date_between_dates(date_start = datetime.datetime(2025, 1, 1), date_end = datetime.datetime(2025, 12, 31)),
        "is_rewatch": fake.boolean(chance_of_getting_true= 25),
        "movie_title": fake.word(),
        "genre": "Action",
        "platform": random.choice(platforms),
        "runtime": runtime,
        "mood": mood_selector(expected_moods = 0, manual_set = "excited"),
        "pause_min" : pause_selector(runtime, pause_counts = random.randint(0, 7))
    })


for n in range(remaining_sessions):

    runtime = random.randint(80, 180)

    sessions.append({
        "session_id": len(sessions) + 1,
        "customer_id": random.choice(customer_ids),
        "watch_date": fake.date_between_dates(date_start = datetime.datetime(2025, 1, 1), date_end = datetime.datetime(2025, 12, 31)),
        "is_rewatch": fake.boolean(chance_of_getting_true= 25),
        "movie_title": fake.word(),
        "genre": random.choice(genres),
        "platform": random.choice(platforms),
        "runtime": runtime,
        "mood": mood_selector(expected_moods = random.randint(0, 2)),
        "pause_min" : pause_selector(runtime, pause_counts = random.randint(0, 7))
    })

sessions_df = pd.DataFrame(sessions)
sessions_df_split = split_list_column(df = sessions_df, col = "mood")
sessions_df_split = split_list_column(df = sessions_df_split, col = "pause_min", fill_value = 0)

logging.info(f"Number of Sessions Generated: {len(sessions_df_split)}")
logging.info(f"Number of unique session_ids generated: {len(sessions_df_split.session_id.unique())}")

# --------------------------- Inserting DFs into SQL tables --------------------------

# working databases
customers_df.to_sql(
    name = "customers",
    con = engine,
    if_exists = 'append',
    index = False
)

sessions_df_split.to_sql(
    name = "watch_sessions",
    con = engine,
    if_exists = 'append',
    index = False,
    chunksize = 5000
)

# creating snapshot databases of the working copies to reset for tests
customers_base = """
CREATE TABLE IF NOT EXISTS customers_base AS
SELECT * FROM customers
"""

watch_sessions_base = """
CREATE TABLE IF NOT EXISTS watch_sessions_base AS
SELECT * from watch_sessions 
"""

with engine.connect() as conn:
    conn.execute(text(customers_base))
    conn.execute(text(watch_sessions_base))
    conn.commit()
    result1 = conn.execute(text("SELECT COUNT(*) FROM customers_base"))
    logging.info(f"customers_base row count: {result1.scalar()}")
    result2 = conn.execute(text("SELECT COUNT(*) from watch_sessions_base"))
    logging.info(f"watch_sessions_base row count: {result2.scalar()}")

# --------------------------- SQL Tasks and testing --------------------------

def test_query(query, engine, updating_table = None):
    
    if updating_table is not None:
        delete_query = text(f"DELETE FROM {updating_table}")
        reset_query = text(f"INSERT INTO {updating_table} SELECT * FROM {updating_table}_base")
        count_query = text(f"SELECT COUNT(*) from {updating_table}")
    
        with engine.begin() as conn:
            start = time.perf_counter()
            conn.execute(query)
            end = time.perf_counter()
            result = conn.execute(count_query)
            logging.info(f"Update Complete, new {updating_table} count: {result.scalar()}")

        with engine.begin() as conn:
            conn.execute(delete_query)
            conn.execute(reset_query)
            result = conn.execute(count_query)
            logging.info(f"Data Refreshed, {updating_table} count: {result.scalar()}")
    
    else:
        with engine.connect() as conn:
            start = time.perf_counter()
            result = conn.execute(query)
            rows = result.fetchall()
            end = time.perf_counter()
    query_time = end  - start
    return(query_time)

def test_running(query, engine, test_task, run_size = 10, updating_table = None):
    times = []
    logging.info(f"Starting Runs for {test_task}")
    for n in range(run_size):
        time = test_query(query = query, engine = engine, updating_table = updating_table)
        times.append(time)
        logging.info(f"Run {n + 1} time: {time}")
    logging.info(f"Average time for {test_task}: {sum(times) / len(times)}")
    
    # resetting watch_sessions table as well since it auto deletes on customers deletion
    if updating_table == 'customers':
        delete_query = text("DELETE FROM watch_sessions")
        reset_query = text("INSERT INTO watch_sessions SELECT * FROM watch_sessions_base")
        with engine.begin() as conn:
            conn.execute(delete_query)
            conn.execute(reset_query)
            result = conn.execute(text("SELECT COUNT(*) FROM watch_sessions"))
            logging.info(f"Reset watch_sessions after customers update tests, watch_sessions row count {result.scalar()}")



# Task 1: Update first pause minute for a session
task_1_query = text(
    """
    UPDATE watch_sessions
    SET pause_min1 = 42
    WHERE session_id = 1;
    """
)
test_running(task_1_query, engine = engine, test_task = "Task 1", updating_table= 'watch_sessions')

# Task 2: Customers <20 or >50 with at least one contact value present

task_2_query = text(
    """
    SELECT *
    FROM customers
    WHERE (age < 20 OR age > 50)
    AND (contact_number IS NOT NULL OR email IS NOT NULL)
    """
)

test_running(task_2_query, engine = engine, test_task = "Task 2")

# Task 3: Increase age for customers over 60

task_3_query = text(
    """
    UPDATE customers
    SET age = age + 1
    WHERE age > 60;
    """
)

test_running(task_3_query, engine = engine, test_task = "Task 3", updating_table = 'customers')

# Task 4: Count sessions per customer
task_4_query = text(
    """
    SELECT
        c.customer_id,
        c.name,
        COUNT(ws.session_id) AS session_count
    FROM customers c
    JOIN watch_sessions ws
        ON c.customer_id = ws.customer_id
    GROUP BY c.customer_id, c.name
    ORDER BY COUNT(ws.session_id) desc;
    """
)
test_running(task_4_query, engine = engine, test_task = "Task 4")

# Task 5: Customers who watched Action movies
task_5_query = text(
    """
    SELECT DISTINCT
        c.customer_id,
        c.name
    FROM customers c
    JOIN watch_sessions ws
        ON c.customer_id = ws.customer_id
    WHERE ws.genre = 'Action';
    """
)
test_running(task_5_query, engine = engine, test_task = "Task 5")

# Task 6: Customers with missing phone but email present
task_6_query = text(
    """
    SELECT customer_id, name, age, contact_number, email
    FROM customers
    WHERE contact_number IS NULL
    AND email IS NOT NULL;
    """
)
test_running(task_6_query, engine = engine, test_task = "Task 6")

# Task 7: Delete sessions with no moods recorded


task_7_query = text(
    """
    DELETE FROM watch_sessions
    WHERE mood1 IS NULL
    AND mood2 IS NULL;
    """
)
test_running(task_7_query, engine = engine, test_task = "Task 7", updating_table = 'watch_sessions')

# Task 8: Distinct watch dates
task_8_query = text(
    """
    SELECT DISTINCT watch_date
    FROM watch_sessions
    ORDER BY watch_date;
    """
)
test_running(task_8_query, engine = engine, test_task = "Task 8")

# Task 9: 
task_9_query = text(
    """
    DELETE FROM customers
    WHERE age > 60
    AND email IS NULL;
    """
)
test_running(task_9_query, engine = engine, test_task = "Task 9", updating_table = 'customers')

# Task 10: Sessions with excited but not bored
task_10_query = text(
    """
    SELECT *
    FROM watch_sessions
    WHERE ('excited' IN (mood1, mood2))
    AND ('bored' NOT IN (mood1, mood2));
    """
)
test_running(task_10_query, engine = engine, test_task = "Task 10")