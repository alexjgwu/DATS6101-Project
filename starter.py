# generate_datasets.py
# ------------------------------------------------------------
# Creates three CSV files in ./data:
#   - patients.csv
#   - doctors.csv
#   - appointments.csv
#
# IDs are simple unique strings you generate here, and
# appointments re-use those IDs to link the collections.
# ------------------------------------------------------------

from faker import Faker
import pandas as pd
import random
from pathlib import Path
import logging
import datetime

logging.basicConfig(level=logging.INFO)


# ----------------- settings (adjust if needed) -----------------
SEED = 2025

if SEED is None:
    random.seed(SEED)

customer_size = 15000
watch_session_size  = 100000

logging.info(f"Customer Size: {customer_size}")
logging.info(f"Watch Session Size: {watch_session_size}")

cust_case_1 = random.randint(100, 250)
cust_case_2 = random.randint(100, 250)
cust_case_3 = random.randint(10, 50)
remaining_cust = customer_size - (cust_case_1 + cust_case_2 + cust_case_3)

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
    "Excited", "Bored", "Intrigued",
    "Shocked", "Frightened", "Disgusted"
]

# Helper: sometimes return None (so CSV cells are empty)
def maybe(value, p_missing=0.1):
    """Return value or None with probability p_missing."""
    return value if random.random() > p_missing else None

# Ensure output folder exists
out_dir = Path("data")
out_dir.mkdir(parents=True, exist_ok=True)

# --------------------------- Customers --------------------------
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

customers_df.to_csv(out_dir / "customers.csv", index=False)


# --------------------------- Watch Sessions --------------------------

def mood_selector(expected_moods = 2):
    """Returns list of moods with defined length expected_moods"""
    moods_list = []
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

sessions = []

for n in range(watch_session_size):

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

