from faker import Faker
import pandas as pd
import random
import datetime

SEED = 2025

fake = Faker()

platforms = ["HBO Max", "Apple TV", "Netflix", "Prime Video", "Peacock", "Tubi"]
genres = ["Action", "Comedy", "Documentary", "Drama", "Sci-Fi", "Thriller"]
moods = ["excited", "bored", "intrigued", "shocked", "frightened", "disgusted"]


def maybe(value, p_missing=0.1):
    return value if random.random() > p_missing else None


# ---------------- CUSTOMERS ----------------
def generate_customers(size):
    customers = []

    case1 = random.randint(100, 250)  # age > 60, no email
    case2 = random.randint(100, 250)  # no phone, has email
    case3 = random.randint(10, 50)    # guaranteed contact present and >50

    remaining = size - (case1 + case2 + case3)

    # Case 1
    for _ in range(case1):
        customers.append({
            "customer_id": len(customers) + 1,
            "name": fake.name(),
            "age": random.randint(60, 80),
            "contact_number": maybe(fake.phone_number()),
            "email": None
        })

    # Case 2
    for _ in range(case2):
        customers.append({
            "customer_id": len(customers) + 1,
            "name": fake.name(),
            "age": random.randint(18, 80),
            "contact_number": None,
            "email": fake.email()
        })

    # Case 3
    for _ in range(case3):
        customers.append({
            "customer_id": len(customers) + 1,
            "name": fake.name(),
            "age": random.randint(51, 80),
            "contact_number": fake.phone_number(),
            "email": fake.email()
        })

    # Remaining
    for _ in range(remaining):
        customers.append({
            "customer_id": len(customers) + 1,
            "name": fake.name(),
            "age": random.randint(18, 80),
            "contact_number": maybe(fake.phone_number(), 0.2),
            "email": maybe(fake.email(), 0.2)
        })

    return pd.DataFrame(customers)


# ---------------- HELPERS ----------------
def mood_selector(expected_moods=2, manual_set=None):
    result = []
    if manual_set:
        result.append(manual_set)

    for _ in range(expected_moods):
        result.append(random.choice(moods))

    return result


def pause_selector(runtime, pause_counts=3):
    return [random.randint(1, runtime) for _ in range(pause_counts)]


def split_list_column(df, col, fill_value=None):
    s = df[col].apply(lambda x: x if isinstance(x, list) else [])
    max_cols = s.map(len).max()

    expanded = s.apply(
        lambda x: pd.Series((x + [fill_value] * max_cols)[:max_cols])
    )

    expanded.columns = [f"{col}{i+1}" for i in range(max_cols)]
    return df.drop(columns=[col]).join(expanded)


# ---------------- SESSIONS ----------------
def generate_sessions(customers_df, size):
    sessions = []
    customer_ids = customers_df["customer_id"].tolist()

    case1 = random.randint(1, 1000)  # no mood
    case2 = random.randint(1, 1000)  # action genre
    case3 = random.randint(1, 1000)  # excited only

    remaining = size - (case1 + case2 + case3)

    # Case 1: no moods
    for _ in range(case1):
        runtime = random.randint(80, 180)
        sessions.append({
            "session_id": len(sessions) + 1,
            "customer_id": random.choice(customer_ids),
            "watch_date": fake.date_between_dates(datetime.datetime(2025,1,1), datetime.datetime(2025,12,31)),
            "is_rewatch": fake.boolean(25),
            "movie_title": fake.word(),
            "genre": random.choice(genres),
            "platform": random.choice(platforms),
            "runtime": runtime,
            "mood": mood_selector(0),
            "pause_min": pause_selector(runtime, random.randint(0,7))
        })

    # Case 2: action genre
    for _ in range(case2):
        runtime = random.randint(80, 180)
        sessions.append({
            "session_id": len(sessions) + 1,
            "customer_id": random.choice(customer_ids),
            "watch_date": fake.date_between_dates(datetime.datetime(2025,1,1), datetime.datetime(2025,12,31)),
            "is_rewatch": fake.boolean(25),
            "movie_title": fake.word(),
            "genre": "Action",
            "platform": random.choice(platforms),
            "runtime": runtime,
            "mood": mood_selector(random.randint(0,2)),
            "pause_min": pause_selector(runtime, random.randint(0,7))
        })

    # Case 3: excited only
    for _ in range(case3):
        runtime = random.randint(80, 180)
        sessions.append({
            "session_id": len(sessions) + 1,
            "customer_id": random.choice(customer_ids),
            "watch_date": fake.date_between_dates(datetime.datetime(2025,1,1), datetime.datetime(2025,12,31)),
            "is_rewatch": fake.boolean(25),
            "movie_title": fake.word(),
            "genre": "Action",
            "platform": random.choice(platforms),
            "runtime": runtime,
            "mood": mood_selector(0, manual_set="excited"),
            "pause_min": pause_selector(runtime, random.randint(0,7))
        })

    # Remaining
    for _ in range(remaining):
        runtime = random.randint(80, 180)
        sessions.append({
            "session_id": len(sessions) + 1,
            "customer_id": random.choice(customer_ids),
            "watch_date": fake.date_between_dates(datetime.datetime(2025,1,1), datetime.datetime(2025,12,31)),
            "is_rewatch": fake.boolean(25),
            "movie_title": fake.word(),
            "genre": random.choice(genres),
            "platform": random.choice(platforms),
            "runtime": runtime,
            "mood": mood_selector(random.randint(0,2)),
            "pause_min": pause_selector(runtime, random.randint(0,7))
        })

    df = pd.DataFrame(sessions)

    return df


# ---------------- ENTRY ----------------
def generate_all(customer_size, session_size):
    customers_df = generate_customers(customer_size)
    sessions_df = generate_sessions(customers_df, session_size)
    return customers_df, sessions_df
