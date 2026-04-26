import time
import logging
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)

engine = create_engine("mysql+mysqlconnector://root:@localhost/moviesdb_sql")


def test_query(query, updating_table=None):
    if updating_table:
        delete_query = text(f"TRUNCATE TABLE {updating_table}")
        reset_query = text(f"INSERT INTO {updating_table} SELECT * FROM {updating_table}_base")
        count_query = text(f"SELECT COUNT(*) FROM {updating_table}")

        with engine.begin() as conn:
            start = time.perf_counter()
            conn.execute(query)
            end = time.perf_counter()
            result = conn.execute(count_query)
            logging.info(f"{updating_table} count after update: {result.scalar()}")

        # reset table
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            conn.execute(delete_query)
            conn.execute(reset_query)
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            result = conn.execute(count_query)
            logging.info(f"{updating_table} reset to: {result.scalar()}")

    else:
        with engine.connect() as conn:
            start = time.perf_counter()
            conn.execute(query).fetchall()
            end = time.perf_counter()

    return end - start


def test_running(query, name, runs=30, updating_table=None):
    times = []

    logging.info(f"Running {name}")

    for i in range(runs):
        t = test_query(query, updating_table)
        times.append(t)
        logging.info(f"Run {i+1}: {t:.4f}s")

    logging.info(f"Average: {sum(times)/len(times):.4f}s\n")

    # reset dependent table if customers modified
    if updating_table == "customers":
        delete_ws = text("TRUNCATE TABLE watch_sessions")
        reset_ws = text("INSERT INTO watch_sessions SELECT * FROM watch_sessions_base")

        with engine.begin() as conn:
            conn.execute(delete_ws)
            conn.execute(reset_ws)
            result = conn.execute(text("SELECT COUNT(*) FROM watch_sessions"))
            logging.info(f"watch_sessions reset after customers test: {result.scalar()}")


# ---------------- TASKS ----------------

task_1 = text("UPDATE watch_sessions SET pause_min1 = 42 WHERE session_id = 1;")

task_2 = text("""
SELECT *
FROM customers
WHERE (age < 20 OR age > 50)
AND (contact_number IS NOT NULL OR email IS NOT NULL)
""")

task_3 = text("UPDATE customers SET age = age + 1 WHERE age > 60;")

task_4 = text("""
SELECT c.customer_id, c.name, COUNT(ws.session_id)
FROM customers c
JOIN watch_sessions ws ON c.customer_id = ws.customer_id
GROUP BY c.customer_id, c.name
""")

task_5 = text("""
SELECT DISTINCT c.customer_id, c.name
FROM customers c
JOIN watch_sessions ws ON c.customer_id = ws.customer_id
WHERE ws.genre = 'Action'
""")

task_6 = text("""
SELECT * FROM customers
WHERE contact_number IS NULL AND email IS NOT NULL
""")

task_7 = text("""
DELETE FROM watch_sessions
WHERE mood1 IS NULL AND mood2 IS NULL
""")

task_8 = text("SELECT DISTINCT watch_date FROM watch_sessions")

task_9 = text("""
DELETE FROM customers
WHERE age > 60 AND email IS NULL
""")

task_10 = text("""
SELECT *
FROM watch_sessions
WHERE 'excited' IN (mood1, mood2)
AND 'bored' NOT IN (mood1, mood2)
""")


def main():
    test_running(task_1, "Task 1", updating_table="watch_sessions")
    test_running(task_2, "Task 2")
    test_running(task_3, "Task 3", updating_table="customers")
    test_running(task_4, "Task 4")
    test_running(task_5, "Task 5")
    test_running(task_6, "Task 6")
    test_running(task_7, "Task 7", updating_table="watch_sessions")
    test_running(task_8, "Task 8")
    test_running(task_9, "Task 9", updating_table="customers")
    test_running(task_10, "Task 10")


if __name__ == "__main__":
    main()