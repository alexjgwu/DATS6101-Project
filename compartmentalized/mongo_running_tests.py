import time
import logging
import inspect
import textwrap
from pymongo import MongoClient

logging.basicConfig(level=logging.WARNING)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['moviesdb_mongo']
customers = db["customers"]
customers_base = db["customers_base"]
watch_sessions = db["watch_sessions"]
watch_sessions_base = db["watch_sessions_base"]


def test_query(query, refresh=None):
    # for calculating query time
    if refresh is not None:
        start = time.perf_counter()
        query()
        end = time.perf_counter()
        refresh()
    else:
        start = time.perf_counter()
        result = query()
        try:
            list(result)
        except TypeError:
            pass
        end = time.perf_counter()

    return end - start


def test_running(query_func, name, runs=10, refresh=None):
    """
    for screenshot
    """
    print(f"\n{'=' * 60}")
    print(f"[{name}]")

    # print code
    q_lines, _ = inspect.getsourcelines(query_func)
    orgcode = "".join(q_lines[1:])
    print(f"Query Logic:\n{textwrap.dedent(orgcode).strip()}")
    print("-" * 30)

    # define target_collection
    target_collection = None
    if refresh == refresh_customers:
        target_collection = customers
    elif refresh == refresh_sessions:
        target_collection = watch_sessions

    # excute 1 time for result
    if target_collection is not None:
        before_count = target_collection.count_documents({})
        res = query_func()

        # get count of rows influenced
        affected = "N/A (Multi-step or unreturned)"
        if hasattr(res, 'modified_count'):
            affected = res.modified_count
        elif hasattr(res, 'deleted_count'):
            affected = res.deleted_count

        after_count = target_collection.count_documents({})

        print(f"Result (Action on '{target_collection.name}'):")
        print(f"  -> Documents Affected: {affected}")
        print(f"  -> Collection Count: {before_count} (Before) -> {after_count} (After)")

        refresh()
    else:
        res = query_func()
        res_list = list(res) if type(res) is not list else res
        print(f"Result (Total {len(res_list)} documents, showing first 3):")
        for i, row in enumerate(res_list[:3]):
            print(f"  Doc {i + 1}: {row}")

    print("-" * 30)
    print("Execution Times:")

    # excute time calculate
    times = []
    for i in range(runs):
        t = test_query(query_func, refresh)
        times.append(t)
        print(f"  Run {i + 1:<2}: {t:.4f}s")
    avg_time = sum(times) / len(times)
    print(f"  Average: {avg_time:.4f}s")
    print(f"{'=' * 60}\n")


# refresh tables
def refresh_customers():
    customers_base.aggregate([
        {'$match': {}},
        {'$out': 'customers'}
    ])
    refresh_sessions()

def refresh_sessions():
    watch_sessions_base.aggregate([
        {'$match': {}},
        {'$out': 'watch_sessions'}
    ])

# ---------------- Tasks ----------------
# Task 1
def task_1():
    watch_sessions.update_one(
        {"_id": 1},
        {
            "$set": {
                "pause_min.0": 42
                }
        }
    )
# Task 2
def task_2():
    return customers.find({
        "$and": [
            {
                "$or": [
                    {"age": {"$lt": 20}},
                    {"age": {"$gt": 50}}
                    ]
            },
            {
                "$or": [
                    {"contact_number": {"$ne": None}},
                    {"email": {"$ne": None}}
                    ]
            }
        ]
    })
# Task 3
def task_3():
    customers.update_many(
    {"age": {"$gt": 60}},
    {"$inc": {"age": 1}}
    )
# Task 4
def task_4():
    return watch_sessions.aggregate([
        {
            "$group": {
                    "_id": "$customer_id",
                    "session_count": {"$sum": 1}
                    }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "_id",
                "foreignField": "_id",
                "as": "customer"
            }
        },
        {
            "$unwind": "$customer"
        },
        {
            "$project": {
                "customer_id": "$_id",
                "customer_name": "$customer.name",
                "session_count": 1,
                "_id": 0
            }
        }
    ])
# Task 5
def task_5():
    return list(
        watch_sessions.aggregate([
            {
                "$match": {
                    "genre": "Action"
                }
            },
            {
                "$group": {
                    "_id": "$customer_id"
                }
            },
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "customer"
                }
            },
            {
                "$unwind": "$customer"
            },
            {
                "$project": {
                    "customer_id": "$_id",
                    "customer_name": "$customer.name",
                    "_id": 0
                }
            }
        ])
    )
# Task 6
def task_6():
    return list(
        customers.find({
            "$and": [
                {
                    "$or": [
                            {"contact_number": {"$exists": False}},
                            {"contact_number": None}
                            ]
                },
                {
                    "email": {"$exists": True, "$ne": None}
                }
            ]
        })
    )
# Task 7
def task_7():
    return watch_sessions.delete_many({
        "$or": [
            {"mood": {"$exists": False}},
            {"mood": None},
            {"mood": {"$size": 0}}
            ]
    })
# Task 8
def task_8():
    return list(watch_sessions.distinct("watch_date"))
# Task 9
def task_9():
    ids = customers.distinct("_id", {
    "age": {"$gt": 60},
    "email": None
    })
    
    watch_sessions.delete_many({"customer_id": {"$in": ids}})
    customers.delete_many({"_id": {"$in": ids}})
# Task 10
def task_10():
    return list(watch_sessions.find({
            "mood": "excited",
            "mood": {"$ne": "bored"}
        })
    )

def main():
    test_running(task_1, "Task 1", refresh = refresh_sessions)
    test_running(task_2, "Task 2")
    test_running(task_3, "Task 3", refresh = refresh_customers)
    test_running(task_4, "Task 4")
    test_running(task_5, "Task 5")
    test_running(task_6, "Task 6")
    test_running(task_7, "Task 7", refresh = refresh_sessions)
    test_running(task_8, "Task 8")
    test_running(task_9, "Task 9", refresh = refresh_customers)
    test_running(task_10, "Task 10")


if __name__ == "__main__":
    main()
