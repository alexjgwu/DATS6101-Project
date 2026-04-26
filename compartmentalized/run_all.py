import subprocess
import sys
import time

# for running these three py file automatically

def run_script(script_name):
    print(f"\n{'=' * 20}")
    print(f"starting running: {script_name}")
    print(f"{'=' * 20}")

    result = subprocess.run([sys.executable, script_name], capture_output=False)

    if result.returncode == 0:
        print(f"{script_name} running successfully")
    else:
        print(f"{script_name} running failed")
        sys.exit(1)


if __name__ == "__main__":
    start_total = time.perf_counter()

    run_script("sql_db_pipeline.py")

    run_script("sql_running_tests.py")

    run_script("mongo_running_tests.py")

    end_total = time.perf_counter()
    print(f"\n everything is done in:{end_total - start_total:.2f} s")