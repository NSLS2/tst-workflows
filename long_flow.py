from prefect import task, flow, get_run_logger
import time as ttime


@task
def print_and_sleep(iterations, sleep_length, dry_run=False):
    logger = get_run_logger()
    # Long running task
    print("Long task...")
    if dry_run:
        logger.info("Dry run: skipping long task")
        return
    for i in range(int(iterations)):
        print(f"Iteration number {i}")
        ttime.sleep(int(sleep_length))


@flow(log_prints=True)
def long_flow(iterations, sleep_length, dry_run=False):
    print("Starting long flow...")
    print_and_sleep(iterations, sleep_length, dry_run=dry_run)
    print("Done!")
