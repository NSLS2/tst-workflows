from prefect import task, flow, get_run_logger
from data_validation import data_validation
from test_extra_client import get_other_docs
# from long_flow import long_flow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run:{dry_run}")


@flow
def end_of_run_workflow(stop_doc, dry_run=False, api_key=None):
    uid = stop_doc["run_start"]
    # hello_world()
    data_validation(uid, return_state=True, dry_run=dry_run, api_key=api_key)
    get_other_docs(uid, dry_run=dry_run)
    # long_flow(iterations=100, sleep_length=10, dry_run=dry_run)
    log_completion(dry_run=dry_run)
