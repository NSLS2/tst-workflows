import os
import sys

from prefect import task, flow, get_run_logger
from data_validation import data_validation
from test_extra_client import get_other_docs
from dotenv import load_dotenv
# from long_flow import long_flow


@task
def get_api_key_from_env(api_key=None):
    with open("/srv/tiled.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
def end_of_run_workflow(stop_doc, dry_run=False, api_key=None):
    uid = stop_doc["run_start"]
    # hello_world()
    if not api_key:
        api_key = get_api_key_from_env(api_key=api_key)
    data_validation(uid, return_state=True, dry_run=dry_run, api_key=api_key)
    get_other_docs(uid, dry_run=dry_run, api_key=api_key)
    # long_flow(iterations=100, sleep_length=10, dry_run=dry_run)
    log_completion(dry_run=dry_run)
    return True


if __name__ == "__main__":
    tiled_api_key = os.environ["TEST_TILED_API_KEY"]
    stop_doc = sys.argv[1]
    end_of_run_workflow(stop_doc, api_key=tiled_api_key)
