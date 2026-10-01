from prefect import task, get_run_logger
from data_validation import get_run


@task
def get_other_docs(uid, api_key=None):
    logger = get_run_logger()
    result = get_run(uid, api_key=api_key)
    for name, doc in result.documents():
        logger.info(f"name: {name}, doc: {doc}")
