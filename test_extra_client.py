from prefect import task, get_run_logger
from utils import get_tiled_client


@task
def get_other_docs(uid, dry_run=False, api_key=None):
    logger = get_run_logger()
    if not dry_run:
        result = get_tiled_client(api_key)["raw"][uid]
        for name, doc in result.documents():
            logger.info(f"name: {name}, doc: {doc}")
    else:
        logger.info("Dry run: not getting docs")
