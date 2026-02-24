from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
import time as ttime
from tiled.client import from_profile
import logging

logger2 = logging.getLogger(__name__)
logger2.setLevel("INFO")


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym):
    logger = get_run_logger()
    api_key = Secret.load("tiled-tst-api-key").get()
    cl = from_profile("nsls2", api_key=api_key)
    run = cl["tst"]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")


@task
def test_print(root_client):
    logger2.warning(f"logging testtesttest: {root_client}")


@flow
def data_validation(uid):
    cl = from_profile("nsls2")
    root_client = cl["tst"]["raw"][uid]
    test_print(root_client)
    read_all_streams(uid, beamline_acronym="tst")
