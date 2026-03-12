import os

from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    with open("/srv/env.secrets", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    logger.info(f"first 4 characters of key: {api_key[:4]}")
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl["tst/raw"][uid]
    return run


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


@flow
def data_validation(uid, api_key=None):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = read_stream(run, stream)  # noqa: F841
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
