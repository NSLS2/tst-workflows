import time as ttime

from dotenv import dotenv_values
from prefect import flow, get_run_logger, task
from tiled.client import from_uri

BEAMLINE_OR_ENDSTATION = "tst"


def get_api_key_from_env():
    config = dotenv_values("/srv/container.secret")
    return config.get("TILED_API_KEY")


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    return cl[f"{BEAMLINE_OR_ENDSTATION}/raw"][uid]


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


@flow
def data_validation(uid, api_key=None):
    logger = get_run_logger()
    logger.info(f"Validating uid {uid}")
    start_time = ttime.monotonic()
    run_client = get_run(uid, api_key=api_key)
    for stream in run_client:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = read_stream(run_client, stream)  # noqa: F841
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
