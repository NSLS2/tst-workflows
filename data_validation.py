from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
import time as ttime
from tiled.client import from_profile


@task(retries=2, retry_delay_seconds=10)
def read_run(client, uid, dry_run=False):
    logger = get_run_logger()
    if dry_run:
        logger.info(f"Dry run: not reading {uid}")
        return None
    run = client["tst"]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    return run


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


@flow
def read_all_streams(uid, beamline_acronym, dry_run=False):
    logger = get_run_logger()
    api_key = Secret.load("tiled-tst-api-key").get()
    cl = from_profile("nsls2", api_key=api_key)
    run = read_run(cl, uid, dry_run)
    start_time = ttime.monotonic()
    if dry_run:
        logger.info(f"Dry run: not reading streams from uid {uid}")
    else:
        for stream in run:
            logger.info(f"{stream}:")
            stream_start_time = ttime.monotonic()
            stream_data = read_stream(run, stream)  # noqa: F841
            stream_elapsed_time = ttime.monotonic() - stream_start_time
            logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
            logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")


@flow
def data_validation(uid, dry_run=False):
    read_all_streams(uid, beamline_acronym="tst", dry_run=dry_run)
