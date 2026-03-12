from prefect.logging import disable_run_logger
from end_of_run_workflow import end_of_run_workflow
import os
import pytest


@pytest.fixture(autouse=True, scope="session")
def prefect_disable_logging():
    with disable_run_logger():
        yield


def test_end_of_run_workflow(prefect_disable_logging):
    print("starting test!")
    assert end_of_run_workflow(
        stop_doc={"run_start": "f0954c84-f652-4350-9f6d-44b724f4ed9f"},
        api_key=os.environ["TEST_TILED_API_KEY"],
    )
    print("finished test!")
