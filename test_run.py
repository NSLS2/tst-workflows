from prefect.logging import disable_run_logger
from prefect.testing.utilities import prefect_test_harness
from end_of_run_workflow import end_of_run_workflow


@pytest.fixture(autouse=True, scope="session")
def prefect_disable_logging():
    with disable_run_logger():
        yield


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def test_end_of_run_workflow():
    with prefect_test_harness():
        assert end_of_run_workflow(stop_doc={"run_start":"f0954c84-f652-4350-9f6d-44b724f4ed9f"})
