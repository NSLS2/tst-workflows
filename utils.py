from dotenv import load_dotenv
from prefect import get_run_logger
from tiled.client import from_uri


def get_tiled_client():
    logger = get_run_logger()
    with open("/srv/env.secrets", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)["tst"]
    return tiled_client
