from tiled.client import from_uri
from prefect.blocks.system import Secret

import os

LOCATION = "tst"


def get_tiled_client(api_key=None):
    if not api_key:
        api_key = Secret.load(f"tiled-{LOCATION}-api-key").get()
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)[LOCATION]
    return tiled_client
