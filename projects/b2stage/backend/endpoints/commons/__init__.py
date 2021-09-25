"""
Common functions for EUDAT endpoints
"""
from restapi.config import API_URL
from restapi.env import Env

# from restapi.utilities.logs import log

CURRENT_B2ACCESS_ENVIRONMENT = Env.get("B2ACCESS_ENV")

MAIN_ENDPOINT_NAME = Env.get("MAIN_ENDPOINT", default="")
PUBLIC_ENDPOINT_NAME = Env.get("PUBLIC_ENDPOINT", default="")

CURRENT_MAIN_ENDPOINT = f"{API_URL}/{MAIN_ENDPOINT_NAME}"
PUBLIC_ENDPOINT = f"{API_URL}/{PUBLIC_ENDPOINT_NAME}"
