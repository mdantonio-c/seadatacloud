"""
Common functions for EUDAT endpoints
"""
from restapi.config import API_URL, PRODUCTION
from restapi.env import Env

CURRENT_HTTPAPI_SERVER = Env.get("DOMAIN") or ""
CURRENT_B2ACCESS_ENVIRONMENT = Env.get("B2ACCESS_ENV")

MAIN_ENDPOINT_NAME = Env.get("MAIN_ENDPOINT", default="")
PUBLIC_ENDPOINT_NAME = Env.get("PUBLIC_ENDPOINT", default="")

CURRENT_MAIN_ENDPOINT = f"{API_URL}/{MAIN_ENDPOINT_NAME}"
PUBLIC_ENDPOINT = f"{API_URL}/{PUBLIC_ENDPOINT_NAME}"

if not PRODUCTION:
    # FIXME: how to get the PORT?
    # It is not equivalent to config.get_backend_url() ??
    CURRENT_HTTPAPI_SERVER += ":8080"
