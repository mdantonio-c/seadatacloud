import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from restapi.connectors.celery import CeleryExt, Task
from restapi.utilities.logs import log
from seadata.connectors.b2handle import PIDgenerator
from seadata.endpoints import UID_PREFIX, ImportManagerAPI

# Size in bytes
# TODO: move me into the configuration
MAX_ZIP_SIZE = 2147483648  # 2 gb

ext_api = ImportManagerAPI()

#####################
# https://stackoverflow.com/q/16040039
log.debug("celery: disable prefetching")
# disable prefetching
CeleryExt.celery_app.conf.update(
    # CELERYD_PREFETCH_MULTIPLIER=1,
    # CELERY_ACKS_LATE=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)

pmaker = PIDgenerator()


def notify_error(
    error: Tuple[str, str],
    payload: Dict[str, Any],
    backdoor: bool,
    # Any annotation to be fixed with:
    # https://github.com/python/mypy/issues/5876
    task: Any,
    extra: Optional[Any] = None,
    subject: Optional[Path] = None,
    edmo_code: Optional[int] = None,
) -> str:

    error_message = f"Error {error[0]}: {error[1]}"
    if subject is not None:
        error_message = f"{error_message}. [{subject}]"
    log.error(error_message)
    if extra:
        log.error(str(extra))

    payload["errors"] = []
    e = {
        "error": error[0],
        "description": error[1],
    }
    if subject is not None:
        e["subject"] = str(subject)

    payload["errors"].append(e)

    if backdoor:
        log.warning(
            "You enabled the backdoor: this json will not be sent to ImportManagerAPI"
        )
        log.info(payload)
    else:
        ext_api.post(payload, edmo_code=edmo_code)

    task_errors = [error_message]
    if extra:
        task_errors.append(str(extra))
    task.update_state(state="FAILED", meta={"errors": task_errors})
    return "Failed"


def generate_uid() -> str:
    """generate an uid made of a prefix related to the single instance and a random uuid"""
    new_uid = f"{UID_PREFIX}/{uuid.uuid4()}"
    return new_uid
