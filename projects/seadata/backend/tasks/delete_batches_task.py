from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, List

from celery.app.task import Task
from glom import glom
from restapi.connectors.celery import CeleryExt
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.endpoints import ErrorCodes
from seadata.tasks.seadata import ext_api, notify_error

TIMEOUT = 180


@CeleryExt.task(idempotent=False)
def delete_batches(
    self: Task, batches_path: str, local_batches_path: str, myjson: Dict[str, Any]
) -> str:

    if "parameters" not in myjson:
        myjson["parameters"] = {}

    backdoor = glom(myjson, "parameters.backdoor", default=False)

    if "request_id" not in myjson:
        return notify_error(ErrorCodes.MISSING_REQUEST_ID, myjson, backdoor, self)

    myjson["parameters"]["request_id"] = myjson["request_id"]
    myjson["request_id"] = self.request.id

    # params = myjson.get('parameters', {})

    batches = myjson["parameters"].pop("batches", None)
    if batches is None:
        return notify_error(
            ErrorCodes.MISSING_BATCHES_PARAMETER, myjson, backdoor, self
        )
    total = len(batches)

    if total == 0:
        return notify_error(ErrorCodes.EMPTY_BATCHES_PARAMETER, myjson, backdoor, self)

    try:
        with irods.get_instance() as imain:

            errors: List[Dict[str, str]] = []
            counter = 0
            for batch in batches:

                counter += 1
                self.update_state(
                    state="PROGRESS",
                    meta={"total": total, "step": counter, "errors": len(errors)},
                )

                batch_path = Path(batches_path, batch)
                local_batch_path = Path(local_batches_path, batch)
                log.info("Delete request for batch collection {}", batch_path)
                log.info("Delete request for batch path {}", local_batch_path)

                try:
                    start_timeout(TIMEOUT)
                    if not imain.is_collection(batch_path):
                        errors.append(
                            {
                                "error": ErrorCodes.BATCH_NOT_FOUND[0],
                                "description": ErrorCodes.BATCH_NOT_FOUND[1],
                                "subject": batch,
                            }
                        )

                        self.update_state(
                            state="PROGRESS",
                            meta={
                                "total": total,
                                "step": counter,
                                "errors": len(errors),
                            },
                        )
                        stop_timeout()
                        continue
                    imain.remove(batch_path, recursive=True)
                    stop_timeout()
                except BaseException as e:
                    log.error(e)
                    errors.append(
                        {
                            "error": ErrorCodes.UNEXPECTED_ERROR[0],
                            "description": ErrorCodes.UNEXPECTED_ERROR[1],
                            "subject": batch,
                        }
                    )
                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue

                if local_batch_path.is_dir():
                    rmtree(local_batch_path, ignore_errors=True)

            if len(errors) > 0:
                myjson["errors"] = errors
            ret = ext_api.post(myjson, backdoor=backdoor)
            log.info("CDI IM CALL = {}", ret)
    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
