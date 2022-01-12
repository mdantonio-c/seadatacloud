from typing import Any, Dict

from celery.app.task import Task
from restapi.connectors.celery import CeleryExt
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.endpoints import ErrorCodes
from seadata.tasks.seadata import ext_api, notify_error

TIMEOUT = 180


@CeleryExt.task(idempotent=False)
def list_resources(
    self: Task, batch_path: str, order_path: str, myjson: Dict[str, Any]
) -> str:

    try:
        with irods.get_instance() as imain:

            param_key = "parameters"

            if param_key not in myjson:
                myjson[param_key] = {}

            myjson[param_key]["request_id"] = myjson["request_id"]
            myjson["request_id"] = self.request.id

            params = myjson.get(param_key, {})
            backdoor = params.pop("backdoor", False)

            if param_key not in myjson:
                myjson[param_key] = {}

            myjson[param_key]["batches"] = []
            try:
                start_timeout(TIMEOUT)
                batches = imain.list(batch_path)
                for n in batches:
                    myjson[param_key]["batches"].append(n)

                myjson[param_key]["orders"] = []
                orders = imain.list(order_path)
                for n in orders:
                    myjson[param_key]["orders"].append(n)

                ret = ext_api.post(myjson, backdoor=backdoor)
                log.info("CDI IM CALL = {}", ret)
                stop_timeout()
            except BaseException as e:
                log.error(e)
                return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

            return "COMPLETED"
    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
