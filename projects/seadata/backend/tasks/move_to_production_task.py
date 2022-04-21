import json
import time
from pathlib import Path
from typing import Any, Dict, List

from celery.app.task import Task
from restapi.connectors import redis
from restapi.connectors.celery import CeleryExt
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.connectors.b2handle import PIDgenerator
from seadata.connectors.rabbit_queue import prepare_message
from seadata.endpoints import INGESTION_DIR, MOUNTPOINT, ErrorCodes
from seadata.endpoints import Metadata as md
from seadata.tasks.seadata import ext_api, notify_error

pmaker = PIDgenerator()

TIMEOUT = 1800


@CeleryExt.task()
def move_to_production_task(
    self: Task, batch_id: str, batch_path: str, cloud_path: str, myjson: Dict[str, Any]
) -> str:

    self.update_state(state="STARTING", meta={"total": None, "step": 0, "errors": 0})

    ###############
    log.info("I'm {} (move_to_production_task)!", self.request.id)
    local_path = MOUNTPOINT.joinpath(INGESTION_DIR, batch_id)

    try:
        with irods.get_instance() as imain:

            out_data = []
            errors: List[Dict[str, str]] = []
            counter = 0
            param_key = "parameters"
            params = myjson.get(param_key, {})
            elements = params.get("pids", {})
            backdoor = params.pop("backdoor", False)
            total = len(elements)
            self.update_state(
                state="PROGRESS",
                meta={"total": total, "step": counter, "errors": len(errors)},
            )

            if elements is None:
                return notify_error(
                    ErrorCodes.MISSING_PIDS_LIST, myjson, backdoor, self
                )

            MAX_RETRIES = 5
            SLEEP_TIME = 120

            r = redis.get_instance().r

            for element in elements:

                temp_id = element.get("temp_id")  # do not pop
                record_id = element.get("format_n_code")
                local_element = local_path.joinpath(temp_id)
                # it is not equal to temp_id !?
                current_file_name = local_element.name

                # [fs -> irods]
                # Exists and has size greater than zero
                if local_element.exists() and local_element.stat().st_size > 0:
                    log.info("Found: {}", local_element)
                else:
                    log.error("NOT found: {}", local_element)
                    errors.append(
                        {
                            "error": ErrorCodes.INGESTION_FILE_NOT_FOUND[0],
                            "description": ErrorCodes.INGESTION_FILE_NOT_FOUND[1],
                            "subject": record_id,
                        }
                    )

                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue

                ###############
                # 1. copy file (irods) [fs -> irods]
                ifile = str(Path(cloud_path, current_file_name))
                for i in range(MAX_RETRIES):
                    try:
                        start_timeout(TIMEOUT)
                        imain.put(str(local_element), str(ifile))
                        log.info("File copied on irods: {}", ifile)
                        stop_timeout()
                        break
                    except BaseException as e:
                        log.error(e)
                        time.sleep(SLEEP_TIME)
                        continue
                else:
                    # failed upload for the file
                    errors.append(
                        {
                            "error": ErrorCodes.UNABLE_TO_MOVE_IN_PRODUCTION[0],
                            "description": ErrorCodes.UNABLE_TO_MOVE_IN_PRODUCTION[1],
                            "subject": record_id,
                        }
                    )

                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue

                ###############
                # 2. request pid (irule)
                for i in range(MAX_RETRIES):
                    try:
                        start_timeout(TIMEOUT)
                        if backdoor:
                            log.warning("Backdoor enabled: skipping PID request")
                            PID = "NO_PID_WITH_BACKDOOR"
                        else:
                            PID = pmaker.pid_request(imain, ifile)
                        log.info("PID: {}", PID)
                        # # save inside the cache
                        r.set(PID, ifile)
                        r.set(ifile, PID)
                        log.debug("PID cache updated")
                        stop_timeout()
                        break
                    except BaseException as e:
                        log.error(e)
                        time.sleep(SLEEP_TIME)
                        continue

                else:
                    # failed PID assignment
                    errors.append(
                        {
                            "error": ErrorCodes.UNABLE_TO_ASSIGN_PID[0],
                            "description": ErrorCodes.UNABLE_TO_ASSIGN_PID[1],
                            "subject": record_id,
                        }
                    )

                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue

                ###############
                # 3. set metadata (icat)
                # Remove me in a near future
                for i in range(MAX_RETRIES):
                    try:
                        start_timeout(TIMEOUT)
                        metadata = imain.get_metadata(ifile)

                        for key in md.keys:
                            if key not in metadata:
                                value = element.get(key, "***MISSING***")
                                args = {"path": ifile, key: value}
                                imain.set_metadata(**args)
                        log.debug("Metadata set for {}", current_file_name)
                        stop_timeout()
                        break
                    except BaseException as e:
                        log.error(e)
                        time.sleep(SLEEP_TIME)
                        continue
                else:
                    # failed metadata setting
                    errors.append(
                        {
                            "error": ErrorCodes.UNABLE_TO_SET_METADATA[0],
                            "description": ErrorCodes.UNABLE_TO_SET_METADATA[1],
                            "subject": record_id,
                        }
                    )

                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue

                ###############
                # 3-bis. set metadata (dataobject)
                for i in range(MAX_RETRIES):
                    try:
                        start_timeout(TIMEOUT)
                        content = {}
                        for key in md.keys:
                            value = element.get(key, "***MISSING***")
                            content[key] = value
                        content["PID"] = PID

                        metadata_file = ifile + ".meta"
                        imain.create_empty(metadata_file, ignore_existing=True)
                        imain.write_file_content(metadata_file, json.dumps(content))
                        log.debug("Metadata dumped in {}", metadata_file)
                        stop_timeout()
                        break
                    except BaseException as e:
                        log.error(e)
                        time.sleep(SLEEP_TIME)
                        continue
                else:
                    # failed metadata setting
                    errors.append(
                        {
                            "error": ErrorCodes.UNABLE_TO_SET_METADATA[0],
                            "description": ErrorCodes.UNABLE_TO_SET_METADATA[1],
                            "subject": record_id,
                        }
                    )

                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )
                    continue
                ###############
                # 4. remove the batch file?
                # or move it into a "completed/" folder
                # where we can check if it was already done?

                ###############
                # 5. add to logs
                element["pid"] = PID
                out_data.append(element)

                counter += 1
                self.update_state(
                    state="PROGRESS",
                    meta={"total": total, "step": counter, "errors": len(errors)},
                )

            ###############
            # Notify the CDI API
            myjson[param_key]["pids"] = out_data
            msg = prepare_message(self, get_json=True)
            for key, value in msg.items():
                myjson[key] = value
            if len(errors) > 0:
                myjson["errors"] = errors
            ret = ext_api.post(myjson, backdoor=backdoor)
            log.info("CDI IM CALL = {}", ret)

            out = {
                "total": total,
                "step": counter,
                "errors": len(errors),
                "out": out_data,
            }
            self.update_state(state="COMPLETED", meta=out)
    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
