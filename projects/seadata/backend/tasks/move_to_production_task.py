import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List

from restapi.connectors import redis, sqlalchemy
from restapi.connectors.celery import CeleryExt, Task
from restapi.exceptions import DatabaseDuplicatedEntry
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors.rabbit_queue import prepare_message
from seadata.endpoints import INGESTION_DIR, MOUNTPOINT, ErrorCodes
from seadata.endpoints import Metadata as md
from seadata.exceptions import DatabaseEntryNotFound
from seadata.tasks.seadata import ext_api, generate_uid, notify_error


@CeleryExt.task(idempotent=False)
def move_to_production_task(
    self: Task[[str, str, str, Dict[str, Any]], str],
    batch_id: str,
    batch_path: str,
    cloud_path: str,
    myjson: Dict[str, Any],
) -> str:

    self.update_state(state="STARTING", meta={"total": None, "step": 0, "errors": 0})

    ###############
    log.info("I'm {} (move_to_production_task)!", self.request.id)

    try:
        db = sqlalchemy.get_instance()
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
            return notify_error(ErrorCodes.MISSING_PIDS_LIST, myjson, backdoor, self)
        MAX_RETRIES = 5

        r = redis.get_instance().r

        for element in elements:

            temp_id = element.get("temp_id")  # do not pop
            record_id = element.get("format_n_code")
            element_to_ingest = Path(batch_path).joinpath(temp_id)
            current_file_name = element_to_ingest.name

            # [fs -> irods]
            # Exists and has size greater than zero
            if element_to_ingest.exists() and element_to_ingest.stat().st_size > 0:
                log.info("Found: {}", element_to_ingest)
            else:
                log.error("NOT found: {}", element_to_ingest)
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
            # 1. copy file to the production directory
            ingested_file = Path(cloud_path, current_file_name)
            try:
                shutil.copy(element_to_ingest, ingested_file)
                log.info("File copied to production dir: {}", ingested_file)
            except BaseException as e:
                log.error(e)
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
            # 2. request pid
            try:
                if backdoor:
                    log.warning("Backdoor enabled: skipping PID request")
                    PID = "NO_PID_WITH_BACKDOOR"
                else:
                    for i in range(MAX_RETRIES):
                        try:
                            PID = generate_uid()
                            # save the correlation PID/path retry if the PID is not unique
                            data_object = db.DataObject(
                                uid=PID, path=str(ingested_file)
                            )
                            db.session.add(data_object)
                            db.session.commit()
                            break
                        except DatabaseDuplicatedEntry as e:
                            log.error(e)
                            db.session.rollback()
                            continue
                    else:
                        # failed upload for the file
                        errors.append(
                            {
                                "error": ErrorCodes.UNABLE_TO_ASSIGN_PID[0],
                                "description": ErrorCodes.UNABLE_TO_ASSIGN_PID[1],
                                "subject": record_id,
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
                        continue

                    # # save inside the cache
                    r.set(PID, str(ingested_file))
                    r.set(str(ingested_file), PID)
                    log.debug("PID cache updated")
                log.info("PID: {}", PID)

            except BaseException as e:
                log.error(e)

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
            # 3 set metadata
            try:
                content = {}
                for key in md.keys:
                    value = element.get(key, "***MISSING***")
                    content[key] = value
                content["PID"] = PID

                # get the dataobject entry from the db
                if not backdoor:
                    dataobject = db.DataObject
                    dataobject_entry = dataobject.query.filter(
                        dataobject.uid == PID
                    ).first()
                    if not dataobject_entry:
                        raise DatabaseEntryNotFound(
                            f"not found an entry in database for PID {PID} in local database"
                        )

                    dataobject_entry.object_metadata = content
                    # save the metadata in db
                    db.session.commit()
                    log.debug("Metadata saved for object with uid {}", PID)
            except (BaseException, DatabaseEntryNotFound) as e:
                log.error(e)
                # failed metadata setting
                # rollback the database
                db.session.rollback()
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
