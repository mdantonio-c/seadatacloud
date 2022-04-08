import logging
import os
import re
from pathlib import Path
from shutil import make_archive, rmtree
from typing import Any, Dict, List

from celery.app.task import Task
from plumbum import local  # type: ignore
from plumbum.commands.processes import ProcessExecutionError  # type: ignore
from restapi.connectors import redis
from restapi.connectors.celery import CeleryExt
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.connectors.b2handle import PIDgenerator, b2handle
from seadata.connectors.rabbit_queue import prepare_message
from seadata.endpoints import MOUNTPOINT, ORDERS_DIR, ErrorCodes
from seadata.tasks.seadata import MAX_ZIP_SIZE, ext_api, notify_error

TIMEOUT = 180

logging.getLogger("b2handle").setLevel(logging.WARNING)
b2handle_client = b2handle.instantiate_for_read_access()
pmaker = PIDgenerator()


@CeleryExt.task()
def unrestricted_order(
    self: Task,
    order_id: str,
    order_path: str,
    zip_file_name: str,
    myjson: Dict[str, Any],
) -> str:

    log.info("I'm {} (unrestricted_order)", self.request.id)

    params = myjson.get("parameters", {})
    backdoor = params.pop("backdoor", False)
    pids = params.get("pids", [])
    total = len(pids)
    self.update_state(
        state="STARTING",
        meta={"total": total, "step": 0, "errors": 0, "verified": 0},
    )

    ##################
    # SETUP
    local_dir = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
    local_zip_dir = local_dir.joinpath("tobezipped")
    local_zip_dir.mkdir(parents=True)

    r = redis.get_instance().r
    try:
        with irods.get_instance() as imain:

            log.info("Retrieving paths for {} PIDs", len(pids))
            ##################
            # Verify pids
            files: Dict[str, Path] = {}
            errors: List[Dict[str, str]] = []
            counter = 0
            verified = 0
            for pid in pids:

                ################
                # avoid empty pids?
                if "/" not in pid or len(pid) < 10:
                    continue

                ################
                # Check the cache first
                ifile = r.get(pid)
                if ifile is not None:
                    files[pid] = Path(ifile.decode())
                    verified += 1
                    self.update_state(
                        state="PROGRESS",
                        meta={
                            "total": total,
                            "step": counter,
                            "verified": verified,
                            "errors": len(errors),
                        },
                    )
                    continue

                # otherwise b2handle remotely
                try:
                    b2handle_output = b2handle_client.retrieve_handle_record(pid)
                except BaseException:
                    self.update_state(
                        state="FAILED",
                        meta={
                            "total": total,
                            "step": counter,
                            "verified": verified,
                            "errors": len(errors),
                        },
                    )
                    return notify_error(
                        ErrorCodes.B2HANDLE_ERROR, myjson, backdoor, self
                    )

                if b2handle_output is None:
                    errors.append(
                        {
                            "error": ErrorCodes.PID_NOT_FOUND[0],
                            "description": ErrorCodes.PID_NOT_FOUND[1],
                            "subject": pid,
                        }
                    )
                    self.update_state(
                        state="PROGRESS",
                        meta={"total": total, "step": counter, "errors": len(errors)},
                    )

                    log.warning("PID not found: {}", pid)
                else:
                    pid_path = pmaker.parse_pid_dataobject_path(b2handle_output)

                    if not pid_path:
                        log.error("Can't extract a PID from {}", b2handle_output)
                    else:
                        log.debug("PID verified: {}\n({})", pid, pid_path)
                        files[pid] = pid_path
                        r.set(pid, str(pid_path))
                        r.set(str(pid_path), pid)

                        verified += 1
                        self.update_state(
                            state="PROGRESS",
                            meta={
                                "total": total,
                                "step": counter,
                                "verified": verified,
                                "errors": len(errors),
                            },
                        )
            log.info("Retrieved paths for {} PIDs", len(files))

            # Recover files
            for pid, ipath in files.items():

                # Copy files from irods into a local TMPDIR
                filename = ipath.name
                local_file = local_zip_dir.joinpath(filename)

                if not local_file.exists() or local_file.stat().st_size == 0:
                    try:
                        start_timeout(TIMEOUT)
                        with imain.get_dataobject(ipath).open("r") as source:
                            with open(local_file, "wb") as target:
                                chunk_size = 10485760
                                while True:
                                    data = source.read(chunk_size)
                                    if not data:
                                        break
                                    target.write(data)
                        stop_timeout()
                    except BaseException as e:
                        log.error(e)
                        errors.append(
                            {
                                "error": ErrorCodes.UNABLE_TO_DOWNLOAD_FILE[0],
                                "description": ErrorCodes.UNABLE_TO_DOWNLOAD_FILE[1],
                                "subject_alt": filename,
                                "subject": pid,
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

                    # log.debug("Copy to local: {}", local_file)
                #########################
                #########################

                counter += 1
                if counter % 1000 == 0:
                    self.update_state(
                        state="PROGRESS",
                        meta={
                            "total": total,
                            "step": counter,
                            "verified": verified,
                            "errors": len(errors),
                        },
                    )
                    log.info("{} pids already processed", counter)
                # # Set current file to the metadata collection
                # if pid not in metadata:
                #     md = {pid: ipath}
                #     imain.set_metadata(order_path, **md)
                #     log.debug("Set metadata")

            zip_ipath = None
            if counter > 0:
                ##################
                # Zip the dir
                zip_local_file = local_dir.joinpath(zip_file_name)
                log.debug("Zip local path: {}", zip_local_file)
                if not zip_local_file.exists() or zip_local_file.stat().st_size == 0:
                    make_archive(
                        base_name=str(
                            zip_local_file.parent.joinpath(zip_local_file.stem)
                        ),
                        format="zip",
                        root_dir=local_zip_dir,
                    )

                    log.info("Compressed in: {}", zip_local_file)

                ##################
                # Copy the zip into irods
                zip_ipath = Path(order_path, zip_file_name)
                # NOTE: always overwrite
                try:
                    start_timeout(TIMEOUT)
                    imain.put(str(zip_local_file), str(zip_ipath))
                    log.info("Copied zip to irods: {}", zip_ipath)
                    stop_timeout()
                except BaseException as e:
                    log.error(e)
                    return notify_error(
                        ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self
                    )

                if os.path.getsize(str(zip_local_file)) > MAX_ZIP_SIZE:
                    log.warning("Zip too large, splitting {}", zip_local_file)

                    # Create a sub folder for split files. If already exists,
                    # remove it before to start from a clean environment
                    split_path = local_dir.joinpath("unrestricted_zip_split")
                    rmtree(str(split_path), ignore_errors=True)
                    split_path.mkdir()

                    # Execute the split of the whole zip
                    split_params = [
                        "-n",
                        MAX_ZIP_SIZE,
                        "-b",
                        str(split_path),
                        zip_local_file,
                    ]
                    try:
                        zipsplit = local["/usr/bin/zipsplit"]
                        zipsplit(split_params)
                    except ProcessExecutionError as e:

                        if "Entry is larger than max split size" in e.stdout:
                            reg = r"Entry too big to split, read, or write \((.*)\)"
                            extra = None
                            m = re.search(reg, e.stdout)
                            if m:
                                extra = m.group(1)
                            return notify_error(
                                ErrorCodes.ZIP_SPLIT_ENTRY_TOO_LARGE,
                                myjson,
                                backdoor,
                                self,
                                extra=extra,
                            )
                        else:
                            log.error(e.stdout)

                        return notify_error(
                            ErrorCodes.ZIP_SPLIT_ERROR,
                            myjson,
                            backdoor,
                            self,
                            extra=str(zip_local_file),
                        )

                    regexp = "^.*[^0-9]([0-9]+).zip$"
                    zip_files = os.listdir(split_path)
                    base_filename, _ = os.path.splitext(zip_file_name)
                    for subzip_file in zip_files:
                        m = re.search(regexp, subzip_file)
                        if not m:
                            log.error(
                                "Cannot extract index from zip name: {}",
                                subzip_file,
                            )
                            return notify_error(
                                ErrorCodes.INVALID_ZIP_SPLIT_OUTPUT,
                                myjson,
                                backdoor,
                                self,
                                extra=str(zip_local_file),
                            )
                        index = m.group(1).lstrip("0")
                        subzip_path = split_path.joinpath(subzip_file)

                        subzip_ifile = f"{base_filename}{index}.zip"
                        subzip_ipath = Path(order_path, subzip_ifile)

                        log.info("Uploading {} -> {}", subzip_path, subzip_ipath)
                        try:
                            start_timeout(TIMEOUT)
                            imain.put(str(subzip_path), str(subzip_ipath))
                            stop_timeout()
                        except BaseException as e:
                            log.error(e)
                            return notify_error(
                                ErrorCodes.UNEXPECTED_ERROR,
                                myjson,
                                backdoor,
                                self,
                                extra=str(subzip_path),
                            )

            #########################
            # NOTE: should I close the iRODS session ?
            #########################
            # imain.prc

            ##################
            # CDI notification
            reqkey = "request_id"
            msg = prepare_message(self, get_json=True)
            zipcount = 0
            if counter > 0:
                # FIXME: what about when restricted is there?
                zipcount += 1
            myjson["parameters"] = {
                # "request_id": msg['request_id'],
                reqkey: myjson[reqkey],
                "order_number": order_id,
                "zipfile_name": params["file_name"],
                "file_count": counter,
                "zipfile_count": zipcount,
            }
            for key, value in msg.items():
                if key == reqkey:
                    continue
                myjson[key] = value
            if len(errors) > 0:
                myjson["errors"] = errors
            myjson[reqkey] = self.request.id
            ret = ext_api.post(myjson, backdoor=backdoor)
            log.info("CDI IM CALL = {}", ret)

            ##################
            out = {
                "total": total,
                "step": counter,
                "verified": verified,
                "errors": len(errors),
                "zip": zip_ipath,
            }
            self.update_state(state="COMPLETED", meta=out)

    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
