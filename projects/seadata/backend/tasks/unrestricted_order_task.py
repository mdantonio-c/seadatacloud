import logging
import os
import re
from pathlib import Path
from shutil import copy, make_archive, rmtree
from typing import Any, Dict, List

from plumbum import local  # type: ignore
from plumbum.commands.processes import ProcessExecutionError  # type: ignore
from restapi.connectors import redis, sqlalchemy
from restapi.connectors.celery import CeleryExt, Task
from restapi.utilities.logs import log
from seadata.connectors.rabbit_queue import prepare_message
from seadata.endpoints import MOUNTPOINT, ORDERS_DIR, ErrorCodes
from seadata.tasks.seadata import MAX_ZIP_SIZE, ext_api, notify_error


@CeleryExt.task(idempotent=False)
def unrestricted_order(
    self: Task[[str, str, str, Dict[str, Any]], str],
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
    order_dir = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
    local_zip_dir = order_dir.joinpath("tobezipped")
    local_zip_dir.mkdir(parents=True, exist_ok=True)
    oversize_file_dir = local_zip_dir.parent.joinpath("oversize_files")

    r = redis.get_instance().r
    try:
        log.info("Retrieving paths for {} PIDs", len(pids))
        db = sqlalchemy.get_instance()
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
            file_path = r.get(pid)
            if file_path is not None:
                files[pid] = Path(file_path.decode())
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

            # otherwise check the local database
            dataobject = db.DataObject
            dataobject_entry = dataobject.query.filter(dataobject.uid == pid).first()

            if not dataobject_entry:
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
                pid_path = Path(dataobject_entry.path)

                if not pid_path:
                    log.error(
                        "Can't extract a path from the following record: ID: {}, PID: {}",
                        dataobject_entry.id,
                        dataobject_entry.uid,
                    )
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
        for pid, path in files.items():

            # Copy files from production into a local TMPDIR
            filename = path.name
            local_file = local_zip_dir.joinpath(filename)

            if not local_file.exists() or local_file.stat().st_size == 0:
                try:
                    copy(path, local_file)
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

        if counter > 0:
            split_path = order_dir.joinpath("unrestricted_zip_split")
            zip_path = order_dir.joinpath(zip_file_name)
            ##################
            # Zip the dir
            log.debug("Zip path: {}", zip_path)
            if not zip_path.exists() or zip_path.stat().st_size == 0:
                make_archive(
                    base_name=str(zip_path.parent.joinpath(zip_path.stem)),
                    format="zip",
                    root_dir=local_zip_dir,
                )

                log.info("Compressed in: {}", zip_path)

            ##################

            if os.path.getsize(str(zip_path)) > MAX_ZIP_SIZE:
                log.warning("Zip too large, splitting {}", zip_path)
                # check if there are oversize files
                # check if the file is bigger than the max zip size
                file_list = list(local_zip_dir.glob("*"))
                for file in file_list:
                    if file.stat().st_size >= MAX_ZIP_SIZE:
                        # check if an oversize directory exists, if not create it
                        if not oversize_file_dir.exists():
                            oversize_file_dir.mkdir()
                        # move the file in the oversize directory
                        file.rename(oversize_file_dir.joinpath(file.name))
                if (
                    oversize_file_dir.exists()
                    and len(list(oversize_file_dir.glob("*"))) > 0
                ):
                    oversize_number = len(list(oversize_file_dir.glob("*")))
                    log.info(f"{oversize_number} Oversize files found")
                    if len(list(local_zip_dir.glob("*"))) > 0:
                        zipfile_to_split = zip_path.parent.joinpath(
                            f"{zip_path.stem}_tmp.zip"
                        )
                        make_archive(
                            base_name=str(
                                zip_path.parent.joinpath(f"{zip_path.stem}_tmp")
                            ),
                            format="zip",
                            root_dir=local_zip_dir,
                        )

                        log.info("New zip to split compressed in: {}", zipfile_to_split)
                    else:
                        # all the files are in the oversize file dir so there is nothing to split
                        zipfile_to_split = None
                else:
                    zipfile_to_split = zip_path

                if zipfile_to_split:
                    # Create a sub folder for split files. If already exists,
                    # remove it before to start from a clean environment
                    rmtree(str(split_path), ignore_errors=True)
                    split_path.mkdir()

                    # Execute the split of the whole zip
                    split_params = [
                        "-n",
                        MAX_ZIP_SIZE,
                        "-b",
                        str(split_path),
                        zipfile_to_split,
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
                            extra=str(zip_path),
                        )
            # if there are oversize files zip them
            if (
                oversize_file_dir.exists()
                and len(list(oversize_file_dir.glob("*"))) > 0
            ):
                # check if zip split directory exists, if not create it
                if not split_path.exists():
                    split_path.mkdir()
                    zipsplit_index = 1
                else:
                    zipsplit_index = len(list(split_path.glob("*"))) + 1
                oversize_list = list(oversize_file_dir.glob("*"))
                for f in oversize_list:
                    tmp_dir = oversize_file_dir.joinpath("tmp")
                    tmp_dir.mkdir(exist_ok=True)
                    # make_archive can't create an archive from file (only from a folder)
                    tmp_file = tmp_dir.joinpath(f.name)
                    f.rename(tmp_file)
                    make_archive(
                        base_name=str(
                            split_path.joinpath(f"oversize{str(zipsplit_index)}")
                        ),
                        format="zip",
                        root_dir=tmp_dir,
                    )
                    # move back the file on the oversize_cache cache
                    tmp_file.rename(f)
                    zipsplit_index += 1

                    rmtree(tmp_dir)

                # to save space, delete the folder where oversize files were stored
                rmtree(str(oversize_file_dir), ignore_errors=True)

            subzip_counter = 0
            # if there are splitted zips or oversize zips rename them
            if len(list(split_path.glob("*"))) > 0:
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
                            extra=str(zip_path),
                        )
                    index = m.group(1).lstrip("0")
                    subzip_path = split_path.joinpath(subzip_file)

                    subzip_order_file = f"{base_filename}{index}.zip"
                    subzip_order_path = Path(order_path, subzip_order_file)

                    # TODO: the unzipped file has to be maintained anyway?
                    # log.info("Delete the not zipped path at {}",zip_path)
                    # zip_path.unlink()
                    log.info("Copying {} -> {}", subzip_path, subzip_order_path)
                    try:
                        copy(subzip_path, subzip_order_path)
                    except BaseException as e:
                        log.error(e)
                        return notify_error(
                            ErrorCodes.UNEXPECTED_ERROR,
                            myjson,
                            backdoor,
                            self,
                            extra=str(subzip_path),
                        )
                subzip_counter += 1

                # to save space, delete the folder where splitted files were stored
                rmtree(str(split_path), ignore_errors=True)

        # to save space delete the folder when temp files were stored
        log.info(
            "Deleting the temp dir where unzipped data are stored: Path '{}'",
            local_zip_dir,
        )
        rmtree(str(local_zip_dir), ignore_errors=True)

        # check if temporary files are present and delete them
        temp_zipfile = zip_path.parent.joinpath(f"{zip_path.stem}_tmp.zip")
        if temp_zipfile.exists():
            temp_zipfile.unlink()

        # CDI notification
        reqkey = "request_id"
        msg = prepare_message(self, get_json=True)
        zipcount = 0
        if subzip_counter > 0:
            zipcount = subzip_counter
        elif counter > 0:
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
            "zip": str(zip_path),
        }
        self.update_state(state="COMPLETED", meta=out)

    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
