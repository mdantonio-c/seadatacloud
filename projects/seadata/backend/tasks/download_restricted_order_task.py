import hashlib
import os
import re
import zipfile
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, List, Optional, Tuple

import requests
from plumbum import local  # type: ignore
from plumbum.commands.processes import ProcessExecutionError  # type: ignore
from restapi.connectors.celery import CeleryExt, Task
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.connectors.irods import IrodsException
from seadata.endpoints import MOUNTPOINT, ORDERS_DIR, ErrorCodes
from seadata.tasks.seadata import MAX_ZIP_SIZE, ext_api, notify_error

TIMEOUT = 1800

DOWNLOAD_HEADERS = {
    "User-Agent": "SDC CDI HTTP-APIs",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}


def check_params(params: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    params_to_check = [
        "order_number",
        "download_path",
        "zipfile_name",
        "file_name",
        "file_size",
        "data_file_count",
        "file_checksum",
    ]
    errors = [
        ErrorCodes.MISSING_ORDER_NUMBER_PARAM,
        ErrorCodes.MISSING_DOWNLOAD_PATH_PARAM,
        ErrorCodes.MISSING_ZIPFILENAME_PARAM,
        ErrorCodes.MISSING_FILENAME_PARAM,
        ErrorCodes.MISSING_FILESIZE_PARAM,
        ErrorCodes.MISSING_FILECOUNT_PARAM,
        ErrorCodes.MISSING_CHECKSUM_PARAM,
    ]

    for p in params_to_check:
        param_value = params.get(p)
        if param_value is None:
            list_index = params_to_check.index(p)
            return errors[list_index]
        if p == "download_path" and param_value == "":
            return ErrorCodes.EMPTY_DOWNLOAD_PATH_PARAM

    return None


@CeleryExt.task(idempotent=False)
def download_restricted_order(
    self: Task[[str, str, Dict[str, Any]], str],
    order_id: str,
    order_path: str,
    myjson: Dict[str, Any],
) -> str:

    myjson["parameters"]["request_id"] = myjson["request_id"]
    myjson["request_id"] = self.request.id

    params = myjson.get("parameters", {})

    backdoor = params.pop("backdoor", False)
    request_edmo_code = myjson.get("edmo_code", None)

    # Make sure you have a path with no trailing slash
    order_path = order_path.rstrip("/")

    try:
        with irods.get_instance() as imain:
            if not imain.is_collection(order_path):
                return notify_error(
                    ErrorCodes.ORDER_NOT_FOUND,
                    myjson,
                    backdoor,
                    self,
                    edmo_code=request_edmo_code,
                )

            params_error = check_params(params)
            if params_error:
                return notify_error(
                    params_error,
                    myjson,
                    backdoor,
                    self,
                    edmo_code=request_edmo_code,
                )

            # order_number = params.get("order_number")

            # check if order_numer == order_id ?

            download_path = params.get("download_path")
            # NAME OF FINAL ZIP
            filename = params.get("zipfile_name")

            base_filename = filename
            if filename.endswith(".zip"):
                log.warning("{} already contains extention .zip", filename)
                # TO DO: save base_filename as filename - .zip
            else:
                filename += ".zip"

            final_zip = Path(order_path, filename.rstrip("/"))

            log.info("order_id = {}", order_id)
            log.info("order_path = {}", order_path)
            log.info("final_zip = {}", final_zip)

            # ############################################

            # INPUT PARAMETERS CHECKS

            # zip file uploaded from partner
            file_name = params.get("file_name")
            file_size = params.get("file_size")

            try:
                int(file_size)
            except BaseException:
                return notify_error(
                    ErrorCodes.INVALID_FILESIZE_PARAM,
                    myjson,
                    backdoor,
                    self,
                    edmo_code=request_edmo_code,
                )

            file_count = params.get("data_file_count")

            try:
                int(file_count)
            except BaseException:
                return notify_error(
                    ErrorCodes.INVALID_FILECOUNT_PARAM,
                    myjson,
                    backdoor,
                    self,
                    edmo_code=request_edmo_code,
                )

            file_checksum = params.get("file_checksum")

            self.update_state(state="PROGRESS")

            errors: List[Dict[str, str]] = []
            local_finalzip_path = None
            log.info("Merging zip file", file_name)

            if not file_name.endswith(".zip"):
                file_name += ".zip"

            # 1 - download in local-dir
            download_url = os.path.join(download_path, file_name)
            log.info("Downloading file from {}", download_url)
            try:
                r = requests.get(
                    download_url,
                    stream=True,
                    verify=False,
                    headers=DOWNLOAD_HEADERS,
                    timeout=120,
                )
            except requests.exceptions.ConnectionError:
                return notify_error(
                    ErrorCodes.UNREACHABLE_DOWNLOAD_PATH,
                    myjson,
                    backdoor,
                    self,
                    subject=download_url,
                    edmo_code=request_edmo_code,
                )
            except requests.exceptions.MissingSchema as e:
                log.error(str(e))
                return notify_error(
                    ErrorCodes.UNREACHABLE_DOWNLOAD_PATH,
                    myjson,
                    backdoor,
                    self,
                    subject=download_url,
                    edmo_code=request_edmo_code,
                )

            if r.status_code != 200:

                return notify_error(
                    ErrorCodes.UNREACHABLE_DOWNLOAD_PATH,
                    myjson,
                    backdoor,
                    self,
                    subject=download_url,
                    edmo_code=request_edmo_code,
                )

            log.info("Request status = {}", r.status_code)

            local_dir = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
            local_dir.mkdir(exist_ok=True)
            log.info("Local dir = {}", local_dir)

            local_zip_path = local_dir.joinpath(file_name)
            log.info("partial_zip = {}", local_zip_path)

            with open(local_zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

            # 2 - verify checksum
            log.info("Computing checksum for {}...", local_zip_path)
            local_file_checksum = hashlib.md5(
                open(local_zip_path, "rb").read()
            ).hexdigest()

            if local_file_checksum.lower() != file_checksum.lower():
                return notify_error(
                    ErrorCodes.CHECKSUM_DOESNT_MATCH,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )
            log.info("File checksum verified for {}", local_zip_path)

            # 3 - verify size
            local_file_size = os.path.getsize(str(local_zip_path))
            if local_file_size != int(file_size):
                log.error(
                    "File size {} for {}, expected {}",
                    local_file_size,
                    local_zip_path,
                    file_size,
                )
                return notify_error(
                    ErrorCodes.FILESIZE_DOESNT_MATCH,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )

            log.info("File size verified for {}", local_zip_path)

            # 4 - decompress
            d = os.path.splitext(os.path.basename(str(local_zip_path)))[0]
            local_unzipdir = local_dir.joinpath(d)

            if local_unzipdir.exists():
                log.warning("{} already exist, removing it", local_unzipdir)
                rmtree(local_unzipdir, ignore_errors=True)

            local_unzipdir.mkdir()
            log.info("Local unzip dir = {}", local_unzipdir)

            log.info("Unzipping {}", local_zip_path)
            zip_ref = None
            try:
                zip_ref = zipfile.ZipFile(local_zip_path, "r")
            except FileNotFoundError:
                return notify_error(
                    ErrorCodes.UNZIP_ERROR_FILE_NOT_FOUND,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )

            except zipfile.BadZipFile:
                return notify_error(
                    ErrorCodes.UNZIP_ERROR_INVALID_FILE,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )

            if zip_ref is not None:
                zip_ref.extractall(str(local_unzipdir))
                zip_ref.close()

            # 5 - verify num files?
            local_file_count = len(os.listdir(str(local_unzipdir)))

            log.info("Unzipped {} files from {}", local_file_count, local_zip_path)

            if local_file_count != int(file_count):
                log.error("Expected {} files for {}", file_count, local_zip_path)
                return notify_error(
                    ErrorCodes.UNZIP_ERROR_WRONG_FILECOUNT,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )

            log.info("File count verified for {}", local_zip_path)

            log.info("Verifying final zip: {}", final_zip)
            # 6 - check if final_zip exists
            if not imain.exists(str(final_zip)):
                # 7 - if not, simply copy partial_zip -> final_zip
                log.info("Final zip does not exist, copying partial zip")
                try:
                    start_timeout(TIMEOUT)
                    imain.put(str(local_zip_path), str(final_zip))
                    stop_timeout()
                except IrodsException as e:
                    log.error(str(e))
                    return notify_error(
                        ErrorCodes.B2SAFE_UPLOAD_ERROR,
                        myjson,
                        backdoor,
                        self,
                        subject=file_name,
                        edmo_code=request_edmo_code,
                    )
                except BaseException as e:
                    log.error(e)
                    return notify_error(
                        ErrorCodes.UNEXPECTED_ERROR,
                        myjson,
                        backdoor,
                        self,
                        subject=file_name,
                        edmo_code=request_edmo_code,
                    )
                local_finalzip_path = local_zip_path
            else:
                # 8 - if already exists merge zips
                log.info("Already exists, merge zip files")

                log.info("Copying zipfile locally")
                local_finalzip_path = local_dir.joinpath(final_zip.name)
                try:
                    start_timeout(TIMEOUT)
                    imain.open(str(final_zip), str(local_finalzip_path))
                    stop_timeout()
                except BaseException as e:
                    log.error(e)
                    return notify_error(
                        ErrorCodes.UNEXPECTED_ERROR,
                        myjson,
                        backdoor,
                        self,
                        subject=final_zip,
                        edmo_code=request_edmo_code,
                    )

                log.info("Reading local zipfile")
                zip_ref = None
                try:
                    zip_ref = zipfile.ZipFile(local_finalzip_path, "a")
                except FileNotFoundError:
                    log.error("Local file not found: {}", local_finalzip_path)
                    return notify_error(
                        ErrorCodes.UNZIP_ERROR_FILE_NOT_FOUND,
                        myjson,
                        backdoor,
                        self,
                        subject=final_zip,
                        edmo_code=request_edmo_code,
                    )

                except zipfile.BadZipFile:
                    log.error("Invalid local file: {}", local_finalzip_path)
                    return notify_error(
                        ErrorCodes.UNZIP_ERROR_INVALID_FILE,
                        myjson,
                        backdoor,
                        self,
                        subject=final_zip,
                        edmo_code=request_edmo_code,
                    )

                log.info("Adding files to local zipfile")
                if zip_ref is not None:
                    try:
                        for loc_file in os.listdir(str(local_unzipdir)):
                            # log.debug("Adding {}", loc_file)
                            zip_ref.write(
                                os.path.join(str(local_unzipdir), loc_file), loc_file
                            )
                        zip_ref.close()
                    except BaseException as e:
                        log.error(e)
                        return notify_error(
                            ErrorCodes.UNABLE_TO_CREATE_ZIP_FILE,
                            myjson,
                            backdoor,
                            self,
                            subject=final_zip,
                            edmo_code=request_edmo_code,
                        )

                log.info("Creating a backup copy of final zip")
                try:
                    start_timeout(TIMEOUT)
                    backup_zip = final_zip.with_suffix(".bak")
                    if imain.is_dataobject(backup_zip):
                        log.info(
                            "{} already exists, removing previous backup",
                            backup_zip,
                        )
                        imain.remove(backup_zip)
                    imain.move(final_zip, backup_zip)

                    log.info("Uploading final updated zip")
                    imain.put(str(local_finalzip_path), str(final_zip))
                    stop_timeout()
                except BaseException as e:
                    log.error(e)
                    return notify_error(
                        ErrorCodes.UNEXPECTED_ERROR,
                        myjson,
                        backdoor,
                        self,
                        subject=final_zip,
                        edmo_code=request_edmo_code,
                    )

                # imain.remove(local_zip_path)
            rmtree(local_unzipdir, ignore_errors=True)

            self.update_state(state="COMPLETED")

            if os.path.getsize(str(local_finalzip_path)) > MAX_ZIP_SIZE:
                log.warning("Zip too large, splitting {}", local_finalzip_path)

                # Create a sub folder for split files. If already exists,
                # remove it before to start from a clean environment
                split_path = Path(local_dir, "restricted_zip_split")
                # split_path is an object
                rmtree(split_path, ignore_errors=True)
                # path create requires a path object
                split_path.mkdir()

                # Execute the split of the whole zip
                split_params = [
                    "-n",
                    MAX_ZIP_SIZE,
                    "-b",
                    str(split_path),
                    local_finalzip_path,
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
                            edmo_code=request_edmo_code,
                        )
                    else:
                        log.error(e.stdout)

                    return notify_error(
                        ErrorCodes.ZIP_SPLIT_ERROR,
                        myjson,
                        backdoor,
                        self,
                        extra=str(local_finalzip_path),
                        edmo_code=request_edmo_code,
                    )

                regexp = "^.*[^0-9]([0-9]+).zip$"
                zip_files = os.listdir(split_path)
                for subzip_file in zip_files:
                    m = re.search(regexp, subzip_file)
                    if not m:
                        log.error("Cannot extract index from zip name: {}", subzip_file)
                        return notify_error(
                            ErrorCodes.INVALID_ZIP_SPLIT_OUTPUT,
                            myjson,
                            backdoor,
                            self,
                            extra=str(local_finalzip_path),
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
                            subject=subzip_path,
                            edmo_code=request_edmo_code,
                        )

            if len(errors) > 0:
                myjson["errors"] = errors

            ret = ext_api.post(myjson, backdoor=backdoor, edmo_code=request_edmo_code)
            log.info("CDI IM CALL = {}", ret)
    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
