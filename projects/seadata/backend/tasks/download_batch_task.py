import hashlib
import zipfile
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict
from urllib.parse import urljoin

import requests
from restapi.connectors.celery import CeleryExt, Task
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods
from seadata.endpoints import ErrorCodes
from seadata.tasks.seadata import ext_api, notify_error

TIMEOUT = 1800

DOWNLOAD_HEADERS = {
    "User-Agent": "SDC CDI HTTP-APIs",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}


@CeleryExt.task(idempotent=False)
def download_batch(
    self: Task[[str, str, Dict[str, Any]], str],
    batch_path: str,
    local_path: str,
    myjson: Dict[str, Any],
) -> str:

    log.info("I'm {} (download_batch)", self.request.id)
    log.info("Batch irods path: {}", batch_path)
    log.info("Batch local path: {}", local_path)

    params = myjson.get("parameters", {})
    request_edmo_code = myjson.get("edmo_code", None)
    if type(params) != dict:
        return notify_error(
            ErrorCodes.MISSING_BATCHES_PARAMETER,
            myjson,
            False,
            self,
            edmo_code=request_edmo_code,
        )
    backdoor = params.pop("backdoor", False)

    batch_number = params.get("batch_number")
    if batch_number is None:
        return notify_error(
            ErrorCodes.MISSING_BATCH_NUMBER_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

    download_path = params.get("download_path")
    if download_path is None:
        return notify_error(
            ErrorCodes.MISSING_DOWNLOAD_PATH_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )
    if download_path == "":
        return notify_error(
            ErrorCodes.EMPTY_DOWNLOAD_PATH_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

    file_count = params.get("data_file_count")
    if file_count is None:
        return notify_error(
            ErrorCodes.MISSING_FILECOUNT_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

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

    file_name = params.get("file_name")
    if file_name is None:
        return notify_error(
            ErrorCodes.MISSING_FILENAME_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

    file_size = params.get("file_size")
    if file_size is None:
        return notify_error(
            ErrorCodes.MISSING_FILESIZE_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

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

    file_checksum = params.get("file_checksum")
    if file_checksum is None:
        return notify_error(
            ErrorCodes.MISSING_CHECKSUM_PARAM,
            myjson,
            backdoor,
            self,
            edmo_code=request_edmo_code,
        )

    try:
        with irods.get_instance() as imain:
            if not imain.is_collection(batch_path):
                return notify_error(
                    ErrorCodes.BATCH_NOT_FOUND,
                    myjson,
                    backdoor,
                    self,
                    edmo_code=request_edmo_code,
                )

            # 1 - download the file
            download_url = urljoin(download_path, file_name)
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
            batch_file = Path(local_path, file_name)

            with open(batch_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

            # 2 - verify checksum
            log.info("Computing checksum for {}...", batch_file)
            local_file_checksum = hashlib.md5(open(batch_file, "rb").read()).hexdigest()

            if local_file_checksum.lower() != file_checksum.lower():
                return notify_error(
                    ErrorCodes.CHECKSUM_DOESNT_MATCH,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )
            log.info("File checksum verified for {}", batch_file)

            # 3 - verify size
            local_file_size = batch_file.stat().st_size
            if local_file_size != int(file_size):
                log.error(
                    "File size {} for {}, expected {}",
                    local_file_size,
                    batch_file,
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

            log.info("File size verified for {}", batch_file)

            # 4 - decompress
            local_unzipdir = Path(local_path, batch_file.stem)

            if local_unzipdir.is_dir():
                log.warning("{} already exist, removing it", local_unzipdir)
                rmtree(local_unzipdir, ignore_errors=True)

            local_unzipdir.mkdir()
            log.info("Local unzip dir = {}", local_unzipdir)

            log.info("Unzipping {}", batch_file)
            zip_ref = None
            try:
                zip_ref = zipfile.ZipFile(batch_file, "r")
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
                zip_ref.extractall(local_unzipdir)
                zip_ref.close()

            # 6 - verify num files?
            local_file_count = len(list(local_unzipdir.iterdir()))

            log.info("Unzipped {} files from {}", local_file_count, batch_file)

            if local_file_count != int(file_count):
                log.error("Expected {} files for {}", file_count, batch_file)
                return notify_error(
                    ErrorCodes.UNZIP_ERROR_WRONG_FILECOUNT,
                    myjson,
                    backdoor,
                    self,
                    subject=file_name,
                    edmo_code=request_edmo_code,
                )

            log.info("File count verified for {}", batch_file)

            rmtree(local_unzipdir, ignore_errors=True)

            # 7 - copy file from B2HOST filesystem to irods

            """
            The data is copied from the path on the
            local filesystem inside the celery worker
            container (/usr/share/batches/<batch_id>),
            which is a directory mounted from the host,
            to the irods_path (usually /myzone/batches/<batch_id>)
            """

            irods_batch_file = Path(batch_path, file_name)
            log.debug("Copying {} into {}...", batch_file, irods_batch_file)

            try:
                start_timeout(TIMEOUT)
                imain.put(str(batch_file), str(irods_batch_file))
                stop_timeout()
            except BaseException as e:
                log.error(e)
                return notify_error(
                    ErrorCodes.UNEXPECTED_ERROR,
                    myjson,
                    backdoor,
                    self,
                    subject=batch_file,
                    edmo_code=request_edmo_code,
                )

            # NOTE: permissions are inherited thanks to the ACL already SET
            # Not needed to set ownership to username
            log.info("Copied: {}", irods_batch_file)

            ret = ext_api.post(myjson, backdoor=backdoor, edmo_code=request_edmo_code)
            log.info("CDI IM CALL = {}", ret)
            return "COMPLETED"

    except BaseException as e:
        log.error(e)
        log.error(type(e))
        return notify_error(ErrorCodes.UNEXPECTED_ERROR, myjson, backdoor, self)

    return "ok"
