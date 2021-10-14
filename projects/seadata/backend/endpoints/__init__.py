import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import pytz
import requests
from restapi.config import PRODUCTION
from restapi.connectors import sqlalchemy
from restapi.env import Env
from restapi.models import Schema, fields
from restapi.rest.definition import EndpointResource, Response, ResponseContent
from restapi.utilities.logs import log
from seadata.connectors import irods
from webargs import fields as webargs_fields

seadata_vars = Env.load_variables_group(prefix="seadata")

MISSING_BATCH = 0
NOT_FILLED_BATCH = 1
PARTIALLY_ENABLED_BATCH = 2
ENABLED_BATCH = 3
BATCH_MISCONFIGURATION = 4


DEFAULT_IMAGE_PREFIX = "docker"

"""
These are the names of the directories in the irods
zone for ingestion (i.e. pre-production) batches,
for production batches, and for orders being prepared.

They are being defined project_configuration.yml / .projectrc
"""
INGESTION_COLL = seadata_vars.get("ingestion_coll") or "batches"
ORDERS_COLL = seadata_vars.get("orders_coll") or "orders"
PRODUCTION_COLL = seadata_vars.get("production_coll") or "cloud"
MOUNTPOINT = seadata_vars.get("resources_mountpoint") or "/usr/share"

"""
These are the paths to the data on the hosts
that runs containers (both backend, celery and QC containers)
"""
INGESTION_DIR = seadata_vars.get("workspace_ingestion") or "batches"
ORDERS_DIR = seadata_vars.get("workspace_orders") or "orders"

"""
These are how the paths to the data on the host
are mounted into the containers.

Prepended before this is the RESOURCES_LOCALPATH,
defaulting to /usr/share.
"""

# THIS CANNOT CHANGE, otherwise QC containers will not work anymore!
FS_PATH_IN_CONTAINER = "/usr/share/batch"
# At least, the 'batch' part has to be like this, I am quite sure.
# About the '/usr/share', I am not sure, it might be read form some
# environmental variable passed to the container. But it is safe
# to leave it hard-coded like this.


class SeaDataEndpoint(EndpointResource):
    """
    Base to use rancher in many endpoints
    """

    _credentials: Dict[str, str] = {}

    _r = None  # main resources handler
    _path_separator = "/"
    _post_delimiter = "?"

    def load_rancher_credentials(self) -> Dict[str, str]:

        if not hasattr(self, "_credentials") or not self._credentials:
            self._credentials = Env.load_variables_group(prefix="resources")

        return self._credentials

    def get_ingestion_path_on_host(self, localpath: str, batch_id: str) -> str:
        """
        Return the path where the data is located
        on the Rancher host.

        The parts of the path can be configured,
        see: RESOURCES_LOCALPATH=/usr/share
        see: SEADATA_WORKSPACE_INGESTION=ingestion

        Example: /usr/share/ingestion/<batch_id>
        """

        return str(
            Path(
                localpath,  # "/usr/share" (default)
                INGESTION_DIR,  # "batches"  (default)
                batch_id,
            )
        )

    def get_ingestion_path_in_container(self) -> str:
        """
        Return the path where the data is located
        mounted inside the Rancher containers.

        The start of the path can be configured,
        see: RESOURCES_LOCALPATH=/usr/local
        The directory name is fixed.

        Note: The batch_id is not part of the path,
        as every container only works on one batch
        anyway. With every batch being mounted into
        the same path, the programs inside the container
        can easily operate on whichever data is inside
        that directory.

        Example: /usr/share/batch/
        """
        # "/usr/share/batch" (hard-coded)
        return str(Path(FS_PATH_IN_CONTAINER))

    def get_irods_path(
        self,
        irods_client: irods.IrodsPythonExt,
        mypath: str,
        suffix: Optional[str] = None,
    ) -> str:
        """
        Helper to construct a path of a data object
        inside irods.

        Note: Helper, only used inside this file.
        """
        suffix_path = Path(mypath)
        if suffix:
            suffix_path.joinpath(suffix)

        return irods_client.get_current_zone(suffix=str(suffix_path))

    def get_irods_production_path(
        self, irods_client: irods.IrodsPythonExt, batch_id: Optional[str] = None
    ) -> str:
        """
        Return path of the batch inside irods, once the
        batch is in production.

        It consists of the irods zone (retrieved from
        the irods client object), the production batch
        directory (from config) and the batch_id if given.

        Example: /myIrodsZone/cloud/<batch_id>
        """
        return self.get_irods_path(irods_client, PRODUCTION_COLL, batch_id)

    def get_irods_batch_path(
        self, irods_client: irods.IrodsPythonExt, batch_id: Optional[str] = None
    ) -> str:
        """
        Return path of the batch inside irods, before
        the batch goes to production.

        It consists of the irods zone (retrieved from
        the irods client object), the ingestion batch
        directory (from config) and the batch_id if given.

        Example: /myIrodsZone/batches/<batch_id>
        """
        return self.get_irods_path(irods_client, INGESTION_COLL, batch_id)

    def get_irods_order_path(
        self, irods_client: irods.IrodsPythonExt, order_id: Optional[str] = None
    ) -> str:
        """
        Return path of the order inside irods.

        It consists of the irods zone (retrieved from
        the irods client object), the order directory
        (from config) and the order_id if given.

        Example: /myIrodsZone/orders/<order_id>
        """
        return self.get_irods_path(irods_client, ORDERS_COLL, order_id)

    def return_async_id(self, request_id: str) -> Response:
        # dt = "20170712T15:33:11"
        dt = datetime.strftime(datetime.now(), "%Y%m%dT%H:%M:%S")
        return self.response({"request_id": request_id, "datetime": dt})

    @staticmethod
    def get_container_name(
        batch_id: str, qc_name: str, qc_label: Optional[str] = None
    ) -> str:
        qc_name = (
            qc_name.replace("_", "").replace("-", "").replace(":", "").replace(".", "")
        )

        if qc_label is None:
            return f"{batch_id}_{qc_name}"

        return f"{batch_id}_{qc_label}_{qc_name}"

    @staticmethod
    def get_container_image(qc_name: str, prefix: Optional[str] = None) -> str:
        if prefix is None:
            prefix = DEFAULT_IMAGE_PREFIX
        return f"{prefix}/{qc_name}"

    def get_batch_status(
        self, imain: irods.IrodsPythonExt, irods_path: str, local_path: Path
    ) -> Tuple[int, Union[List[str], Dict[str, Dict[str, Any]]]]:

        if not imain.is_collection(irods_path):
            return MISSING_BATCH, []

        if not local_path.exists():
            return MISSING_BATCH, []

        irods_files = imain.list(irods_path, detailed=True)

        # Too many files on irods
        fnum = len(irods_files)
        if fnum > 1:
            return BATCH_MISCONFIGURATION, irods_files

        # 1 file on irods -> everything is ok
        if fnum == 1:
            return ENABLED_BATCH, irods_files

        # No files on irods, let's check on filesystem
        fs_files = []
        for x in local_path.glob("*"):
            if x.is_file():
                fs_files.append(os.path.basename(str(x)))

        if not fs_files:
            return NOT_FILLED_BATCH, fs_files

        return PARTIALLY_ENABLED_BATCH, fs_files

    def irods_user(self, username: str) -> str:

        user = self.auth.get_user(username)
        sql = sqlalchemy.get_instance()

        if user is not None:
            log.debug("iRODS user already cached: {}", username)
        else:

            userdata = {
                "email": username,
                "name": username,
                # Password will not be used because the authmethod is `irods`
                "password": username,
                "surname": "iCAT",
                "authmethod": "irods",
            }
            user = self.auth.create_user(userdata, [self.auth.default_role])
            try:
                sql.session.commit()
                log.info("Cached iRODS user: {}", username)
            except BaseException as e:
                sql.session.rollback()
                log.error("Errors saving iRODS user: {}", username)
                log.error(str(e))
                log.error(type(e))

                user = self.auth.get_user(username)
                # Unable to do something...
                if user is None:
                    raise e

        # token
        payload, full_payload = self.auth.fill_payload(user)
        token = self.auth.create_token(payload)
        now = datetime.now(pytz.utc)
        if user.first_login is None:
            user.first_login = now
        user.last_login = now
        try:
            sql.session.add(user)
            sql.session.commit()
        except BaseException as e:
            log.error("DB error ({}), rolling back", e)
            sql.session.rollback()

        self.auth.save_token(user, token, full_payload)

        return token

    def response(  # type: ignore
        self,
        content: ResponseContent = None,
        errors: Optional[List[str]] = None,
        code: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        head_method: bool = False,
        allow_html: bool = False,
    ) -> Response:

        # Locally apply the response wrapper, no longer available in the core

        if content is None:
            elements = 0
        elif isinstance(content, str):
            elements = 1
        else:
            elements = len(content)

        if errors is None:
            total_errors = 0
        else:
            total_errors = len(errors)

        if code is None:
            code = 200

        resp = {
            "Response": {"data": content, "errors": errors},
            "Meta": {
                "data_type": str(type(content)),
                "elements": elements,
                "errors": total_errors,
                "status": int(code),
            },
        }

        return super().response(
            content=resp,
            code=code,
            headers=headers,
            head_method=head_method,
            allow_html=allow_html,
        )


EDMO_CODE = seadata_vars.get("edmo_code")
API_VERSION = seadata_vars.get("api_version")


class Parameter(webargs_fields.Raw):
    def _deserialize(
        self,
        value: Any,
        attr: Optional[str],
        data: Optional[Mapping[str, Any]],
        **kwargs: Any,
    ) -> Any:

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                log.error("Not a valid dictionary: {}", value)
                return super()._deserialize(value, attr, data, **kwargs)

        return super()._deserialize(value, attr, data, **kwargs)


# Validation should raise:
# f"Missing JSON key: {key}
class EndpointsInputSchema(Schema):
    request_id = fields.Str(required=True)
    edmo_code = fields.Int(required=True)
    datetime = fields.Str(required=True)
    api_function = fields.Str(required=True)
    version = fields.Str(required=True)
    test_mode = fields.Str(required=True)

    eudat_backdoor = fields.Bool(required=False, load_default=False)

    parameters = Parameter(required=True)


class ErrorCodes:
    PID_NOT_FOUND = ("41", "PID not found")
    INGESTION_FILE_NOT_FOUND = ("50", "File requested not found")

    # addzip_restricted_order
    MISSING_ZIPFILENAME_PARAM = ("4000", "Parameter zip_filename is missing")
    MISSING_FILENAME_PARAM = ("4001", "Parameter file_name is missing")
    MISSING_FILESIZE_PARAM = ("4002", "Parameter file_size is missing")
    INVALID_FILESIZE_PARAM = ("4003", "Invalid parameter file_size, integer expected")
    MISSING_FILECOUNT_PARAM = ("4004", "Parameter file_count is missing")
    INVALID_FILECOUNT_PARAM = ("4005", "Invalid parameter file_count, integer expected")
    FILENAME_DOESNT_EXIST = ("4006", "Partner zip (zipfile_name) does not exist")
    CHECKSUM_DOESNT_MATCH = ("4007", "Checksum does not match")
    FILESIZE_DOESNT_MATCH = ("4008", "File size does not match")
    UNZIP_ERROR_FILE_NOT_FOUND = ("4009", "Unzip error: zip file not found")
    UNZIP_ERROR_INVALID_FILE = ("4010", "Unzip error: zip file is invalid")
    UNZIP_ERROR_WRONG_FILECOUNT = ("4011", "Unzip error: file count does not match")
    B2SAFE_UPLOAD_ERROR = ("4012", "Unable to upload restricted zip on b2safe")
    UNZIP_ERROR_INVALID_FILE = ("4013", "Unable to create restricted zip file")
    ORDER_NOT_FOUND = ("4016", "Order does not exist or you lack permissions")
    BATCH_NOT_FOUND = ("4017", "Batch does not exist or you lack permissions")
    MISSING_PIDS_LIST = ("4018", "Parameter 'pids' is missing")
    UNABLE_TO_MOVE_IN_PRODUCTION = ("4019", "Cannot move file in production")
    UNABLE_TO_ASSIGN_PID = ("4020", "Unable to assign a PID to the file")
    B2HANDLE_ERROR = ("4021", "PID server (b2handle) unreachable")
    UNABLE_TO_DOWNLOAD_FILE = ("4022", "Unable to download the file")
    ZIP_SPLIT_ERROR = ("4023", "Zip split unexpected error")
    ZIP_SPLIT_ENTRY_TOO_LARGE = (
        "4024",
        "One or more files are larger than max zip size",
    )
    MISSING_BATCHES_PARAMETER = ("4025", "Parameter batches is missing")
    MISSING_ORDERS_PARAMETER = ("4026", "Parameter orders is missing")
    EMPTY_BATCHES_PARAMETER = ("4027", "Parameter batches is empty")
    EMPTY_ORDERS_PARAMETER = ("4028", "Parameter orders is empty")
    MISSING_CHECKSUM_PARAM = ("4029", "Parameter file_checksum is missing")
    INVALID_ZIPFILENAME_PARAM = (
        "4030",
        "Invalid parameter zipfile_name, list expected",
    )
    INVALID_CHECKSUM_PARAM = ("4031", "Invalid parameter file_checksum, list expected")
    INVALID_ZIPFILENAME_LENGTH = ("4032", "Unexpected lenght of zipfile_name parameter")
    INVALID_FILESIZE_LENGTH = ("4033", "Unexpected lenght of file_size parameter")
    INVALID_FILECOUNT_LENGTH = ("4034", "Unexpected lenght of file_count parameter")
    INVALID_CHECKSUM_LENGTH = ("4035", "Unexpected lenght of file_checksum parameter")
    INVALID_FILENAME_PARAM = (
        "4036",
        "Invalid parameter zipfile_name, a string is expected",
    )
    MISSING_BATCH_NUMBER_PARAM = ("4037", "Parameter batch_number is missing")
    UNREACHABLE_DOWNLOAD_PATH = ("4039", "Download path is unreachable")
    MISSING_ORDER_NUMBER_PARAM = ("4040", "Parameter order_number is missing")
    MISSING_DOWNLOAD_PATH_PARAM = ("4041", "Parameter download_path is missing")
    UNABLE_TO_CREATE_ZIP_FILE = ("4042", "Unable to create merged zip file")
    INVALID_ZIP_SPLIT_OUTPUT = ("4043", "Unable to retrieve results from zip split")
    EMPTY_DOWNLOAD_PATH_PARAM = ("4044", "Parameter download_path is empty")
    UNEXPECTED_ERROR = ("4045", "An unexpected error occurred")
    MISSING_REQUEST_ID = ("4046", "Request ID is missing")
    UNABLE_TO_SET_METADATA = ("4047", "Unable to set metadata to the file")


class Metadata:

    """{
    "cdi_n_code": "1522222",
    "format_n_code": "541555",
    "data_format_l24": "CFPOINT",
    "version": "1"
    }"""

    tid = "temp_id"
    keys = [
        "cdi_n_code",
        "format_n_code",
        "data_format_l24",
        "version",
        "batch_date",
        "test_mode",
    ]
    max_size = 10


class ImportManagerAPI:

    _uri = seadata_vars.get("api_im_url")

    def post(
        self,
        payload: Dict[str, Any],
        backdoor: bool = False,
        edmo_code: Optional[int] = None,
    ) -> bool:

        # timestamp '20180320T08:15:44' = YYMMDDTHH:MM:SS
        payload["edmo_code"] = edmo_code or EDMO_CODE
        payload["datetime"] = datetime.today().strftime("%Y%m%dT%H:%M:%S")
        if "api_function" not in payload:
            payload["api_function"] = "unknown_function"
        payload["api_function"] += "_ready"
        payload["version"] = API_VERSION

        if backdoor:
            log.warning(
                "The following json should be sent to ImportManagerAPI, "
                + "but you enabled the backdoor"
            )
            log.info(payload)
            return False

        if not PRODUCTION:
            log.warning(
                "The following json should be sent to ImportManagerAPI, "
                + "but you are not in production"
            )
            log.info(payload)
            return False

        # print("TEST", self._uri)

        if not self._uri:
            log.error("Invalid external APIs URI")
            return False

        r = requests.post(self._uri, json=payload)
        log.info("POST external IM API, status={}, uri={}", r.status_code, self._uri)

        if r.status_code != 200:
            log.error(
                "CDI: failed to call external APIs (status: {}, uri: {})",
                r.status_code,
                self._uri,
            )
            return False
        else:
            log.info(
                "CDI: called POST on external APIs (status: {}, uri: {})",
                r.status_code,
                self._uri,
            )
            return True
