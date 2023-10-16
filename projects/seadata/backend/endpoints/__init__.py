import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import pytz
import requests
from cryptography.fernet import Fernet
from restapi.config import APP_SECRETS, PRODUCTION, get_backend_url
from restapi.connectors import sqlalchemy
from restapi.env import Env
from restapi.exceptions import BadRequest, RestApiException
from restapi.models import Schema, fields
from restapi.rest.definition import EndpointResource, Response, ResponseContent
from restapi.services.authentication import import_secret
from restapi.utilities.logs import log
from seadata.connectors import irods
from webargs import fields as webargs_fields

seadata_vars = Env.load_variables_group(prefix="seadata")

MISSING_BATCH = 0
NOT_FILLED_BATCH = 1
PARTIALLY_ENABLED_BATCH = 2
ENABLED_BATCH = 3
BATCH_MISCONFIGURATION = 4

BATCH_DQ_FOLDER_NAME = "original"


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
MOUNTPOINT = Path(seadata_vars.get("resources_mountpoint") or "/usr/share")

"""
These are the paths to the data on the hosts
that runs containers (both backend, celery and QC containers)
"""
INGESTION_DIR = seadata_vars.get("workspace_ingestion") or "batches"
PRODUCTION_DIR = seadata_vars.get("workspace_production") or "cloud"
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

"""this is the prefix used for UID generation. It is related to the single instances and should be defined in the .projectrc"""
UID_PREFIX = seadata_vars.get("uid_prefix") or "21.L00000"


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
        main_collection: str,  # oneof PRODUCTION_COLL, INGESTION_COLL, ORDERS_COLL
        obj_id: str = "",  # one of batch_id or order_id or empty
    ) -> str:
        """
        Helper to construct a path of a data object in irods
        """
        return irods_client.get_current_zone(suffix=Path(main_collection, obj_id))

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

    @staticmethod
    def list(
        path: Optional[Path] = None,
        recursive: bool = False,
        detailed: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """List the files inside a path/collection"""

        if path is None:
            # TODO check if okay
            return {}

        if path.is_file():
            # TODO is okay raising an exception here?
            raise RestApiException("Cannot list a file; you may get it instead.")

        data: Dict[str, Dict[str, Any]] = {}

        for el in path.iterdir():
            if el.is_dir() and el.name != BATCH_DQ_FOLDER_NAME:
                row: Dict[str, Any] = {}
                key = el.name
                row["name"] = el.name
                row["objects"] = {}
                if recursive:
                    row["objects"] = SeaDataEndpoint.list(
                        path=el,
                        recursive=recursive,
                        detailed=detailed,
                    )
                row["path"] = str(el.parent)
                row["object_type"] = "directory"
                # if detailed:
                #     row["owner"] = "-" # without irods this parameter is unuseful

                data[key] = row
            elif el.is_file():
                row = {}
                key = el.name
                row["name"] = el.name
                row["path"] = str(el.parent)
                row["object_type"] = "dataobject"

                if detailed:
                    # row["owner"] = str(el.owner) # without irods this parameter is unuseful
                    row["content_length"] = el.stat().st_size
                    row["created"] = datetime.fromtimestamp(
                        el.stat().st_ctime
                    ).strftime("%Y-%m-%d, %H:%M:%S")
                    row["last_modified"] = datetime.fromtimestamp(
                        el.stat().st_mtime
                    ).strftime("%Y-%m-%d, %H:%M:%S")
                data[key] = row

        return data

    def get_batch_status(
        self, local_path: Path
    ) -> Tuple[int, Union[List[str], Dict[str, Dict[str, Any]]]]:

        if not local_path.exists():
            return MISSING_BATCH, []

        fs_files = self.list(local_path, detailed=True)

        # TODO Too many files on irods --> is a problem also for the filesystem?
        fnum = len(fs_files)
        if fnum > 1:
            return BATCH_MISCONFIGURATION, fs_files

        # 1 file on irods -> everything is ok
        if fnum == 1:
            return ENABLED_BATCH, fs_files

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

    def get_seed_path(self, abs_order_path: Path) -> Path:
        return abs_order_path.joinpath(".seed")

    def get_seed(self, abs_order_path: Path) -> str:
        return import_secret(self.get_seed_path(abs_order_path))[0:12].decode()

    def get_secret(self) -> bytes:
        return import_secret(APP_SECRETS.joinpath("order_secrets.key"))

    def get_token(self, abs_order_path: Path, relative_zip_path: str) -> str:

        secret = self.get_secret()
        fernet = Fernet(secret)

        seed = self.get_seed(abs_order_path)
        plain = f"{seed}:{relative_zip_path}"
        return fernet.encrypt(plain.encode()).decode()

    def read_token(self, cypher: str) -> str:
        secret = self.get_secret()
        fernet = Fernet(secret)

        # This is seed:order_id/filefile
        plain = fernet.decrypt(cypher.encode()).decode().split(":")

        # This is seed
        seed = plain[0]

        # This is order_id/filefile
        zip_filepath = Path(plain[1])
        order_id = zip_filepath.parent.name

        abs_zip_path = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)

        expected_seed = self.get_seed(abs_zip_path)

        if seed != expected_seed:
            raise BadRequest("Invalid token seed")

        return str(zip_filepath)

    def get_download(
        self,
        order_id: str,
        order_path: Path,
        files: Dict[str, Dict[str, Any]],
        restricted: bool = False,
        index: Optional[int] = None,
        get_only_url: bool = False,
    ) -> Optional[Dict[str, Any]]:

        zip_file_name = self.get_order_zip_file_name(order_id, restricted, index)

        if zip_file_name not in files:
            return None

        zip_path = str(order_path.joinpath(zip_file_name))
        log.debug("Zip path: {}", zip_path)

        # in the same order folder there are multiple files, so a seed file is shared and can't be invalidated every time
        # TODO the seed file is ok that is by single order or it is preferable a more stricted rule and having a seed file par file?
        # seed_path = self.get_seed_path(order_path)
        # if seed_path.exists() and not get_only_url:
        #     log.info("Invalidating previous download URLs")
        #     seed_path.unlink()

        # This is not a path, this s the string that will be encoded in the token
        relative_path = os.path.join(order_id, zip_file_name)
        token = self.get_token(order_path, relative_path)

        ftype = ""
        if restricted:
            ftype += "1"
        else:
            ftype += "0"
        if index is None:
            ftype += "0"
        else:
            ftype += str(index)

        host = get_backend_url()

        # too many work for THEM to skip the add of the protocol
        # they prefer to get back an incomplete url
        host = host.replace("https://", "").replace("http://", "")

        url = f"{host}/api/orders/{order_id}/download/{ftype}/c/{token}"

        if get_only_url:
            # return only the url
            return {"url": url}

        ##################
        # TODO check if saving the metadata is still needed
        # # Set the url as Metadata in the irods file
        # imain.set_metadata(zip_ipath, download=url)
        #
        # # TOFIX: we should add a database or cache to save this,
        # # not irods metadata (known for low performances)
        # imain.set_metadata(zip_ipath, iticket_code=code)

        info = files[zip_file_name]

        return {
            "name": zip_file_name,
            "url": url,
            "size": info.get("content_length", 0),
        }

    def get_order_zip_file_name(
        self, order_id: str, restricted: bool = False, index: Optional[int] = None
    ) -> str:

        label = "restricted" if restricted else "unrestricted"
        if index is None:
            zip_file_name = f"order_{order_id}_{label}.zip"
        else:
            zip_file_name = f"order_{order_id}_{label}{index}.zip"

        return zip_file_name

    def response(  # type: ignore
        self,
        content: ResponseContent = None,
        errors: Optional[List[str]] = None,
        code: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        head_method: bool = False,
        allow_html: bool = False,
        force_json: bool = False,
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

        r = requests.post(self._uri, json=payload, timeout=30)
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
