import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytz
from b2stage.connectors import irods
from b2stage.endpoints.commons import (
    CURRENT_B2SAFE_SERVER,
    CURRENT_HTTPAPI_SERVER,
    HTTP_PROTOCOL,
    IRODS_PROTOCOL,
    IRODS_VARS,
    PRODUCTION,
)
from irods import exception as iexceptions
from restapi.env import Env
from restapi.exceptions import RestApiException, Unauthorized
from restapi.rest.definition import EndpointResource, Response, ResponseContent
from restapi.utilities.logs import log
from seadata.endpoints.commons.seadatacloud import seadata_vars

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

CONTAINERS_VARS = Env.load_variables_group(prefix="containers")


class SeaDataEndpoint(EndpointResource):
    """
    Base to use rancher in many endpoints
    """

    _credentials: Dict[str, str] = {}

    _r = None  # main resources handler
    _path_separator = "/"
    _post_delimiter = "?"

    def load_credentials(self):

        if not hasattr(self, "_credentials") or not self._credentials:
            self._credentials = Env.load_variables_group(prefix="resources")

        return self._credentials

    def get_or_create_handle(self):
        """
        Create a Rancher object and feed it with
        config that starts with "RESOURCES_",
        including the localpath, which is
        set to "/nfs/share".
        """

        # Import here to prevent circular imports
        from seadata.endpoints.commons.rancher import Rancher

        params = self.load_credentials()
        return Rancher(**params)

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

    def get_ingestion_path_in_container(self):
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

    def get_input_zip_filename(self, filename=None, extension="zip", sep="."):
        if filename is None:
            filename = "input"
        else:
            filename = filename.replace(f"{sep}{extension}", "")
        return f"{filename}{sep}{extension}"

    def get_irods_path(self, irods_client, mypath, suffix=None):
        """
        Helper to construct a path of a data object
        inside irods.

        Note: Helper, only used inside this file.
        Note: The irods_client is of class
        IrodsPythonClient, defined in module
        rapydo/http-api/restapi/connectors/irods/client
        """
        suffix_path = Path(mypath)
        if suffix:
            suffix_path.joinpath(suffix)

        return irods_client.get_current_zone(suffix=str(suffix_path))
        # TODO: Move to other module, has nothing to do with Rancher cluster!

    def get_irods_production_path(self, irods_client, batch_id=None):
        """
        Return path of the batch inside irods, once the
        batch is in production.

        It consists of the irods zone (retrieved from
        the irods client object), the production batch
        directory (from config) and the batch_id if given.

        Example: /myIrodsZone/cloud/<batch_id>
        """
        return self.get_irods_path(irods_client, PRODUCTION_COLL, batch_id)
        # TODO: Move to other module, has nothing to do with Rancher cluster!

    def get_irods_batch_path(self, irods_client, batch_id=None):
        """
        Return path of the batch inside irods, before
        the batch goes to production.

        It consists of the irods zone (retrieved from
        the irods client object), the ingestion batch
        directory (from config) and the batch_id if given.

        Example: /myIrodsZone/batches/<batch_id>
        """
        return self.get_irods_path(irods_client, INGESTION_COLL, batch_id)
        # TODO: Move to other module, has nothing to do with Rancher cluster!

    def get_irods_order_path(self, irods_client, order_id=None):
        """
        Return path of the order inside irods.

        It consists of the irods zone (retrieved from
        the irods client object), the order directory
        (from config) and the order_id if given.

        Example: /myIrodsZone/orders/<order_id>
        """
        return self.get_irods_path(irods_client, ORDERS_COLL, order_id)
        # TODO: Move to other module, has nothing to do with Rancher cluster!

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

    def get_batch_status(self, imain, irods_path, local_path):

        files = {}
        if not imain.is_collection(irods_path):
            return MISSING_BATCH, files

        if not local_path.exists():
            return MISSING_BATCH, files

        files = imain.list(irods_path, detailed=True)

        # Too many files on irods
        fnum = len(files)
        if fnum > 1:
            return BATCH_MISCONFIGURATION, files

        # 1 file on irods -> everything is ok
        if fnum == 1:
            return ENABLED_BATCH, files

        # No files on irods, let's check on filesystem
        files = []
        for x in local_path.glob("*"):
            if x.is_file():
                files.append(os.path.basename(str(x)))
        fnum = len(files)
        if fnum <= 0:
            return NOT_FILLED_BATCH, files

        return PARTIALLY_ENABLED_BATCH, files

    def irods_user(self, username, session):

        user = self.auth.get_user(username)

        if user is not None:
            log.debug("iRODS user already cached: {}", username)
            user.session = session
        else:

            userdata = {
                "email": username,
                "name": username,
                # Password will not be used because the authmedos is `irods`
                "password": session,
                "surname": "iCAT",
                "authmethod": "irods",
                "session": session,
            }
            user = self.auth.create_user(userdata, [self.auth.default_role])
            try:
                self.auth.db.session.commit()
                log.info("Cached iRODS user: {}", username)
            except BaseException as e:
                self.auth.db.session.rollback()
                log.error("Errors saving iRODS user: {}", username)
                log.error(str(e))
                log.error(type(e))

                user = self.auth.get_user(username)
                # Unable to do something...
                if user is None:
                    raise e
                user.session = session

        # token
        payload, full_payload = self.auth.fill_payload(user)
        token = self.auth.create_token(payload)
        now = datetime.now(pytz.utc)
        if user.first_login is None:
            user.first_login = now
        user.last_login = now
        try:
            self.auth.db.session.add(user)
            self.auth.db.session.commit()
        except BaseException as e:
            log.error("DB error ({}), rolling back", e)
            self.auth.db.session.rollback()

        self.auth.save_token(user, token, full_payload)

        return token, username

    def response(
        self,
        content: ResponseContent = None,
        errors: Optional[List[str]] = None,
        code: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        head_method: bool = False,
        allow_html: bool = False,
    ) -> Response:

        SEADATA_PROJECT = os.getenv("SEADATA_PROJECT", "0")
        # Locally apply the response wrapper, no longer available in the core

        if SEADATA_PROJECT == "0":

            return super().response(
                content=content,
                code=code,
                headers=headers,
                head_method=head_method,
                allow_html=allow_html,
            )

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
