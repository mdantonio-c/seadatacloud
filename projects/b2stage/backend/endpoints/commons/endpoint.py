"""
Common functions for EUDAT endpoints
"""

import os

from b2stage.connectors import irods
from b2stage.endpoints.commons import (
    CURRENT_B2SAFE_SERVER,
    CURRENT_HTTPAPI_SERVER,
    HTTP_PROTOCOL,
    IRODS_PROTOCOL,
    IRODS_VARS,
    PRODUCTION,
    InitObj,
)
from b2stage.endpoints.commons.b2access import B2accessUtilities
from irods import exception as iexceptions
from restapi.exceptions import RestApiException
from restapi.utilities.logs import log

MISSING_BATCH = 0
NOT_FILLED_BATCH = 1
PARTIALLY_ENABLED_BATCH = 2
ENABLED_BATCH = 3
BATCH_MISCONFIGURATION = 4


class EudatEndpoint(B2accessUtilities):
    """
    Extend normal API to init
    all necessary EUDAT B2STAGE API services
    """

    _r = None  # main resources handler
    _path_separator = "/"
    _post_delimiter = "?"

    def init_endpoint(self):

        # Get user information from db, associated to token
        # NOTE: user legenda
        # internal_user = user internal to the API
        # external_user = user from oauth (B2ACCESS)
        internal_user = self.get_user()
        log.debug(
            "Token user: {} with auth method {}",
            internal_user,
            internal_user.authmethod,
        )

        #################################
        # decide which type of auth we are dealing with
        # NOTE: icom = irods commands handler (official python driver PRC)

        refreshed = False

        if internal_user is None:
            raise AttributeError("Missing user association to token")

        if internal_user.authmethod == "credentials":

            icom = self.irodsuser_from_b2stage(internal_user)

        elif internal_user.authmethod == "irods":

            icom = self.irodsuser_from_b2safe(internal_user)

        else:
            log.error("Unknown credentials provided")

        # sql = sqlalchemy.get_instance()
        # update user variable to account email, which should be always unique
        user = internal_user.email

        return InitObj(
            username=user,
            icommands=icom,
            valid_credentials=True,
            refreshed=refreshed,
        )

    def irodsuser_from_b2safe(self, user):

        if user.session is not None and len(user.session) > 0:
            log.debug("Validated B2SAFE user: {}", user.uuid)
        else:
            msg = "Current credentials not registered inside B2SAFE"
            raise RestApiException(msg, status_code=401)

        try:
            return irods.get_instance(user_session=user)
        except iexceptions.PAM_AUTH_PASSWORD_FAILED:
            msg = "PAM Authentication failed, invalid password or token"
            raise RestApiException(msg, status_code=401)

        return None

    def irodsuser_from_b2stage(self, internal_user):
        """
        Normal credentials (only available inside the HTTP API database)
        aren't mapped to a real B2SAFE user.
        We force the guest user if it's indicated in the configuration.

        NOTE: this usecase is to be avoided in production.
        """

        if PRODUCTION:
            # 'guest' irods mode is only for debugging purpose
            raise ValueError("Invalid authentication")

        icom = irods.get_instance(
            only_check_proxy=True,
            user=IRODS_VARS.get("guest_user"),
            password=None,
            gss=True,
        )

        return icom

    def httpapi_location(self, ipath, api_path=None, remove_suffix=None):
        """URI for retrieving with GET method"""

        # TODO: check and clean 'remove_suffix parameter'

        # if remove_suffix is not None and uri_path.endswith(remove_suffix):
        #     uri_path = uri_path.replace(remove_suffix, '')

        if api_path is None:
            api_path = ""
        else:
            api_path = "/{}".format(api_path.lstrip("/"))

        # print("TEST", CURRENT_HTTPAPI_SERVER)

        return "{}://{}{}/{}".format(
            HTTP_PROTOCOL,
            CURRENT_HTTPAPI_SERVER,
            api_path,
            ipath.strip(self._path_separator),
        )

    def b2safe_location(self, ipath):
        return "{}://{}/{}".format(
            IRODS_PROTOCOL,
            CURRENT_B2SAFE_SERVER,
            ipath.strip(self._path_separator),
        )

    def fix_location(self, location):
        if not location.startswith(self._path_separator):
            location = self._path_separator + location
        return location

    # @staticmethod
    # def user_from_unity(unity_persistent):
    #     """ Take the last piece of the unity id """
    #     return unity_persistent.split('-')[::-1][0]

    @staticmethod
    def splitall(path):
        allparts = []
        while 1:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                allparts.insert(0, parts[0])
                break
            elif parts[1] == path:  # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                allparts.insert(0, parts[1])
        return allparts

    @staticmethod
    def filename_from_path(path):
        return os.path.basename(os.path.normpath(path))

    def complete_path(self, path, filename=None):
        """Make sure you have a path with no trailing slash"""
        path = path.rstrip("/")
        if filename is not None:
            path += "/" + filename.rstrip("/")
        return path

    def parse_path(self, path):

        # If path is empty or we have a relative path return None
        # so that we can give an error
        if path is None or not os.path.isabs(path):
            return None

        if isinstance(path, str):
            return path.rstrip(self._path_separator)

        return None

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
