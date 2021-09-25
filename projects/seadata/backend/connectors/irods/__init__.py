"""
iRODS file-system flask connector
"""
import logging
from typing import Optional, Union

from irods import exception as iexceptions
from restapi.connectors import Connector
from restapi.env import Env
from restapi.exceptions import ServiceUnavailable
from restapi.utilities.logs import log
from seadata.connectors.irods.client import IrodsPythonClient
from seadata.connectors.irods.session import iRODSPickleSession as iRODSSession

# Silence too much logging from irods
irodslogger = logging.getLogger("irods")
irodslogger.setLevel(logging.INFO)

NORMAL_AUTH_SCHEME = "credentials"
PAM_AUTH_SCHEME = "PAM"


# Excluded from coverage because it is only used by a very specific service
# No further tests will be included in the core
class IrodsPythonExt(Connector, IrodsPythonClient):
    def __init__(self):
        self.prc = None
        super().__init__()

    def get_connection_exception(self):
        # Do not catch irods.exceptions.PAM_AUTH_PASSWORD_FAILED and
        # irods.expcetions.CAT_INVALID_AUTHENTICATION because they are used
        # by b2safeproxy to identify wrong credentials
        return (
            NotImplementedError,
            ServiceUnavailable,
            AttributeError,
            FileNotFoundError,
        )

    def connect(self, **kwargs):

        variables = self.variables.copy()
        variables.update(kwargs)

        session = variables.get("user_session")

        authscheme = variables.get("authscheme", NORMAL_AUTH_SCHEME)

        if session:
            user = session.email
        else:
            user = variables.get("user")
            password = variables.get("password")

        if user is None:
            raise AttributeError("No user is defined")
        log.debug("Irods user: {}", user)

        ######################
        if session:
            # recover the serialized session
            obj = iRODSSession.deserialize(session.session)

        elif authscheme == PAM_AUTH_SCHEME:

            obj = iRODSSession(
                user=user,
                password=password,
                authentication_scheme=authscheme,
                host=variables.get("host"),
                port=variables.get("port"),
                zone=variables.get("zone"),
            )

        elif password is not None:
            authscheme = NORMAL_AUTH_SCHEME

            obj = iRODSSession(
                user=user,
                password=password,
                authentication_scheme="native",
                host=variables.get("host"),
                port=variables.get("port"),
                zone=variables.get("zone"),
            )

        else:
            raise NotImplementedError(
                f"Invalid iRODS authentication scheme: {authscheme}"
            )

        # # set timeout on existing socket/connection
        # with obj.pool.get_connection() as conn:
        #     timer = conn.socket.gettimeout()
        #     log.debug("Current timeout: {}", timer)
        #     conn.socket.settimeout(10.0)
        #     timer = conn.socket.gettimeout()
        #     log.debug("New timeout: {}", timer)

        # based on https://github.com/irods/python-irodsclient/pull/90
        # NOTE: timeout has to be below 30s (http request timeout)
        obj.connection_timeout = Env.to_int(variables.get("timeout"), 15.0)

        try:
            obj.users.get(user, user_zone=variables.get("zone"))

        except iexceptions.CAT_INVALID_AUTHENTICATION as e:
            raise e

        # except iexceptions.PAM_AUTH_PASSWORD_FAILED as e:
        except iexceptions.PAM_AUTH_PASSWORD_FAILED as e:
            raise e

        self.prc = obj
        # self.variables = variables

        return self

    def disconnect(self):
        self.disconnected = True
        if self.prc:
            self.prc.cleanup()

    def is_connected(self):

        return not self.disconnected


instance = IrodsPythonExt()


def get_instance(
    verification: Optional[int] = None,
    expiration: Optional[int] = None,
    **kwargs: Union[Optional[str], int],
) -> "IrodsPythonExt":

    return instance.get_instance(
        verification=verification, expiration=expiration, **kwargs
    )
