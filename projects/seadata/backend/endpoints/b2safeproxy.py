from restapi import decorators
from restapi.exceptions import Unauthorized
from restapi.models import Schema, fields
from restapi.rest.definition import Response
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.connectors.irods import IrodsException, iexceptions
from seadata.endpoints import SeaDataEndpoint


class Credentials(Schema):
    username = fields.Str(required=True)
    password = fields.Str(
        required=True,
        password=True,
    )
    authscheme = fields.Str(default="credentials")


class B2safeProxy(SeaDataEndpoint):
    """Login to B2SAFE: directly."""

    _anonymous_user = "anonymous"

    labels = ["eudat", "b2safe", "authentication"]

    def get_and_verify_irods_session(
        self, user: str, password: str, authscheme: str
    ) -> bool:

        try:
            irods.get_instance(
                user=user,
                password=password,
                authscheme=authscheme,
            )
            return True

        except iexceptions.CAT_INVALID_USER:
            log.warning("Invalid user: {}", user)
        except iexceptions.UserDoesNotExist:
            log.warning("Invalid iCAT user: {}", user)
        except iexceptions.CAT_INVALID_AUTHENTICATION:
            log.warning("Invalid password for {}", user)
        except BaseException as e:
            log.warning('Failed with unknown reason:\n[{}] "{}"', type(e), e)
            error = "Failed to verify credentials against B2SAFE. " + "Unknown error: "
            if str(e).strip() == "":
                error += e.__class__.__name__
            else:
                error += str(e)
            raise IrodsException(error)

        return False

    @decorators.use_kwargs(Credentials)
    @decorators.endpoint(
        path="/auth/b2safeproxy",
        summary="Authenticate inside http api with b2safe user",
        description="Normal credentials (username and password) login endpoint",
        responses={
            401: "Invalid username or password for the current b2safe instance",
            200: "B2safe credentials provided are valid",
        },
    )
    def post(
        self, username: str, password: str, authscheme: str = "credentials"
    ) -> Response:

        if authscheme.upper() == "PAM":
            authscheme = "PAM"

        if username == self._anonymous_user:
            password = "WHATEVERYOUWANT:)"

        if not username or not password:
            raise Unauthorized("Missing username or password")

        valid = self.get_and_verify_irods_session(
            user=username,
            password=password,
            authscheme=authscheme,
        )

        if not valid:
            raise Unauthorized("Failed to authenticate on B2SAFE")

        token = self.irods_user(username)

        imain = irods.get_instance()

        user_home = imain.get_user_home(username)
        if imain.is_collection(user_home):
            b2safe_home = user_home
        else:
            b2safe_home = imain.get_user_home(append_user=False)

        response = {
            "token": token,
            "b2safe_user": username,
            "b2safe_home": b2safe_home,
        }

        return self.response(response)
