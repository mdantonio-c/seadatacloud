import json
from typing import Optional

from restapi import decorators
from restapi.endpoints.login import Login
from restapi.endpoints.schemas import Credentials
from restapi.rest.definition import Response
from restapi.utilities.logs import log
from seadata.endpoints import SeaDataEndpoint


class SeadataLogin(Login, SeaDataEndpoint):
    @decorators.use_kwargs(Credentials)
    @decorators.endpoint(
        path="auth/seadata/login",
        summary="Login by proving your credentials",
        description="Login with basic credentials (username and password)",
        responses={
            200: "Credentials are valid",
            401: "Invalid access credentials",
            403: "Access to this account is not allowed",
        },
    )
    def post(
        self,
        username: str,
        password: str,
        new_password: Optional[str] = None,
        password_confirm: Optional[str] = None,
        totp_code: Optional[str] = None,
    ) -> Response:
        login_response = super().post()
        login_content = json.loads(login_response.get_data().decode())
        token = login_content["Response"]["data"]
        response = {"token": token, "user": username}
        return self.response(response)
