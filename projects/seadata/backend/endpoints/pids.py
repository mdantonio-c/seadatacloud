"""
B2SAFE HTTP REST API endpoints.
Code to implement the extended endpoints.

Note:
Endpoints list and behaviour are available at:
https://github.com/EUDAT-B2STAGE/http-api/blob/master/docs/user/endpoints.md

"""

from b2stage.endpoints.commons import path
from b2stage.endpoints.commons.b2handle import PIDgenerator
from restapi import decorators
from restapi.exceptions import BadRequest
from restapi.models import fields
from restapi.rest.definition import Response
from restapi.services.authentication import Role, User
from restapi.services.download import Downloader
from restapi.services.uploader import Uploader
from restapi.utilities.logs import log
from seadata.endpoints import SeaDataEndpoint
from seadata.endpoints.commons.seadatacloud import Metadata


class PIDEndpoint(SeaDataEndpoint, Uploader, Downloader):
    """Handling PID on endpoint requests"""

    labels = ["pids"]

    @decorators.auth.require_all(Role.USER)
    # "description": "Activate file downloading (if PID points to a single file)",
    @decorators.use_kwargs({"download": fields.Bool()}, location="query")
    @decorators.endpoint(
        path="/pids/<path:pid>",
        summary="Resolve a pid and retrieve metadata or download it link object",
        responses={
            200: "The information related to the file which the pid points to or the file content if download is activated or the list of objects if the pid points to a collection"
        },
    )
    def get(self, pid: str, user: User, download: bool = False) -> Response:
        """Get metadata or file from pid"""

        pmaker = PIDgenerator()

        b2handle_output = pmaker.check_pid_content(pid)
        if b2handle_output is None:
            raise BadRequest(f"PID {pid} not found")

        log.debug("PID {} verified", pid)
        ipath = pmaker.parse_pid_dataobject_path(b2handle_output)
        response = {
            "PID": pid,
            "verified": True,
            "metadata": {},
            "temp_id": path.last_part(ipath),
            "batch_id": path.last_part(path.dir_name(ipath)),
        }

        imain = self.get_main_irods_connection()
        metadata, _ = imain.get_metadata(ipath)

        for key, value in metadata.items():
            if key in Metadata.keys:
                response["metadata"][key] = value

        return self.response(response)