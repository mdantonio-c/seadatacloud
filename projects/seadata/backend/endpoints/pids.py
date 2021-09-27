"""
B2SAFE HTTP REST API endpoints.
Code to implement the extended endpoints.

Note:
Endpoints list and behaviour are available at:
https://github.com/EUDAT-B2STAGE/http-api/blob/master/docs/user/endpoints.md

"""

from restapi import decorators
from restapi.exceptions import BadRequest, NotFound
from restapi.models import fields
from restapi.rest.definition import Response
from restapi.services.authentication import Role, User
from restapi.services.download import Downloader
from restapi.services.uploader import Uploader
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.connectors.b2handle import PIDgenerator
from seadata.endpoints import Metadata, SeaDataEndpoint


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

        if not ipath:
            raise NotFound(f"Object referenced by {pid} cannot be found")

        response = {
            "PID": pid,
            "verified": True,
            "metadata": {},
            "temp_id": ipath.name,
            "batch_id": ipath.parent.name,
        }

        imain = irods.get_instance()
        metadata = imain.get_metadata(str(ipath))

        for key, value in metadata.items():
            if key in Metadata.keys:
                response["metadata"][str(key)] = value

        return self.response(response)
