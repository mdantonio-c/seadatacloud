"""
B2SAFE HTTP REST API endpoints.
Code to implement the extended endpoints.

Note:
Endpoints list and behaviour are available at:
https://github.com/EUDAT-B2STAGE/http-api/blob/master/docs/user/endpoints.md

"""

if True:
    import warnings

    warnings.filterwarnings(
        "ignore",
        # raised when imprting b2handle on python 3.10
        message="the imp module is deprecated in favour of importlib and slated for removal in Python 3.12; see the module's documentation for alternative uses",
    )

    warnings.filterwarnings(
        "ignore",
        # raised when imprting b2handle on python 3.9
        message="the imp module is deprecated in favour of importlib; see the module's documentation for alternative uses",
    )

import hashlib
from pathlib import Path

from restapi import decorators
from restapi.connectors import sqlalchemy
from restapi.exceptions import BadRequest, NotFound
from restapi.models import fields
from restapi.rest.definition import Response
from restapi.services.authentication import Role, User
from restapi.services.download import Downloader
from restapi.services.uploader import Uploader
from restapi.utilities.logs import log
from seadata.endpoints import Metadata, SeaDataEndpoint


class PIDEndpoint(SeaDataEndpoint, Uploader, Downloader):
    """Handling PID on endpoint requests"""

    labels = ["pids"]

    @decorators.auth.require()
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

        db = sqlalchemy.get_instance()
        # get the entry from the database
        dataobject = db.DataObject
        dataobject_entry = dataobject.query.filter(dataobject.uid == pid).first()

        if not dataobject_entry:
            raise BadRequest(f"PID {pid} not found")

        log.debug("PID {} verified", pid)
        path = Path(dataobject_entry.path)

        if not path or not path.exists():
            raise NotFound(f"File referenced by {pid} cannot be found")

        response = {
            "PID": pid,
            "verified": True,
            "metadata": {},
            "filename": path.name,
            "filesize": path.stat().st_size,
            "file_checksum": hashlib.md5(open(path, "rb").read()).hexdigest(),
            "batch_id": path.parent.name,
        }

        # get the metadata
        for key, value in dataobject_entry.object_metadata.items():
            if key in Metadata.keys:
                response["metadata"][key] = value  # type: ignore

        return self.response(response)
