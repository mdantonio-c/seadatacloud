import requests
from restapi import decorators
from restapi.connectors import celery
from restapi.exceptions import NotFound, ServiceUnavailable
from restapi.rest.definition import Response
from restapi.services.authentication import Role, User
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.endpoints import (
    MOUNTPOINT,
    PRODUCTION_COLL,
    PRODUCTION_DIR,
    SeaDataEndpoint,
)


class PidCache(SeaDataEndpoint):

    labels = ["helper"]

    @decorators.auth.require_any(Role.ADMIN, Role.STAFF)
    @decorators.endpoint(
        path="/pidcache",
        summary="Retrieve values from the pid cache",
        responses={200: "Async job started"},
    )
    def get(self, user: User) -> Response:

        c = celery.get_instance()
        task = c.celery_app.send_task("inspect_pids_cache")
        log.info("Async job: {}", task.id)
        return self.return_async_id(task.id)

    @decorators.auth.require_any(Role.ADMIN, Role.STAFF)
    @decorators.endpoint(
        path="/pidcache/<batch_id>",
        summary="Fill the pid cache",
        responses={200: "Async job started"},
    )
    def post(self, batch_id: str, user: User) -> Response:

        collection = MOUNTPOINT.joinpath(PRODUCTION_DIR, batch_id)

        if not collection.is_dir():
            raise NotFound(f"Invalid batch id {batch_id}")

        c = celery.get_instance()
        task = c.celery_app.send_task("cache_batch_pids", args=[str(collection)])
        log.info("Async job: {}", task.id)

        return self.return_async_id(task.id)
