from typing import Any

import requests
from restapi import decorators
from restapi.connectors import celery
from restapi.exceptions import ServiceUnavailable
from restapi.rest.definition import Response
from restapi.services.authentication import Role, User
from restapi.services.uploader import Uploader
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.endpoints import ORDERS_COLL, EndpointsInputSchema, SeaDataEndpoint


class Restricted(SeaDataEndpoint, Uploader):

    labels = ["restricted"]

    # Request for a file download into a restricted order
    @decorators.auth.require_any(Role.ADMIN, Role.STAFF)
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/restricted/<order_id>",
        summary="Request to import a zip file into a restricted order",
        responses={200: "Request unqueued for download"},
    )
    def post(self, order_id: str, user: User, **json_input: Any) -> Response:

        try:
            imain = irods.get_instance()
            order_path = self.get_irods_path(imain, ORDERS_COLL, order_id)
            if not imain.is_collection(order_path):
                # Create the path and set permissions
                imain.create_collection_inheritable(order_path, user.email)

            c = celery.get_instance()
            task = c.celery_app.send_task(
                "download_restricted_order",
                args=[order_id, order_path, json_input],
                queue="restricted",
                routing_key="restricted",
            )
            log.info("Async job: {}", task.id)
            return self.return_async_id(task.id)
        except requests.exceptions.ReadTimeout:  # pragma: no cover
            raise ServiceUnavailable("B2SAFE is temporarily unavailable")
