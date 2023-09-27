from typing import Any

import requests
from restapi import decorators
from restapi.connectors import celery
from restapi.exceptions import ServiceUnavailable
from restapi.rest.definition import Response
from restapi.services.authentication import User
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.endpoints import (
    INGESTION_COLL,
    INGESTION_DIR,
    MOUNTPOINT,
    ORDERS_COLL,
    ORDERS_DIR,
    EndpointsInputSchema,
    SeaDataEndpoint,
)


class ListResources(SeaDataEndpoint):

    labels = ["helper"]

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/resourceslist",
        summary="Request a list of existing batches and orders",
        responses={200: "Returning id of async request"},
    )
    def post(self, user: User, **json_input: Any) -> Response:

        ingestion_dir = MOUNTPOINT.joinpath(INGESTION_DIR)
        orders_dir = MOUNTPOINT.joinpath(ORDERS_DIR)

        c = celery.get_instance()
        task = c.celery_app.send_task(
            "list_resources",
            args=[
                str(ingestion_dir),
                str(orders_dir),
                json_input,
            ],
        )
        log.info("Async job: {}", task.id)
        return self.return_async_id(task.id)
