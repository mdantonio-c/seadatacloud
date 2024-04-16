"""
Ingestion process submission to upload the SeaDataNet marine data.
"""
from typing import Any, Dict

import requests
from restapi import decorators
from restapi.connectors import celery
from restapi.exceptions import (
    NotFound,
    RestApiException,
    ServerError,
    ServiceUnavailable,
)
from restapi.rest.definition import Response
from restapi.services.authentication import User
from restapi.services.uploader import Uploader
from restapi.utilities.logs import log
from seadata.connectors.rabbit_queue import log_into_queue, prepare_message
from seadata.endpoints import (
    BATCH_MISCONFIGURATION,
    ENABLED_BATCH,
    INGESTION_DIR,
    MISSING_BATCH,
    MOUNTPOINT,
    NOT_FILLED_BATCH,
    PARTIALLY_ENABLED_BATCH,
    EndpointsInputSchema,
    SeaDataEndpoint,
)

ingestion_user = "RM"


class IngestionEndpoint(SeaDataEndpoint, Uploader):
    """Create batch folder and upload zip files inside it"""

    labels = ["ingestion"]

    @decorators.auth.require()
    @decorators.endpoint(
        path="/ingestion/<batch_id>",
        summary="Check if the ingestion batch is enabled",
        responses={
            200: "Batch enabled",
            400: "Batch misconfiguration",
            404: "Batch not enabled or lack of permissions",
        },
    )
    def get(self, batch_id: str, user: User) -> Response:

        log.info("Batch request: {}", batch_id)

        local_path = MOUNTPOINT.joinpath(INGESTION_DIR, batch_id)
        log.info("Batch local path: {}", local_path)

        batch_status, batch_files = self.get_batch_status(local_path)

        if batch_status == MISSING_BATCH:
            raise NotFound(
                f"Batch '{batch_id}' not enabled or you have no permissions",
            )

        if batch_status == BATCH_MISCONFIGURATION:
            # TODO is a check we have to mantain also in the no-irods version?
            log.error(
                "Misconfiguration: {} files in {} (expected 1)",
                len(batch_files),
                local_path,
            )
            raise RestApiException(
                f"Misconfiguration for batch_id {batch_id}",
                # Bad Resource
                status_code=410,
            )

        data: Dict[str, Any] = {}
        data["batch"] = batch_id
        if batch_status == NOT_FILLED_BATCH:
            data["status"] = "not_filled"
        elif batch_status == ENABLED_BATCH:
            data["status"] = "enabled"
        elif batch_status == PARTIALLY_ENABLED_BATCH:
            data["status"] = "partially_enabled"

        data["files"] = batch_files
        return self.response(data)

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/ingestion/<batch_id>",
        summary="Request to import a zip file to the ingestion cloud",
        responses={
            200: "Request unqueued for download",
            404: "Batch not enabled or lack of permissions",
        },
    )
    def post(self, batch_id: str, user: User, **json_input: Any) -> Response:

        local_path = MOUNTPOINT.joinpath(INGESTION_DIR, batch_id)
        log.info("Batch local path: {}", local_path)

        """
        Create the batch folder if not exists
        """

        # Log start (of enable) into RabbitMQ
        log_msg = prepare_message(
            self,
            json={"batch_id": batch_id},
            user=ingestion_user,
            log_string="start",
        )
        log_into_queue(self, log_msg)

        ##################
        # Does it already exist?

        # Create the folder on filesystem
        if not local_path.exists():
            try:
                local_path.mkdir(parents=True)
            except (FileNotFoundError, PermissionError) as e:  # pragma: no cover
                raise ServerError(f"Could not create directory {local_path} ({e})")
        else:
            log.debug("Batch path already exists on filesytem")

        # Log end (of enable) into RabbitMQ
        log_msg = prepare_message(
            self, status="enabled", user=ingestion_user, log_string="end"
        )
        log_into_queue(self, log_msg)

        # Download the file into the batch folder

        c = celery.get_instance()
        task = c.celery_app.send_task(
            "download_batch",
            args=[str(local_path), json_input],
            queue="ingestion",
            routing_key="ingestion",
        )
        log.info("Async job: {}", task.id)
        return self.return_async_id(task.id)

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/ingestion",
        summary="Delete one or more ingestion batches",
        responses={200: "Async job submitted for ingestion batches removal"},
    )
    def delete(self, user: User, **json_input: Any) -> Response:

        local_batch_path = MOUNTPOINT.joinpath(INGESTION_DIR)
        log.debug("Batch path: {}", local_batch_path)

        c = celery.get_instance()
        task = c.celery_app.send_task(
            "delete_batches",
            args=[str(local_batch_path), json_input],
            queue="ingestion",
            routing_key="ingestion",
        )
        log.info("Async job: {}", task.id)
        return self.return_async_id(task.id)
