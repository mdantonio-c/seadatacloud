"""
Move data from ingestion to production
"""
import requests
from b2stage.endpoints.commons.b2handle import B2HandleEndpoint
from restapi import decorators
from restapi.connectors import celery
from restapi.exceptions import BadRequest, NotFound, ServiceUnavailable
from restapi.rest.definition import Response
from restapi.services.authentication import User
from restapi.utilities.logs import log
from seadata.endpoints.commons.cluster import ClusterContainerEndpoint
from seadata.endpoints.commons.seadatacloud import EndpointsInputSchema
from seadata.endpoints.commons.seadatacloud import Metadata as md


#################
# REST CLASS
class MoveToProductionEndpoint(B2HandleEndpoint, ClusterContainerEndpoint):

    labels = ["ingestion"]

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/ingestion/<batch_id>/approve",
        summary="Approve files in a batch that are passing all qcs",
        responses={200: "Registration executed"},
    )
    def post(self, batch_id: str, user: User, **json_input) -> Response:

        params = json_input.get("parameters", {})
        if not params:
            raise BadRequest("parameters is empty")

        files = params.get("pids", [])
        if not files:
            raise BadRequest("pids parameter is empty list")

        filenames = []
        for data in files:

            if not isinstance(data, dict):
                raise BadRequest(
                    "File list contains at least one wrong entry",
                )

            # print("TEST", data)
            for key in md.keys:  # + [md.tid]:
                value = data.get(key)
                if value is None:
                    raise BadRequest(f"Missing parameter: {key}")

                value_len = len(value)
                if value_len > md.max_size:
                    raise BadRequest(f"Param '{key}': exceeds size {md.max_size}")
                elif value_len < 1:
                    raise BadRequest(f"Param '{key}': empty")

            filenames.append(data.get(md.tid))

        ################
        # 1. check if irods path exists
        try:
            imain = self.get_main_irods_connection()
            batch_path = self.get_irods_batch_path(imain, batch_id)
            log.debug("Batch path: {}", batch_path)

            if not imain.is_collection(batch_path):
                raise NotFound(
                    f"Batch '{batch_id}' not enabled (or no permissions)",
                )

            ################
            # 2. make batch_id directory in production if not existing
            prod_path = self.get_irods_production_path(imain, batch_id)
            log.debug("Production path: {}", prod_path)
            obj = self.init_endpoint()
            imain.create_collection_inheritable(prod_path, obj.username)

            ################
            # ASYNC
            log.info("Submit async celery task")

            c = celery.get_instance()
            task = c.celery_app.send_task(
                "move_to_production_task",
                args=[batch_id, batch_path, prod_path, json_input],
                queue="ingestion",
                routing_key="ingestion",
            )
            log.info("Async job: {}", task.id)

            return self.return_async_id(task.id)

        except requests.exceptions.ReadTimeout:
            raise ServiceUnavailable("B2SAFE is temporarily unavailable")
