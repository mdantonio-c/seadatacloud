"""
Launch containers for quality checks in Seadata
"""
import json
import os
import time
from typing import Any

import requests
from restapi import decorators
from restapi.env import Env
from restapi.exceptions import Conflict, NotFound, RestApiException, ServiceUnavailable
from restapi.rest.definition import Response
from restapi.services.authentication import User
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.connectors.rancher import Rancher
from seadata.endpoints import (
    BATCH_MISCONFIGURATION,
    INGESTION_COLL,
    INGESTION_DIR,
    MISSING_BATCH,
    MOUNTPOINT,
    NOT_FILLED_BATCH,
    EndpointsInputSchema,
    SeaDataEndpoint,
)


class Resources(SeaDataEndpoint):

    labels = ["ingestion"]
    depends_on = ["RESOURCES_PROJECT"]

    @decorators.auth.require()
    @decorators.endpoint(
        path="/ingestion/<batch_id>/qc/<qc_name>",
        summary="Resources management",
    )
    def get(self, batch_id: str, qc_name: str, user: User) -> Response:
        """Check my quality check container"""

        # log.info("Request for resources")
        params = self.load_rancher_credentials()
        rancher = Rancher(**params)
        container_name = self.get_container_name(batch_id, qc_name, rancher._qclabel)
        # resources = rancher.list()
        container = rancher.get_container_object(container_name)
        if container is None:
            raise NotFound("Quality check does not exist")

        logs = rancher.recover_logs(container_name)
        # print("TEST", container_name, tmp)
        errors_keys = ["failure", "failed", "error"]
        errors = []
        for line in logs.lower().split("\n"):
            if line.strip() == "":
                continue
            for key in errors_keys:
                if key in line:
                    errors.append(line)
                    break

        response = {
            "batch_id": batch_id,
            "qc_name": qc_name,
            "state": container.get("state"),
            "errors": errors,
        }

        if container.get("transitioning") == "error":
            response["errors"].append(container.get("transitioningMessage"))

        """
        "state": "stopped", / error
        "firstRunningTS": 1517431685000,
        "transitioning": "no",
        "transitioning": "error",
        "transitioningMessage": "Image
        """

        return self.response(response)

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/ingestion/<batch_id>/qc/<qc_name>",
        summary="Launch a quality check as a docker container",
    )
    def put(self, batch_id: str, qc_name: str, **input_json: Any) -> Response:
        """Launch a quality check inside a container"""

        ###########################
        # get name from batch
        try:
            imain = irods.get_instance()
            batch_path = self.get_irods_path(imain, INGESTION_COLL, batch_id)
            local_path = MOUNTPOINT.joinpath(INGESTION_DIR, batch_id)
            log.info("Batch irods path: {}", batch_path)
            log.info("Batch local path: {}", local_path)
            batch_status, batch_files = self.get_batch_status(
                imain, batch_path, local_path
            )

            if batch_status == MISSING_BATCH:
                raise NotFound(f"Batch '{batch_id}' not found (or no permissions)")

            if batch_status == NOT_FILLED_BATCH:
                raise RestApiException(
                    # Bad Resource
                    f"Batch '{batch_id}' not yet filled",
                    status_code=410,
                )

            if batch_status == BATCH_MISCONFIGURATION:
                log.error(
                    "Misconfiguration: {} files in {} (expected 1)",
                    len(batch_files),
                    batch_path,
                )
                raise RestApiException(
                    f"Misconfiguration for batch_id {batch_id}",
                    # Bad Resource
                    status_code=410,
                )
        except requests.exceptions.ReadTimeout:  # pragma: no cover
            raise ServiceUnavailable("B2SAFE is temporarily unavailable")

        ###################
        # Parameters (and checks)
        envs = {}

        # TODO: backdoor check - remove me
        bd = input_json.pop("eudat_backdoor", False)
        if bd:
            im_prefix = "eudat"
        else:
            im_prefix = "maris"
        log.debug("Image prefix: {}", im_prefix)

        response = {
            "batch_id": batch_id,
            "qc_name": qc_name,
            "status": "executed",
            "input": input_json,
        }

        ###################
        try:
            params = self.load_rancher_credentials()
            rancher = Rancher(**params)
        except BaseException as e:
            log.critical(str(e))
            raise ServiceUnavailable(
                "Cannot establish a connection with Rancher",
            )

        container_name = self.get_container_name(batch_id, qc_name, rancher._qclabel)

        # Duplicated quality checks on the same batch are not allowed
        container_obj = rancher.get_container_object(container_name)
        if container_obj is not None:
            log.error("Docker container {} already exists!", container_name)
            response["status"] = "existing"
            raise Conflict(f"Docker container {container_name} already exists!")

        docker_image_name = self.get_container_image(qc_name, prefix=im_prefix)

        ###########################
        # ## ENVS

        host_ingestion_path = self.get_ingestion_path_on_host(
            rancher._localpath, batch_id
        )
        container_ingestion_path = self.get_ingestion_path_in_container()

        envs["BATCH_DIR_PATH"] = container_ingestion_path
        from seadata.connectors.rabbit_queue import QUEUE_VARS

        CONTAINERS_VARS = Env.load_variables_group(prefix="containers")

        for key, value in QUEUE_VARS.items():
            if key == "enable":
                continue

            if key == "user":
                rabbituser = CONTAINERS_VARS.get("rabbituser")
                if not rabbituser:  # pragma: no cover
                    log.warning("Unable to retrieve Rabbit User")
                    continue
                value = rabbituser
            elif key == "password":
                rabbitpass = CONTAINERS_VARS.get("rabbitpass")
                if not rabbitpass:  # pragma: no cover
                    log.warning("Unable to retrieve Rabbit Password")
                    continue
                value = rabbitpass

            envs["LOGS_" + key.upper()] = value
        # envs['DB_USERNAME'] = CONTAINERS_VARS.get('dbuser')
        # envs['DB_PASSWORD'] = CONTAINERS_VARS.get('dbpass')
        # envs['DB_USERNAME_EDIT'] = CONTAINERS_VARS.get('dbextrauser')
        # envs['DB_PASSWORD_EDIT'] = CONTAINERS_VARS.get('dbextrapass')

        # FOLDER inside /batches to store temporary json inputs
        # TODO: to be put into the configuration
        JSON_DIR = "json_inputs"

        # Mount point of the json dir into the QC container
        QC_MOUNTPOINT = "/json"

        # Note: MOUNTPOINT is a Path...
        json_path_backend = os.path.join(MOUNTPOINT, INGESTION_DIR, JSON_DIR)

        if not os.path.exists(json_path_backend):
            log.info("Creating folder {}", json_path_backend)
            os.mkdir(json_path_backend)

        json_path_backend = os.path.join(json_path_backend, batch_id)

        if not os.path.exists(json_path_backend):
            log.info("Creating folder {}", json_path_backend)
            os.mkdir(json_path_backend)

        json_input_file = f"input.{int(time.time())}.json"
        json_input_path = os.path.join(json_path_backend, json_input_file)
        with open(json_input_path, "w+") as f:
            f.write(json.dumps(input_json))

        json_path_qc = self.get_ingestion_path_on_host(rancher._localpath, JSON_DIR)
        json_path_qc = os.path.join(json_path_qc, batch_id)
        envs["JSON_FILE"] = os.path.join(QC_MOUNTPOINT, json_input_file)

        extra_params = {
            "dataVolumes": [
                f"{host_ingestion_path}:{container_ingestion_path}",
                f"{json_path_qc}:{QC_MOUNTPOINT}",
            ],
            "environment": envs,
        }
        if bd:
            extra_params["command"] = ["/bin/sleep", "999999"]

        # log.info(extra_params)
        ###########################
        error = rancher.run(
            container_name=container_name,
            image_name=docker_image_name,
            private=True,
            extras=extra_params,
        )

        if error:
            response["status"] = "failure"
            response["description"] = error
            return self.response(response, code=500)

        return self.response(response)

    @decorators.auth.require()
    @decorators.endpoint(
        path="/ingestion/<batch_id>/qc/<qc_name>",
        summary="Remove a quality check if existing",
    )
    def delete(self, batch_id: str, qc_name: str, user: User) -> Response:
        """
        Remove a quality check executed
        """

        params = self.load_rancher_credentials()
        rancher = Rancher(**params)
        container_name = self.get_container_name(batch_id, qc_name, rancher._qclabel)
        rancher.remove_container_by_name(container_name)
        # wait up to 10 seconds to verify the deletion
        log.info("Removing: {}...", container_name)
        removed = False
        for _ in range(0, 20):
            time.sleep(0.5)
            container_obj = rancher.get_container_object(container_name)
            if container_obj is None:
                log.info("{} removed", container_name)
                removed = True
                break
            else:
                log.debug("{} still exists", container_name)

        if not removed:
            log.warning("{} still in removal status", container_name)
            response = {
                "batch_id": batch_id,
                "qc_name": qc_name,
                "status": "not_yet_removed",
            }
        else:
            response = {"batch_id": batch_id, "qc_name": qc_name, "status": "removed"}
        return self.response(response)
