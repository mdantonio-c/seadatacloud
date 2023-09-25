"""
Orders from production data to be temporary downloadable with a zip file

# order a zip @async
POST /api/order/<OID>
    pids=[PID1, PID2, ...]

# creates the iticket/link to download
PUT /api/order/<OID> -> return iticket_code

# download the file
GET /api/order/<OID>?code=<iticket_code>

# remove the zip and the ticket
DELETE /api/order/<OID>

"""

#################
# IMPORTS
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from irods.exception import NetworkException
from restapi import decorators
from restapi.config import get_backend_url
from restapi.connectors import celery
from restapi.exceptions import BadRequest, NotFound, ServiceUnavailable, Unauthorized
from restapi.rest.definition import Response
from restapi.services.authentication import User
from restapi.services.download import Downloader
from restapi.utilities.logs import log
from seadata.connectors import irods
from seadata.connectors.rabbit_queue import log_into_queue, prepare_message
from seadata.endpoints import (
    MOUNTPOINT,
    ORDERS_COLL,
    ORDERS_DIR,
    EndpointsInputSchema,
    SeaDataEndpoint,
)

TMPDIR = "/tmp"


#################
# REST CLASSES
class DownloadBasketEndpoint(SeaDataEndpoint):

    labels = ["order"]

    def get_filename_from_type(self, order_id: str, ftype: str) -> Optional[str]:
        if len(ftype) < 2:
            return None

        if ftype[0] == "0":
            restricted = False
        elif ftype[0] == "1":
            restricted = True
        else:
            log.warning("Unexpected flag in ftype {}", ftype)
            return None
        try:
            index = int(ftype[1:])
        except ValueError:
            log.warning("Unable to extract numeric index from ftype {}", ftype)

        if index == 0:
            return self.get_order_zip_file_name(
                order_id, restricted=restricted, index=None
            )

        return self.get_order_zip_file_name(
            order_id, restricted=restricted, index=index
        )

    @decorators.endpoint(
        path="/orders/<order_id>/download/<ftype>/c/<code>",
        summary="Download an order",
        responses={
            200: "The order with all files compressed",
            404: "Order does not exist",
        },
    )
    def get(self, order_id: str, ftype: str, code: str) -> Response:
        """downloading (not authenticated)"""
        log.info("Order request: {} (code '{}')", order_id, code)
        json = {"order_id": order_id, "code": code}
        msg = prepare_message(self, json=json, user="anonymous", log_string="start")
        log_into_queue(self, msg)

        # log.info("DOWNLOAD DEBUG 1: {} (code '{}')", order_id, code)

        order_path = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)

        zip_file_name = self.get_filename_from_type(order_id, ftype)

        if zip_file_name is None:
            raise BadRequest(f"Invalid file type {ftype}")

        zip_path = Path(order_path, zip_file_name)

        error = f"Order '{order_id}' not found (or no permissions)"

        log.debug("Checking zip path: {}", zip_path)
        if not zip_path.is_file():
            log.error("File not found {}", zip_path)
            raise NotFound(error)

        # check code validity
        try:
            filepath_from_token = self.read_token(code)
        except Exception as e:
            log.error(e)
            raise Unauthorized("Provided code is invalid")

        # check if the token is related to that specific file
        expected_filepath = f"{order_id}/{zip_file_name}"
        if expected_filepath != filepath_from_token:
            log.error(
                "path from code {} does not match the requested file {}",
                filepath_from_token,
                expected_filepath,
            )
            raise NotFound(error)

        # TODO headers are still needed?
        # headers = {
        #     "Content-Transfer-Encoding": "binary",
        #     "Content-Disposition": f"attachment; filename={zip_file_name}",
        # }
        msg = prepare_message(self, json=json, log_string="end", status="sent")
        log_into_queue(self, msg)

        log.info("Request download for path: {}", zip_path)

        return Downloader.send_file_streamed(
            zip_file_name,
            subfolder=order_path,
        )


class BasketEndpoint(SeaDataEndpoint):

    labels = ["order"]

    @decorators.auth.require()
    @decorators.endpoint(
        path="/orders/<order_id>",
        summary="List orders",
        responses={200: "The list of zip files available"},
    )
    def get(self, order_id: str, user: User) -> Response:
        """listing, not downloading"""

        log.debug("GET request on orders")
        msg = prepare_message(self, json=None, log_string="start")
        log_into_queue(self, msg)

        order_path = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
        log.debug("Order path: {}", order_path)
        if not order_path.exists():
            raise NotFound(f"Order '{order_id}': not existing")

        ##################
        ls = self.list(order_path, detailed=True)

        u = self.get_order_zip_file_name(order_id, restricted=False, index=1)
        # if a split unrestricted zip exists, skip the unsplitted file
        if u in ls:
            u = self.get_order_zip_file_name(order_id, restricted=False, index=None)
            ls.pop(u, None)

        r = self.get_order_zip_file_name(order_id, restricted=True, index=1)
        # if a split restricted zip exists, skip the unsplitted file
        if r in ls:
            r = self.get_order_zip_file_name(order_id, restricted=True, index=None)
            ls.pop(r, None)

        response = []

        for _, data in ls.items():
            name = data.get("name")

            if not name:  # pragma: no cover
                continue

            if name.endswith(".bak") or name.endswith(".seed"):
                continue

            path = data.get("path")
            if not path:  # pragma: no cover
                log.warning("Wrong entry, missing path: {}", data)
                continue
            else:
                # get download url as metadata
                # check if there is a seed file. If not it means that no urls are available for that file
                seed_file = Path(path, ".seed")
                if not seed_file.exists():
                    data["URL"] = ""
                else:
                    # NOTE with irods the code was a metadata of the file. This part replace that function
                    # NOTE that the tickets are different every time they are generated, but they are based on the same seed. If the ticket has always to be the same, this approach has to be changed and it has to be saved in the local db
                    # check if it is restricted
                    restricted = "unrestricted" not in name
                    # check if it has an index
                    label = "restricted" if restricted else "unrestricted"
                    stripped_name = name.strip(f"order_{order_id}_{label}").strip(
                        ".zip"
                    )
                    index = stripped_name if stripped_name else None
                    files = {name: data}
                    download_metadata = self.get_download(
                        order_id,
                        Path(path),
                        files,
                        restricted,
                        index,
                        get_only_url=True,
                    )
                    data["URL"] = download_metadata["url"]

                response.append(data)

        msg = prepare_message(self, log_string="end", status="completed")
        log_into_queue(self, msg)
        return self.response(response)

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/orders",
        summary="Request one order preparation",
        responses={200: "Asynchronous request launched"},
    )
    def post(self, user: User, **json_input: Any) -> Response:

        log.debug("POST request on orders")
        msg = prepare_message(self, json=json_input, log_string="start")
        log_into_queue(self, msg)

        params = json_input.get("parameters", {})
        if len(params) < 1:
            raise BadRequest("missing parameters")

        key = "order_number"
        order_id = params.get(key)
        if order_id is None:
            raise BadRequest(f"Order ID '{key}': missing")
        order_id = str(order_id)

        # ##################
        # Get filename from json input. But it has to follow a
        # specific pattern, so we ignore client input if it does not...
        filename = f"order_{order_id}_unrestricted"
        key = "file_name"
        if key in params and not params[key] == filename:
            log.warning(
                "Client provided wrong filename ({}), will use: {}",
                params[key],
                filename,
            )
        params[key] = filename

        ##################
        # PIDS: can be empty if restricted
        key = "pids"
        pids = params.get(key, [])

        ##################
        # Create the path
        log.info("Order request: {}", order_id)
        order_path = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
        log.debug("Order path: {}", order_path)
        if not order_path.exists():
            # Create the path and set permissions
            order_path.mkdir(parents=True)

        ##################
        # Does the zip already exists?
        zip_file_name = filename + ".zip"
        zip_path = Path(order_path, zip_file_name)
        if zip_path.exists():
            # give error here
            # return {order_id: 'already exists'}
            # json_input['status'] = 'exists'
            json_input["parameters"] = {"status": "exists"}
            return self.response(json_input)

        ################
        # ASYNC
        if len(pids) > 0:
            log.info("Submit async celery task")
            c = celery.get_instance()
            task = c.celery_app.send_task(
                "unrestricted_order",
                args=[order_id, str(order_path), zip_file_name, json_input],
            )
            log.info("Async job: {}", task.id)
            return self.return_async_id(task.id)

        return self.response({"status": "enabled"})

    @decorators.auth.require()
    @decorators.endpoint(
        path="/orders/<order_id>",
        summary="Request a link to download an order (if already prepared)",
        responses={200: "The link to download the order (expires in 2 days)"},
    )
    def put(self, order_id: str, user: User) -> Response:

        log.info("Order request: {}", order_id)
        msg = prepare_message(self, json={"order_id": order_id}, log_string="start")
        log_into_queue(self, msg)

        try:
            order_path = MOUNTPOINT.joinpath(ORDERS_DIR, order_id)
            log.debug("Order path: {}", order_path)
        except BaseException:
            raise NotFound("Order not found")

        response = []

        files_in_order = self.list(order_path, detailed=True)

        # Going through all possible file names of zip files

        # unrestricted zip
        # info = self.get_download(
        #     imain, order_id, order_path, files_in_irods,
        #     restricted=False, index=None)
        # if info is not None:
        #     response.append(info)

        # checking for split unrestricted zip
        info = self.get_download(
            order_id, order_path, files_in_order, restricted=False, index=1
        )

        # No split zip found, looking for the single unrestricted zip
        if info is None:
            info = self.get_download(
                order_id,
                order_path,
                files_in_order,
                restricted=False,
                index=None,
            )
            if info is not None:
                response.append(info)
        # When found one split, looking for more:
        else:
            response.append(info)
            for index in range(2, 100):
                info = self.get_download(
                    order_id,
                    order_path,
                    files_in_order,
                    restricted=False,
                    index=index,
                )
                if info is not None:
                    response.append(info)

        # checking for split restricted zip
        info = self.get_download(
            order_id, order_path, files_in_order, restricted=True, index=1
        )

        # No split zip found, looking for the single restricted zip
        if info is None:
            info = self.get_download(
                order_id,
                order_path,
                files_in_order,
                restricted=True,
                index=None,
            )
            if info is not None:
                response.append(info)
        # When found one split, looking for more:
        else:
            response.append(info)
            for index in range(2, 100):
                info = self.get_download(
                    order_id,
                    order_path,
                    files_in_order,
                    restricted=True,
                    index=index,
                )
                if info is not None:
                    response.append(info)

        if len(response) == 0:
            raise NotFound(f"Order '{order_id}' not found (or no permissions)")

        msg = prepare_message(self, log_string="end", status="enabled")
        log_into_queue(self, msg)

        return self.response(response)

    @decorators.auth.require()
    @decorators.use_kwargs(EndpointsInputSchema)
    @decorators.endpoint(
        path="/orders",
        summary="Remove one or more orders",
        responses={200: "Async job submitted for orders removal"},
    )
    def delete(self, user: User, **json_input: Any) -> Response:

        local_order_path = MOUNTPOINT.joinpath(ORDERS_DIR)
        log.debug("Order path: {}", local_order_path)

        c = celery.get_instance()
        task = c.celery_app.send_task(
            "delete_orders", args=[str(local_order_path), json_input]
        )
        log.info("Async job: {}", task.id)
        return self.return_async_id(task.id)
