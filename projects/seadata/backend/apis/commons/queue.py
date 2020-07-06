"""
close the rabbit connection when the HTTP API finish
    - catch the sigkill?
    - deconstructor in the flask ext?
    - check connection errors
"""

from restapi.env import Env
from restapi.utilities.logs import log

QUEUE_SERVICE = "rabbit"
QUEUE_VARS = Env.load_group(label=QUEUE_SERVICE)

"""
:param instance: The Endpoint.
:param params: The kv pairs to be in the log message.

"""


def prepare_message(instance, user=None, isjson=False, **params):
    """
{ # start
    "request_id": # build a hash for the current request
    "edmo_code": # which eudat centre is
    "json": "the json input file" # parameters maris
    "datetime":"20180328T10:08:30", # timestamp
    "ip_number":"544.544.544.544", # request.remote_addr
    "program":"program/function name", # what's mine?
    "url" ?
    "user":"username", # from maris? marine id?
    "log_string":"start"
}

{ # end
    "datetime":"20180328T10:08:30",
    "request_id":"from the json input file",
    "ip_number":"544.544.544.544",
    "program":"program of function = name",
    "user":"username",
    "edmo_code":"sample 353",
    "log_string":"end"
}
    """
    logmsg = dict(params)

    instance_id = str(id(instance))
    logmsg["request_id"] = instance_id
    # logmsg['request_id'] = instance_id[len(instance_id) - 6:]

    from seadata.apis.commons.seadatacloud import seadata_vars

    logmsg["edmo_code"] = seadata_vars.get("edmo_code")

    from datetime import datetime

    logmsg["datetime"] = datetime.now().strftime("%Y%m%dT%H:%M:%S")

    if isjson:
        return logmsg  # TODO Why this? Why does isjson exist at all?

    from restapi.services.authentication import BaseAuthentication as Service

    ip = Service.get_remote_ip()
    logmsg["ip_number"] = ip

    from flask import request

    # http://localhost:8080/api/pids/<PID>
    import re

    endpoint = re.sub(r"https?://[^\/]+", "", request.url)
    logmsg["program"] = request.method + ":" + endpoint
    if user is None:
        user = "import_manager"  # TODO: True? Not sure!
    logmsg["user"] = user

    return logmsg


"""
Send a log message into the logging queue, so that it
ends up in ElasticSearch.

It needs the following info from config:

* RABBIT_EXCHANGE (where the message is sent to to be
    distributed to a queue).
* RABBIT_QUEUE (will be used as routing key to route the
    message to the correct queue).
* RABBIT_APP_NAME (will determine the name of the
    ElasticSearch index where the message will be stored.
    If not provided, the value of RABBIT_QUEUE will be used).

:param instance: Instance of the Logging service from rapydo.
:param dictionary_message: The message to be logged (as JSON).
"""


def log_into_queue(instance, dictionary_message):
    """ RabbitMQ in the EUDAT infrastructure """

    # temporary disabled
    return False

    log.verbose("LOG MESSAGE to be passed to log-queue: {}", dictionary_message)

    current_exchange = QUEUE_VARS.get("exchange")
    routing_key = QUEUE_VARS.get("queue")
    app_name = QUEUE_VARS.get("app_name")

    if app_name is None:
        app_name = routing_key

    log.debug(
        'Log-queue service: exchange "{}", routing key "{}", app name "{}"',
        current_exchange,
        routing_key,
        app_name,
    )

    try:

        # Error seem to be raised if we don't refresh connection?
        # https://github.com/pika/pika/issues/397#issuecomment-35322410
        # --> Has to be handled in rapydo/http-api, where connection is defined!

        msg_queue = instance.get_service_instance(QUEUE_SERVICE)
        headers = {
            "app_name": app_name,
            "filter_code": "de.dkrz.seadata.filter_code.json",
        }
        log.verbose('Retrieved instance of log-queue service "{}"', QUEUE_SERVICE)
        msg_queue.write_to_queue(
            dictionary_message, routing_key, exchange=current_exchange, headers=headers
        )

    except BaseException as e:
        log.error("Failed to log:\n{}({})", e.__class__.__name__, e)
    else:
        log.verbose("Log message passed to log-queue service.")
        # log.verbose("{}: sent msg '{}'", routing_key, dictionary_message)

        # NOTE: bad! all connections would result in closed
        # # close resource
        # msg_queue.close()
        # FIXME: Close it elsewhere! Catching sigkill for example.

    return True
