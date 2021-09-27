"""
Communicating with docker via rancher

NOTE: to convert the output json and read it:
https://jsonformatter.curiousconcept.com/

API examples:
https://github.com/rancher/validation-tests/tree/master/tests/v2_validation/cattlevalidationtest/core
"""

import json
import time
from typing import Any, Dict, List, Optional, cast

import gdapi
from restapi.env import Env
from restapi.utilities.logs import log

# PERPAGE_LIMIT = 5
# PERPAGE_LIMIT = 50
PERPAGE_LIMIT = 1000

# probably can't do better sinice gdapi is not typed
Container = Any


class Rancher:
    # This receives all config envs that starts with "RESOURCES"
    def __init__(
        self,
        key: str,
        secret: str,
        url: str,
        project: str,
        hub: str,
        hubuser: str,
        hubpass: str,
        localpath: str,
        qclabel: str,
    ) -> None:

        ####################
        # SET URL
        self._url = url
        self._project = project
        # why? explained in http://bit.ly/2BBDJRj
        self._project_uri = f"{url}/projects/{project}/schemas"
        self._hub_uri = hub
        self._hub_credentials = (hubuser, hubpass)
        self._localpath = localpath  # default /nfs/share
        self._qclabel = qclabel
        self._hostlabel = "io.rancher.scheduler.affinity:host_label"

        ####################
        self.connect(key, secret)
        # self.project_handle(project)

    def connect(self, key: str, secret: str) -> None:

        self._client = gdapi.Client(
            url=self._project_uri, access_key=key, secret_key=secret
        )

    # def project_handle(self, project):
    #     return self._client.by_id_project(self._project)

    def hosts(self) -> Dict[str, Dict[str, Any]]:
        """
        'state':'active'
        'agentIpAddress':'130.186.13.150'
        'hostname':'sdc01'
        'driver':'openstack',
        'openstackConfig':{
            'username':'pdonorio'
        'info':{
            'osInfo':{
               'dockerVersion':'Docker version 1.13.1, build 092cba3',
               'kernelVersion':'4.4.0',
               'operatingSystem':'Ubuntu 16.04 LTS'
            'diskInfo':{
               'fileSystems':{
                  '/dev/vda1':{
                     'capacity':29715

            'cpuInfo':{
               'count':8,
            'memoryInfo':{
                'memFree':20287,
                'memTotal':24111,
            "physicalHostId":"1ph3",
        """
        hosts: Dict[str, Dict[str, str]] = {}
        for data in self._client.list_host():
            host = data.get("hostname")
            if not data.get("state") == "active":
                log.warning("Host {} not active", host)
                continue
            hosts[data.get("physicalHostId").replace("p", "")] = {
                "name": host,
                "ip": data.get("agentIpAddress"),
                "provider": data.get("driver"),
            }
        return hosts

    def obj_to_dict(self, obj: Any) -> Dict[str, Any]:

        return cast(Dict[str, Any], json.loads(obj.__repr__().replace("'", '"')))

    def all_containers_available(self) -> List[Container]:
        """
        Handle paginations properly
        https://rancher.com/docs/rancher/v1.5/en/api/v2-beta/
        """

        is_all = False
        containers: List[Container] = []

        while not is_all:
            marker = len(containers)
            onepage = self._client.list_container(
                limit=PERPAGE_LIMIT, marker=f"m{marker}"
            )
            log.debug("Containers list marker: {}", marker)
            pagination = onepage.get("pagination", {})
            # print(pagination)
            is_all = not pagination.get("partial")
            for element in onepage:
                containers.append(element)

        return containers

    def containers(self) -> Dict[str, Any]:
        """
        https://github.com/rancher/gdapi-python/blob/master/gdapi.py#L68
        'io.rancher.container.system': 'true'
        """

        system_label = "io.rancher.container.system"

        containers: Dict[str, Any] = {}
        for info in self.all_containers_available():

            # detect system containers
            try:
                labels = self.obj_to_dict(info.get("labels", {}))
                if labels.get(system_label) is not None:
                    continue
            except BaseException:
                pass

            # labels = info.get('data', {}).get('fields', {}).get('labels', {})
            # info.get('externalId')
            name = info.get("name")
            cid = info.get("uuid")
            if cid is None:
                labels = info.get("labels", {})
                cid = labels.get("io.rancher.container.uuid", None)
            if cid is None:
                log.warning("Container {} launching", name)
                cid = name

            containers[cid] = {
                "name": name,
                "image": info.get("imageUuid"),
                "command": info.get("command"),
                "host": info.get("hostId"),
            }

        return containers

    def list(self) -> Dict[str, Any]:

        resources: Dict[str, Any] = {}
        containers = self.containers()

        for host_id, host_data in self.hosts().items():

            host_name = host_data.get("name")
            if "containers" not in host_data:
                host_data["containers"] = {}

            for container_id, container_data in containers.items():
                if container_data.get("host") == host_id:
                    container_data.pop("host")
                    host_data["containers"][container_id] = container_data

            resources[host_name] = host_data

        return resources

    def recover_logs(self, container_name: str) -> str:
        import websocket as ws

        container = self.get_container_object(container_name)
        if not container:
            log.warning("Container with name {} can't be found", container_name)
            return ""

        logs = container.logs(follow=False, lines=100)
        uri = logs.url + "?token=" + logs.token
        sock = ws.create_connection(uri, timeout=15)
        out = ""
        useless = "/bin/stty: 'standard input': Inappropriate ioctl for device"

        while True:
            try:
                line = sock.recv()
                if useless in line:
                    continue
            except ws.WebSocketConnectionClosedException:
                break
            else:
                out += line + "\n"

        return out

    def catalog_images(self) -> Any:
        """check if container image is there"""
        catalog_url = f"https://{self._hub_uri}/v2/_catalog"
        # print(catalog_url)
        try:
            import requests

            r = requests.get(catalog_url, auth=self._hub_credentials)
            catalog = r.json()
            # print("TEST", catalog)
        except BaseException:
            return None
        else:
            return catalog.get("repositories", {})

    def internal_labels(self, pull: bool = True) -> Dict[str, str]:
        """
        Define Rancher docker labels
        """
        # to launch containers only on selected host(s)
        label_key = "host_type"
        label_value = self._qclabel

        obj = {self._hostlabel: f"{label_key}={label_value}"}

        if pull:
            # force to repull the image every time
            pull_label = "io.rancher.container.pull_image"
            obj[pull_label] = "always"

        return obj

    def run(
        self,
        container_name: str,
        image_name: str,
        private: bool = False,
        extras: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:

        ############
        if private:
            image_name_no_tags = image_name.split(":")[0]
            images_available = self.catalog_images()

            if images_available is None:
                return "Catalog not reachable"

            if image_name_no_tags not in images_available:
                return "Image not found in our private catalog"

            # Add the prefix for private hub if it's there
            image_name = f"{self._hub_uri}/{image_name}"

        ############
        params = {
            "name": container_name,
            "imageUuid": "docker:" + image_name,
            "labels": self.internal_labels(pull=True),
            # entryPoint=['/bin/sh'],
            # command=['sleep', '1234567890'],
        }

        ############
        if extras is not None and isinstance(extras, dict):
            for key, value in extras.items():
                if key not in params:
                    # NOTE: this may print passwords, watch out!
                    params[key] = value

        ############
        from gdapi import ApiError

        try:
            container = self._client.create_container(**params)
        except ApiError as e:
            error_message = f"Rancher failure: {e.__dict__}"
            log.error(error_message)
            return error_message
        else:

            CONTAINERS_VARS = Env.load_variables_group(prefix="containers")
            # Should we wait for the container?
            x = CONTAINERS_VARS.get("wait_stopped") or ""
            wait_stopped = not (x.lower() == "false" or int(x) == 0)

            x = CONTAINERS_VARS.get("wait_running") or ""
            wait_running = not (x.lower() == "false" or int(x) == 0)

            if wait_stopped or wait_running:
                log.info(
                    'Launched container {}" (external id: {})!',
                    container_name,
                    container.externalId,
                )

                # Wait for container to stop...
                while True:
                    co = self.get_container_object(container_name)

                    if not co:
                        log.warning("{} can't be found", container_name)
                        continue

                    log.debug(
                        'Container {}": {} ({}, {}: {})',
                        container_name,
                        co.state,
                        co.transitioning,
                        co.transitioningMessage,
                        co.transitioningProgress,
                    )

                    # Add errors returned by rancher to the errors object:
                    if isinstance(co.transitioningMessage, str):
                        if "error" in co.transitioningMessage.lower():
                            log.error(co.transitioningMessage)

                        if (
                            self._hub_uri in co.transitioningMessage
                            and "no basic auth credentials" in co.transitioningMessage
                        ):
                            log.error(
                                'Message from Rancher: "{}". Possibly you first need to add the registry on the Rancher installation!',
                                co.transitioningMessage,
                            )

                    # Stop loop based on container state:
                    if co.state == "error" or co.state == "erroring":
                        log.error("Error in container!")
                        log.info("Detailed container info {}", co)
                        log.error(co.transitioningMessage)
                        break
                    elif co.state == "stopped" and wait_stopped:
                        # even this does not guarantee success of operation inside container, of course!
                        log.info("Container has stopped!")
                        log.info("Detailed container info {}", co)
                        break
                    elif co.state == "running" and wait_running:
                        log.info("Container is running!")
                        if not not wait_stopped:
                            log.info("Detailed container info {}", co)
                            break

                    else:
                        time.sleep(1)

            # We will not wait for container to be created/running/stopped:
            else:
                log.info(
                    "Launched: {} (external id: {})!",
                    container_name,
                    container.externalId,
                )
            return None

    def get_container_object(self, container_name: str) -> Optional[Any]:
        containers = self.all_containers_available()
        # containers = self._client.list_container(limit=PERPAGE_LIMIT)

        # ####################################
        # # should I clean a little bit?
        # pagination = containers.get('pagination', {})
        # # print(pagination)
        # is_all = not pagination.get('partial')
        # if not is_all:
        #     log.warning('More pages...')

        ####################################
        for element in containers:

            # NOTE: container name is unique in the whole cluster env
            if element.name != container_name:
                continue

            # This patch does not work since Rancher is not able to
            # execute containers with same name, also if deployed
            # on different hosts. Also if verified here, the run will fail later"""
            # 'host_type=qc'
            # labels = element.labels

            # host_label = labels.get(self._hostlabel)

            # if host_label is not None:
            #     expected = self.internal_labels(pull=False).get(self._hostlabel)
            #     if host_label != expected:
            #         log.warning(
            #             "Found {} but deployed on {} (instead of {}). Skipping it",
            #             container_name, host_label, expected
            #         )
            #         continue

            return element

        return None

    def remove_container_by_name(self, container_name: str) -> bool:
        if obj := self.get_container_object(container_name):
            self._client.delete(obj)
            return True
        else:
            log.warning("Did not found container: {}", container_name)
        return False

    def test(self) -> None:
        # client.list_host()
        # client.list_project()
        # client.list_service()
        pass
