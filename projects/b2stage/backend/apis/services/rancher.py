# -*- coding: utf-8 -*-

"""
Communicating with docker via rancher

NOTE: to convert the output json and read it:
https://jsonformatter.curiousconcept.com/

API examples:
https://github.com/rancher/validation-tests/tree/master/tests/v2_validation/cattlevalidationtest/core
"""

from utilities.logs import get_logger
log = get_logger(__name__)


class Rancher(object):

    def __init__(self, key, secret, url, project, hub):

        ####################
        # SET URL
        self._url = url
        self._project = project
        # why? explained in http://bit.ly/2BBDJRj
        self._project_uri = "%s/projects/%s/schemas" % (url, project)
        self._hub_uri = hub

        ####################
        self.connect(key, secret)
        # self.project_handle(project)

    def connect(self, key, secret):
        import gdapi
        self._client = gdapi.Client(
            url=self._project_uri,
            access_key=key, secret_key=secret)

    # def project_handle(self, project):
    #     return self._client.by_id_project(self._project)

    def hosts(self):
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
        hosts = {}
        for data in self._client.list_host():
            host = data.get('hostname')
            if not data.get('state') == 'active':
                log.warning("Host %s not active", host)
                continue
            hosts[data.get('physicalHostId').replace('p', '')] = {
                'name': host,
                'ip': data.get('agentIpAddress'),
                'provider': data.get('driver'),
            }
        return hosts

    def obj_to_dict(self, obj):
        import json
        return json.loads(obj.__repr__().replace("'", '"'))

    def containers(self):
        """
        https://github.com/rancher/gdapi-python/blob/master/gdapi.py#L68
        'io.rancher.container.system': 'true'
        """

        system_label = 'io.rancher.container.system'

        containers = {}
        for info in self._client.list_container():

            # detect system containers
            try:
                labels = self.obj_to_dict(info.get('labels', {}))
                if labels.get(system_label) is not None:
                    # log.verbose("Skipping sycontainer: %s" % system_label)
                    continue
            except BaseException:
                pass

            # labels = info.get('data', {}).get('fields', {}).get('labels', {})
            # info.get('externalId')
            name = info.get('name')
            cid = info.get('uuid')
            if cid is None:
                labels = info.get('labels', {})
                cid = labels.get('io.rancher.container.uuid', None)
            if cid is None:
                log.warning("Container %s launching", name)
                log.pp(info)
                break
                cid = name

            containers[cid] = {
                'name': name,
                'image': info.get('imageUuid'),
                'command': info.get('command'),
                'host': info.get('hostId'),
            }

        return containers

    def list(self):

        resources = {}
        containers = self.containers()
        ckey = 'containers'

        for host_id, host_data in self.hosts().items():

            host_name = host_data.get('name')
            if ckey not in host_data:
                host_data[ckey] = {}

            for container_id, container_data in containers.items():
                if container_data.get('host') == host_id:
                    container_data.pop('host')
                    host_data['containers'][container_id] = container_data

            resources[host_name] = host_data

        return resources

    def run(self, container_name, image_name, private=False):

        if private:
            image_name = "%s/%s" % (self._hub_uri, image_name)

        from gdapi import ApiError

        try:
            pass
            self._client.create_container(
                name=container_name,
                imageUuid='docker:' + image_name,
                # entryPoint=['/bin/sh'],
                # command=['echo', 'it', 'works'],
                # command=['sleep', '1234567890'],
            )
        except ApiError as e:
            log.error("Rancher fail:")
            log.pp(e.__dict__)
            return e.__dict__
        else:
            return None

    def get_container_object(self, container_name):
        for element in self._client.list_container():
            # NOTE: container name is unique in the whole cluster env
            if element.name == container_name:
                # print("found", element)
                return element

        return None

    def remove_container_by_name(self, container_name):
        obj = self.get_container_object(container_name)
        if obj is not None:
            self._client.delete(obj)
            return True
        return False

    def test(self):
        # client.list_host()
        # client.list_project()
        # client.list_service()
        pass
