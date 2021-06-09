from __future__ import annotations

from dataclasses import dataclass, field
from cloudapi_digitalocean.digitaloceanapi.droplets import Droplets
from cloudapi_digitalocean.common.cloudapiexceptions import *
import json
import threading
import time


class DropletManager:
    def __init__(self):
        self.dropletapi = Droplets()

    def create_new_droplet(
        self,
        name,
        region,
        size,
        image=None,
        ssh_keys=[],
        backups=None,
        ipv6=None,
        private_networking=None,
        vpc_uuid=None,
        user_data=None,
        monitoring=None,
        volumes=[],
        tags=[],
    ):
        arguments = locals()
        del arguments["self"]
        newdroplet = Droplet()
        newdroplet.arguments = DropletArguments(**arguments)
        response = self.dropletapi.create_new_droplet(**arguments)
        if response:
            #
            droplet_data = dict(json.loads(response.content.decode("utf-8"))["droplet"])
            newdroplet.attributes = DropletAttributes(**droplet_data)
        else:
            raise Exception(f"Could not create droplet {name}")
        return newdroplet

    def retrieve_droplet_by_id(self, id):
        """
        Returns a Droplet object containing attributes for a droplet with id.

        Args:
            id ([type]): [description]

        Returns:
            [Droplet]:A droplet object containing attributes for a droplet with object id.
        """
        if not self.does_droplet_id_exist(id):
            raise ErrorDropletNotFound(f"Droplet with id:{id} does not exists")
        newdroplet = Droplet(status="retrieve")
        response = self.dropletapi.retrieve_droplet_by_id(id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            droplet_data = dict(content["droplet"])
            newdroplet.attributes = DropletAttributes(**droplet_data)
        return newdroplet

    def retrieve_droplets_by_name(self, name):
        return_droplets = []
        droplets = self.retrieve_all_droplets()
        for droplet in droplets:
            if droplet.attributes.name == name:
                return_droplets.append(droplet)
        return return_droplets

    def retrieve_all_droplets(self):
        """
        Returns an array of Droplet objects, one for each droplet in digitalocean account.

        Returns:
            [type]: [description]
        """

        # Build list of droplets from api, but take in to account possible pagination.
        droplet_list = []
        page, per_page = 1, 10
        response = self.dropletapi.list_all_droplets(page=page, per_page=per_page)
        content = json.loads(response.content.decode("utf-8"))
        droplet_list.extend(content["droplets"])
        try:
            while content["links"]["pages"]["next"]:
                page = page + 1
                response = self.dropletapi.list_all_droplets(
                    page=page, per_page=per_page
                )
                content = json.loads(response.content.decode("utf-8"))
                droplet_list.extend(content["droplets"])
        except KeyError:
            pass

        # Build and return that Droplet object array.
        droplet_objects = []
        for droplet_item in droplet_list:
            newdroplet = Droplet(status="retrieve")
            newdroplet.attributes = DropletAttributes(**droplet_item)
            droplet_objects.append(newdroplet)
        return droplet_objects

    def retrieve_all_droplets_by_tag(self, tag_name=None):
        """
        Returns an array of Droplet objects, one for each droplet in digitalocean account.

        Returns:
            [type]: [description]
        """

        # If no tag has been provided, return no droplets
        if tag_name == None:
            return []

        # Build list of droplets from api, but take in to account possible pagination.
        droplet_list = []
        page, per_page = 1, 10
        response = self.dropletapi.list_all_droplets_by_tag(
            tag_name=tag_name, page=page, per_page=per_page
        )
        content = json.loads(response.content.decode("utf-8"))
        droplet_list.extend(content["droplets"])
        try:
            while content["links"]["pages"]["next"]:
                page = page + 1
                response = self.dropletapi.list_all_droplets(
                    page=page, per_page=per_page
                )
                content = json.loads(response.content.decode("utf-8"))
                droplet_list.extend(content["droplets"])
        except KeyError:
            pass

        # Build and return that Droplet object array.
        droplet_objects = []
        for droplet_item in droplet_list:
            newdroplet = Droplet(status="retrieve")
            newdroplet.attributes = DropletAttributes(**droplet_item)
            droplet_objects.append(newdroplet)
        return droplet_objects

    def delete_droplet(self, droplet: Droplet):
        """
        Given a Droplet object, this method deletes the droplet represnted by the object.

        Args:
            droplet (Droplet): [description]
        """
        self.delete_droplet_by_id(droplet.attributes.id)

    def delete_droplet_by_id(self, id):
        if self.does_droplet_id_exist(id):
            response = self.dropletapi.delete_droplet_id(id)

    def delete_droplets_by_tag(self, tag_name=None):
        if not tag_name == None:
            response = self.dropletapi.delete_droplet_tag(tag_name=tag_name)

    def does_droplet_id_exist(self, id):
        droplets = self.retrieve_all_droplets()
        for droplet in droplets:
            if str(droplet.attributes.id) == str(id):
                return True
        return False


@dataclass
class DropletAttributes:
    id: int = None
    name: str = None
    memory: int = None
    vcpus: int = None
    disk: int = None
    locked: bool = None
    created_at: str = None
    status: str = None
    backup_ids: list = field(default_factory=list)
    snapshot_ids: list = field(default_factory=list)
    features: list = field(default_factory=list)
    region: object = field(default_factory=list)
    image: object = field(default_factory=list)
    size: object = field(default_factory=list)
    size_slug: str = None
    networks: object = field(default_factory=list)
    kernel: object = field(default_factory=list)
    next_backup_window: object = field(default_factory=list)
    tags: list = field(default_factory=list)
    volume_ids: list = field(default_factory=list)
    vpc_uuid: list = field(default_factory=list)


@dataclass
class DropletArguments:

    name: str = None
    region: str = None
    size: str = None
    image: object = None
    ssh_keys: list = field(default_factory=list)
    backups: bool = None
    ipv6: bool = None
    private_networking: bool = None
    vpc_uuid: str = None
    user_data: str = None
    monitoring: bool = None
    volumes: list = field(default_factory=list)
    tags: list = field(default_factory=list)


@dataclass
class DropletLastAction:
    id: int = None
    status: str = None
    type: str = None
    started_at: str = None
    completed_at: str = None
    resource_id: int = None
    resource_type: str = None
    region: object = None
    region_slug: str = None


class Droplet:
    def __init__(self, status=None):
        self.arguments = DropletArguments()
        self.attributes = DropletAttributes()
        self.lastaction = DropletLastAction()
        self.attributes.status = status
        self.dropletapi = Droplets()
        self.update_on_active_status()

    def update(self):
        """
        Updates the Droplet attributes data class with the latest droplet information at digital ocean.
        """
        response = self.dropletapi.retrieve_droplet_by_id(self.attributes.id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            droplet_data = dict(content["droplet"])
            self.attributes = DropletAttributes(**droplet_data)

    def update_on_active_status(self):
        """
        A freshly created droplet will need time to completely boot up and be active.
        Information like IP addresses are not available untill the droplet is active.
        Here we start a background thread that waits for the droplet to become active and then update the droplet attributes.
        """

        def update_status():
            while not self.attributes.status == "active":
                if self.attributes.status == None:
                    time.sleep(10)
                elif self.attributes.status == "new":
                    self.update()
                    time.sleep(10)
                else:
                    break

        thread = threading.Thread(target=update_status, args=())
        thread.start()

    def update_droplet_action(self):
        """
        Updates the Droplet lastaction data class with the latest droplet action information at digital ocean.
        """
        response = self.dropletapi.retrieve_droplet_action(
            self.attributes.id, self.lastaction.id
        )
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)

    def update_on_active_action(self):
        """
        A freshly created droplet will need time to completely boot up and be active.
        Information like IP addresses are not available untill the droplet is active.
        Here we start a background thread that waits for the droplet to become active and then update the droplet attributes.
        """

        def update_action():
            while (self.lastaction.status == None) or (
                self.lastaction.status == "in-progress"
            ):
                if not self.lastaction.status in ["completed", "errored"]:
                    time.sleep(10)
                    self.update_droplet_action()
                else:
                    break

        thread = threading.Thread(target=update_action, args=())
        thread.start()

    def reboot(self):
        droplet_id = self.attributes.id
        response = self.dropletapi.reboot_droplet(droplet_id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)
            self.update_on_active_action()

    def powercycle(self):
        droplet_id = self.attributes.id
        response = self.dropletapi.powercycle_droplet(droplet_id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)
            self.update_on_active_action()

    def shutdown(self):
        droplet_id = self.attributes.id
        response = self.dropletapi.shutdown_droplet(droplet_id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)
            self.update_on_active_action()

    def poweroff(self):
        droplet_id = self.attributes.id
        response = self.dropletapi.poweroff_droplet(droplet_id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)
            self.update_on_active_action()

    def poweron(self):
        droplet_id = self.attributes.id
        response = self.dropletapi.poweron_droplet(droplet_id)
        if response:
            content = json.loads(response.content.decode("utf-8"))
            action_data = dict(content["action"])
            self.lastaction = DropletLastAction(**action_data)
            self.update_on_active_action()


if __name__ == "__main__":
    dmanager = DropletManager()

    # a_droplet = dmanager.create_new_droplet(
    #    name="example.com",
    #    region="nyc3",
    #    size="s-1vcpu-1gb",
    #    image="ubuntu-16-04-x64",
    #    ssh_keys=[],
    #    backups=False,
    #    ipv6=True,
    #    user_data=None,
    #    private_networking=None,
    #    volumes=None,
    #    tags=["banabas"],
    # )

    # while not a_droplet.attributes.status == "active":
    #    time.sleep(5)
    #   print("waiting")

    # print("-----arguments-----")
    # print(a_droplet.arguments)
    # print("-------ATTRIBUTES-------------")
    # print(a_droplet.attributes)
    ## try:
    #    newdroplet = dmanager.retrieve_droplet_by_id(2496119371)
    # except DropletNotFound:
    #    print("Droplet wasnt found")
    # print(newdroplet.attributes)
    # print(newdroplet.arguments)
    # 249699371
    # droplets = dmanager.retrieve_all_droplets_by_tag("banabas")
    # for droplet in droplets:
    #    print(f"Deleteing droplet with id {droplet.attributes.id}")
    #    dmanager.delete_droplet(droplet)

    # print(dmanager.does_droplet_id_exist(249699371))

    # dmanager.delete_droplets_by_tag("banabas")
    droplets = dmanager.retrieve_droplets_by_name("example.com")
    for droplet in droplets:
        print(droplet.attributes.name)
        droplet.powercycle()
        while (droplet.lastaction.status == None) or (
            droplet.lastaction.status == "in-progress"
        ):
            time.sleep(5)
            print("waiting for action...")
        print(droplet.lastaction)
    print("finished")
