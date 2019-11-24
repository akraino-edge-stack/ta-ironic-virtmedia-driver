# Copyright 2019 Nokia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ironic.common import boot_devices
from ironic.common.i18n import _
from ironic.conductor import utils as manager_utils
from ironic.drivers.modules import ipmitool
from ironic_virtmedia_driver.vendors.ironic_virtmedia_hw import IronicVirtMediaHW
from ironic_virtmedia_driver import virtmedia_exception

from redfish import redfish_client, AuthMethod
from redfish.rest.v1 import ServerDownOrUnreachableError

class DELL(IronicVirtMediaHW):
    def __init__(self, log):
        super(DELL, self).__init__(log)
        self.remote_share = '/bootimages/'
        self.idrac_location = '/redfish/v1/Managers/iDRAC.Embedded.1/'

    def _init_connection(self, driver_info):
        """Get connection info and init rest_object"""
        host = 'https://' + driver_info['address']
        user = driver_info['username']
        password = driver_info['password']
        redfishclient = None
        self.log.debug("Init connection: user: %s, passwd: %s, host: %s", user, password, host)
        try:
            redfishclient = redfish_client(base_url=host, \
                  username=user, password=password)
            redfishclient.login(auth=AuthMethod.SESSION)
        except ServerDownOrUnreachableError as error:
            operation = _("iDRAC not responding")
            raise virtmedia_exception.VirtmediaOperationError(
                operation=operation, error=error)
        except Exception as error:
            operation = _("Failed to login to iDRAC")
            raise virtmedia_exception.VirtmediaOperationError(
                operation=operation, error=error)
        return redfishclient

    @staticmethod
    def _check_success(response):
        if response.status >= 200 and response.status < 300:
            return True
        else:
            try:
                _ = response.dict
                raise virtmedia_exception.VirtmediaOperationError("Response status is %d, %s"% (response.status, response.dict["error"]["@Message.ExtendedInfo"][0]["MessageId"].split(".")))
            except Exception:
                raise virtmedia_exception.VirtmediaOperationError("Response status is not 200, %s"% response)

    def _check_supported_idrac_version(self, connection):
        response = connection.get('%s/VirtualMedia/CD'%self.idrac_location)
        self._check_success(response)
        data = response.dict
        for i in data.get('Actions', []):
            if i == "#VirtualMedia.InsertMedia" or i == "#VirtualMedia.EjectMedia":
                return True
        raise virtmedia_exception.VirtmediaOperationError("Unsupported version of iDRAC, please update before continuing")

    def _get_virtual_media_devices(self, connection):
        idr = connection.get("%s" % self.idrac_location)
        self._check_success(idr)
        try:
            virtual_media = connection.get(idr.dict["VirtualMedia"]["@odata.id"])
            self._check_success(virtual_media)
        except KeyError:
            self.log.error("Cannot find a single virtual media device")
            raise virtmedia_exception.VirtmediaOperationError("Cannot find any virtual media device on the server")
        return virtual_media.dict["Members"]

    def _umount_virtual_device(self, connection, media_uri):
        self.log.debug("Unmount")
        unmount_location = media_uri + "/Actions/VirtualMedia.EjectMedia"
        resp = connection.post(unmount_location, body={})
        self._check_success(resp)

    def _mount_virtual_device(self, connection, media_uri, image_location):
        self.log.debug("Mount")
        mount_location = media_uri + "/Actions/VirtualMedia.InsertMedia"
        payload = {'Image': image_location, 'Inserted':True, 'WriteProtected':True}
        resp = connection.post(mount_location, body=payload)
        self._check_success(resp)

    def _unmount_all(self, connection):
        medias = self._get_virtual_media_devices(connection)
        for media in medias:
            uri = media.get("@odata.id", None)
            if not uri or connection.get(uri).dict["ConnectedVia"] == "NotConnected":
                continue
            self._umount_virtual_device(connection, uri)

    def _find_first_media(self, connection, typeinfo):
        medias = self._get_virtual_media_devices(connection)
        for media in medias:
            response = connection.get(media["@odata.id"])
            if typeinfo in response.dict["MediaTypes"]:
                return media["@odata.id"]
        return None

    def _mount_virtual_cd(self, connection, image_location):
        self._unmount_all(connection)
        self.log.debug("Mount")
        media_uri = self._find_first_media(connection, "DVD")
        self._mount_virtual_device(connection, media_uri, image_location)

    def attach_virtual_cd(self, image_filename, driver_info, task):
        connection = None
        try:
            self.log.debug("attach_virtual_cd")
            connection = self._init_connection(driver_info)
            self._check_supported_idrac_version(connection)
            image_location = 'http://' + str(driver_info['provisioning_server']) + ':' + str(driver_info['provisioning_server_http_port']) + self.remote_share + image_filename
            self._mount_virtual_cd(connection, image_location)

            connection.logout()
            return True
        except Exception:
            if connection:
                connection.logout()
            raise

    def detach_virtual_cd(self, driver_info, task):
        connection = None
        try:
            self.log.debug("detach_virtual_cd")
            connection = self._init_connection(driver_info)
            self._check_supported_idrac_version(connection)
            self._unmount_all(connection)
            connection.logout()
            return True
        except Exception:
            if connection:
                connection.logout()
            raise

    def set_boot_device(self, task):
        try:
            #BMC boot flag valid bit clearing 1f -> all bit set
            #P 420 of ipmi spec
            # https://www.intel.com/content/www/us/en/servers/ipmi/ipmi-second-gen-interface-spec-v2-rev1-1.html
            cmd = '0x00 0x08 0x03 0x1f'
            ipmitool.send_raw(task, cmd)
            self.log.info('Disable timeout for booting')
        except Exception as err:
            self.log.warning('Failed to disable booting options: %s', str(err))
        #For time being lets do the boot order with ipmitool since, well dell doesn't provide open support
        #for this.
        try:
          # 0x00 0x08 0x05 0x80 0x20: chassis|set|bootdev|for next boot only|remote CD
          # other options for device (per ipmitool's "ipmi_chassis.c"):
          # 04: PXE
          # 08: HDD
          # 0c: Safe
          # 10: Diag
          # 14: CDROM
          # 18: Setup
          # 1c: Remote FDD
          # 24: Remote primary media
          # 2c: Remote HDD
          # 3c: FDD
          ipmitool.send_raw(task, '0x00 0x08 0x05 0x80 0x20 0x00 0x00 0x00')
          self.log.info('Set next boot to remote media')
        except Exception as err:
            self.log.warning('Failed to set next boot to remote media: %s', str(err))
