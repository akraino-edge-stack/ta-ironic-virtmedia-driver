# Copyright 2019 Cachengo
# Copyright 2019 ENEA
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

import time

from ironic.conductor import utils as manager_utils
from ironic.common import boot_devices
from ironic.common import exception
from ironic.drivers.modules import ipmitool

from .ampere import AmpereIronicVirtMediaHW

RAW_CHECK_NFS_SERVICE_STATUS = '0x32 0xd8 0x06 0x01 0x01 0x00'

RAW_GET_VMEDIA_DEVICE_COUNT = '0x32 0xca %s'     # (type)
RAW_SET_VMEDIA_DEVICE_COUNT = '0x32 0xcb %s %s'  # (type, count)
( VMDEIA_DEVICE_TYPE_CD,
  VMDEIA_DEVICE_TYPE_FD,
  VMDEIA_DEVICE_TYPE_HD ) = ( '0x04', '0x05', '0x06' )

RAW_GET_VMEDIA_MOUNT_STATUS = '0x32 0xca 0x00'
RAW_SET_VMEDIA_MOUNT_STATUS = '0x32 0xcb 0x00 %s'

RAW_GET_VMEDIA_STATUS = '0x32 0xca 0x08'
RAW_SET_VMEDIA_STATUS = '0x32 0xcb 0x08 %s'
RAW_RESTART_VMEDIA =    '0x32 0xcb 0x0a 0x01'

# Remote Image Service commands
RAW_RESTART_RIS_CD =   '0x32 0x9f 0x01 0x0b 0x01'
RAW_SET_RIS_NFS =      '0x32 0x9f 0x01 0x05 0x00 0x6e 0x66 0x73'
RAW_SET_RIS_NFS_IP =   '0x32 0x9f 0x01 0x02 0x00 %s'
RAW_SET_RIS_NFS_PATH = '0x32 0x9f 0x01 0x01 0x01 %s'
RAW_SET_RIS_PROGRESS = '0x32 0x9f 0x01 0x01 0x00 %s'
RAW_CLEAR_RIS_CONFIG = '0x32 0x9f 0x01 0x0d'
RAW_RESTART_RIS =      '0x32 0x9f 0x08 0x0b'

RAW_GET_MOUNTED_IMG_COUNT = '0x32 0xd8 0x00 0x01'
RAW_SET_IMG_NAME =  '0x32 0xd7 0x01 0x01 0x01 0x01 %s'
RAW_STOP_REDIRECT = '0x32 0xd7 0x01 0x01 0x01 0x00 %s'


class FALCON(AmpereIronicVirtMediaHW):
    def __init__(self, log):
        super(FALCON, self).__init__(log)

    def get_disk_attachment_status(self, task):
        # Check NFS Service Status
        try:
            cmd = RAW_CHECK_NFS_SERVICE_STATUS
            out, err = ipmitool.send_raw(task, cmd)
            _image_name = str(bytearray.fromhex(out.replace('\n', '').strip()))
            return 'mounted'
        except Exception:
            return 'nfserror'

    def _get_virtual_media_device_count(self, task, devicetype):
        try:
            _num_inst = 0
            # Get num of enabled devices
            if devicetype == 'CD':
                _devparam = VMDEIA_DEVICE_TYPE_CD
                self.log.debug('Get virtual CD count')
            elif devicetype == 'FD':
                _devparam = VMDEIA_DEVICE_TYPE_FD
                self.log.debug('Get virtual FD count')
            elif devicetype == 'HD':
                _devparam = VMDEIA_DEVICE_TYPE_HD
                self.log.debug('Get virtual HD count')
            else:
                self.log.warning('Unknown device type "%s"' % devicetype)
                return _num_inst

            cmd = RAW_GET_VMEDIA_DEVICE_COUNT % _devparam
            out, err = ipmitool.send_raw(task, cmd)
            _num_inst = int(out.strip())
            self.log.debug('Number of enabled %s devices is %d' % (devicetype, _num_inst))
            return _num_inst
        except Exception as err:
            # Drive might not be mounted to start with
            self.log.debug('Exception when getting number of enabled %s devices. error: %s' % (devicetype, str(err)))


    def _set_virtual_media_device_count(self, task, devicetype, devicecount):
        if not 0 <= devicecount <= 4:
            self.log.warning('Number of devices must be in range 0 to 4')
            return False

        if devicetype == 'CD':
            _devparam = VMDEIA_DEVICE_TYPE_CD
            self.log.debug('Setting virtual CD count to %d' % devicecount)
        elif devicetype == 'HD':
            _devparam = VMDEIA_DEVICE_TYPE_HD
            self.log.debug('Setting virtual HD count to %d' % devicecount)
        else:
            self.log.warning('_set_virtual_media_device_count: Unknown device type "%s"' % devicetype)
            return False

        try:
            cmd = RAW_SET_VMEDIA_DEVICE_COUNT % (_devparam, hex(devicecount))
            ipmitool.send_raw(task, cmd)

            _conf_device_num = self._get_virtual_media_device_count(task, devicetype)
            _tries = 40
            while _conf_device_num != devicecount and _tries > 0:
                self.log.debug('Virtual %s count is %d expecting %d' % (devicetype, _conf_device_num, devicecount))
                time.sleep(5)
                _conf_device_num = self._get_virtual_media_device_count(task, devicetype)
                _tries = _tries -1

        except Exception as err:
            self.log.warning('Exception when setting virtual media device count, error: %s' % str(err))
            return False
        return True

    def _check_virtual_media_started(self, task):
        service_status = None
        # check virtmedia service status
        try:
            cmd = RAW_GET_VMEDIA_SERVICE_STATUS
            out, err = ipmitool.send_raw(task, cmd)
            service_status = out.strip()
            self.log.warning('Virtual media service status: %s' % service_status)
        except Exception as err:
            self.log.warning('Exception when checking virtual media service: %s' % str(err))

        return service_status == '01'

    def _start_virtual_media(self, task):
        # Enable "Remote Media Support"
        try:
            cmd = RAW_SET_VMEDIA_STATUS % '0x01'
            self.log.debug('Start virtual media service')
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when starting virtual media service: %s' % str(err))

    def _restart_virtual_media_service(self, task):
        try:
            cmd = RAW_RESTART_VMEDIA
            self.log.debug('Restart virtual media service')
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when restarting virtual media service: %s' % str(err))

    def _restart_ris(self, task):
        # Restart Remote Image Service
        try:
            self.log.debug('Restart Remote Image Service')
            cmd = RAW_RESTART_RIS
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when restarting RIS: %s' % str(err))
            return False
        return True

    def _restart_ris_cd(self, task):
        # Restart Remote Image Service CD media
        try:
            self.log.debug('Restart Remote Image Service CD media')
            cmd = RAW_RESTART_RIS_CD
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when restarting RIS CD media: %s' % str(err))
            return False
        return True

    def _enable_virtual_media(self, task):
        # Speed up things if it service is already running
        if self._check_virtual_media_started(task):
            self.log.debug('Virtual media service already running.')
            return True

        # Just enabling the service doe not seem to start it (in all HW)
        # Resetting it after enabling helps
        self._start_virtual_media(task)
        self._restart_virtual_media_service(task)

        tries = 60
        while tries > 0:
            if self._check_virtual_media_started(task):
                return True
            time.sleep(5)
            tries -= 1

        self.log.warning('Ensure virtual media service start failed: attempts exceeded.')
        return False

    def _set_nfs_server_ip(self, driver_info, task):
        try:
            cmd = RAW_SET_RIS_NFS_IP % (self.hex_convert(driver_info['provisioning_server']))
            self.log.debug('Virtual media server "%s"' % driver_info['provisioning_server'])
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when setting virtual media server: %s' % str(err))
            raise err

    def _set_share_type(self, task):
        try:
            cmd = RAW_SET_RIS_NFS
            self.log.debug('Virtual media share type to NFS.')
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when setting virtual media service type NFS: %s' % str(err))
            raise err

    def _set_nfs_root_path(self, driver_info, task):
        try:
            self.log.debug('Virtual media path to "%s"' % self.remote_share)
            # set progress bit (hmm. seems to return error if it is already set.. So should check..)
            # Welp there is no way checking this. As workaround clearing it first ( does not seem to
            # return error even if alreay cleared).
            # clear progress bit
            cmd = RAW_SET_RIS_PROGRESS % '0x00'
            ipmitool.send_raw(task, cmd)

            # set progress bit
            cmd = RAW_SET_RIS_PROGRESS % '0x01'
            ipmitool.send_raw(task, cmd)
            time.sleep(2)

            cmd = RAW_SET_RIS_NFS_PATH % (self.hex_convert(self.remote_share))
            ipmitool.send_raw(task, cmd)
            time.sleep(2)

            # clear progress bit
            md = RAW_SET_RIS_PROGRESS % '0x00'
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when setting virtual media path: %s' % str(err))
            return False

    def _set_setup_nfs(self, driver_info, task):
        try:
            # Set share type NFS
            self._set_share_type(task)
            # NFS server IP
            self._set_nfs_server_ip(driver_info, task)
            # Set NFS Mount Root path
            self._set_nfs_root_path(driver_info, task)
            return True

        except Exception:
            return False

    def _toggle_virtual_device(self, enabled, task):
        # Enable "Mount CD/DVD" in GUI should cause vmedia restart withing 2 seconds.
        # Seems "Mount CD/DVD" need to be enabled (or toggled) after config.
        # refresh/vmedia restart is not enough(?)
        try:
            status = '01' if enabled else '00'

            cmd = RAW_SET_VMEDIA_MOUNT_STATUS % ('0x' + status)
            self.log.debug('Set mount CD/DVD enable status %s' % status)
            ipmitool.send_raw(task, cmd)

            self.log.debug('Ensure CD/DVD enable status is %s' % status)
            tries = 60
            while tries > 0:
                out, err = ipmitool.send_raw(task, RAW_GET_VMEDIA_MOUNT_STATUS)
                res = out.strip()

                self.log.debug('CD/DVD enable status is %s' % res)
                if res == status:
                    return True

                tries -= 1
                time.sleep(2)

        except Exception as err:
            self.log.warning('Exception when CD/DVD virtual media new firmware? ignoring... Error: %s' % str(err))

        self.log.warning('Ensure virtual media status failed, attempts exceeded.')
        return False

    def _mount_virtual_device(self, task):
        return self._toggle_virtual_device(True, task)

    def _demount_virtual_device(self, task):
        return self._toggle_virtual_device(False, task)

    def _get_mounted_image_count(self, task):
        count = 0
        try:
            cmd = RAW_GET_MOUNTED_IMG_COUNT
            out, err = ipmitool.send_raw(task, cmd)
            out = out.strip()
            data = out[3:5]
            count = int(data, 16)
            self.log.debug('Available image count: %d' % count)
        except Exception as err:
            self.log.debug('Exception when trying to get the image count: %s' % str(err))
        return count

    def _set_image_name(self, image_filename, task):
        try:
            cmd = RAW_SET_IMG_NAME % (self.hex_convert(image_filename))
            self.log.debug('Setting virtual media image: %s' % image_filename)
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.debug('Exception when setting virtual media image: %s' % str(err))
            return False
        return True

    def _stop_remote_redirection(self, task):
        try:
            # Get num of enabled devices
            _num_inst = self._get_virtual_media_device_count(task, 'CD')
            for driveindex in range(0, _num_inst):
                cmd = RAW_STOP_REDIRECT % hex(driveindex)
                self.log.debug('Stop redirection CD/DVD drive index %d' % driveindex)
                out, err = ipmitool.send_raw(task, cmd)
                self.log.debug('ipmitool out = %s' % (out))
        except Exception as err:
            # Drive might not be mounted to start with
            self.log.debug('_stop_remote_redirection: Ignoring exception when stopping redirection CD/DVD drive index %d error: %s' % (driveindex, str(err)))
            pass

    def _clear_ris_configuration(self, task):
        # Clear Remote Image Service configuration
        try:
            cmd = RAW_CLEAR_RIS_CONFIG
            self.log.debug('Clear Remote Image Service configuration.')
            ipmitool.send_raw(task, cmd)
        except Exception as err:
            self.log.warning('Exception when clearing RIS NFS configuration: %s' % str(err))
            return False
        return True

    def _wait_for_mount_count(self, task):
        # Poll until we got some images from server
        tries = 60
        while tries > 0:
            if self._get_mounted_image_count(task) > 0:
                return True
            tries -= 1
            self.log.debug('Check available images count. Tries left: %d' % tries)
            time.sleep(10)

        self.log.warning('Available images count is 0. Attempts exceeded.')
        return False

    def attach_virtual_cd(self, image_filename, driver_info, task):

        # Enable virtual media
        if not self._enable_virtual_media(task):
            self.log.error("Failed to enable virtual media")
            return False

        # Enable CD/DVD device
        if not self._toggle_virtual_device(True, task):
            self.log.error("Failed to enable virtual device")
            return False

        # Clear Remote Image Service configuration
        if not self._clear_ris_configuration(task):
            self.log.error("Failed to clear Remote Image Service configuration")
            return False

        # Setup NFS
        if not self._set_setup_nfs(driver_info, task):
            self.log.error("Failed to setup nfs")
            return False

        # Restart Remote Image CD
        if not self._restart_ris_cd(task):
            self.log.error("Failed to restart Remote Image Service CD")
            return False

        #Wait for device to be mounted
        if not self._wait_for_mount_count(task):
            self.log.error("Failed when waiting for the device to appear")
            return False

        # Set Image Name
        if not self._set_image_name(image_filename, task):
            self.log.error("Failed to set image name")
            return False

        return self.check_and_wait_for_cd_mounting(image_filename, task, driver_info)

    def detach_virtual_cd(self, driver_info, task):
        """Detaches virtual cdrom on the node.

        :param task: an ironic task object
        """
        # Enable virtual media
        if not self._enable_virtual_media(task):
            self.log.error("detach_virtual_cd: Failed to enable virtual media")
            return False

        # Restart Remote Image Service
        if not self._restart_ris(task):
            self.log.error("Failed to restart Remote Image Service")
            return False

        # Stop redirection
        self._stop_remote_redirection(task)

        # Clear Remote Image Service configuration
        if not self._clear_ris_configuration(task):
            self.log.error("detach_virtual_cd: Failed to clear RIS configuration")
            return False

        # Unmount virtual device
        if not self._demount_virtual_device(task):
            self.log.error('detach_virtual_cd: Exception when disabling CD/DVD virtual media')
            return False

        # Reduce the number of virtual devices (both HD and CD default to 4 devices each)
        if not self._set_virtual_media_device_count(task, 'HD', 0):
            return False
        if not self._set_virtual_media_device_count(task, 'CD', 1):
            return False

        return True

    def set_boot_device(self, task):
        manager_utils.node_set_boot_device(task, boot_devices.FLOPPY, persistent=True)
