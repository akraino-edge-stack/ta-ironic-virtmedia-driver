# Copyright 2019 Nokia
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

from ironic.drivers.modules import ipmitool
from ironic.common.i18n import  _translators
from oslo_concurrency import processutils
from ironic.common import exception

from .ironic_virtmedia_hw import IronicVirtMediaHW

class OpenBMCIronicVirtMediaHW(IronicVirtMediaHW):
    def __init__(self, log):
        super(OpenBMCIronicVirtMediaHW, self).__init__(log)
        self.remote_share = '/remote_image_share_root/'

    def get_disk_attachment_status(self, task):
        """ Get the disk attachment status.
        :param task: a TaskManager instance.
        :returns: <str>: 'mounting' if operation is ongoing
                         'nfserror' if failed
                         'mounted' if the disk is successfully mounted
        """
        raise NotImplementedError

    @staticmethod
    def hex_convert(string_value, padding=False, length=0):
        if padding:
           string_value = string_value.ljust(length, '\0')
        return ' '.join('0x%s' % x.encode('hex') for x in string_value)

    def _issue_bmc_reset(self, driver_info, task):
        """ Issues a bmc reset and waits till the BMC is ready for servicing
        """
        cmd = 'bmc reset cold'
        node_uuid = task.node.uuid
        self.log.debug("Issuing bmc cold reset to node %s" %(task.node.name))
        try:
            out, err = ipmitool._exec_ipmitool(driver_info, cmd)
            self.log.debug('bmc reset returned stdout: %(stdout)s, stderr:'
                           ' %(stderr)s', {'stdout': out, 'stderr': err})
        except processutils.ProcessExecutionError as err:
            self.log.exception(_translators.log_error('IPMI "bmc reset" failed for node %(node_id)s '
                                                      'with error: %(error)s.'),
                               {'node_id': node_uuid, 'error': err})
            raise exception.IPMIFailure(cmd=cmd)

        sleep_count = 10
        cmd = 'bmc info'
        while sleep_count:
            try:
                out, err = ipmitool._exec_ipmitool(driver_info, cmd)
                self.log.debug('bmc reset returned stdout: %(stdout)s, stderr:'
                               ' %(stderr)s', {'stdout': out, 'stderr': err})
                break
            except processutils.ProcessExecutionError as err:
                self.log.debug(_translators.log_error('IPMI "bmc info" failed for node %(node_id)s '
                                                      'with error: %(error)s. Sleeping and retrying later.'
                                                      'sleep_count: %(sleep_count)s'),
                               {'node_id': node_uuid, 'error': err, 'sleep_count': sleep_count})
                time.sleep(10)
                sleep_count -= 1

        if not sleep_count:
            self.log.exception('After bmc reset, connection to bmc is lost!')
            raise exception.IPMIFailure(cmd='bmc reset')


    def _wait_for_cd_mounting(self, driver_info, task):
        sleep_count = 10
        while self.get_disk_attachment_status(task) == 'mounting' and sleep_count:
            self.log.debug("Waiting for the CD to be Mounted")
            sleep_count -= 1
            time.sleep(1)

        if sleep_count:
            return True

        self.log.warning("NFS mount timed out!. Trying BMC reset!")
        self._issue_bmc_reset(driver_info, task)

    def check_and_wait_for_cd_mounting(self, image_filename, task, driver_info):
        mount_status = self.get_disk_attachment_status(task)
        if mount_status == 'mounting':
            if self._wait_for_cd_mounting(driver_info, task):
                self.log.debug("Attached CD: %s" %(image_filename))
                return True
            else:
                return False
        elif mount_status == 'nfserror':
            self.log.exception("NFS mount failed!. Issue could be with NFS server status or connectivity to target.")
            raise exception.InstanceDeployFailure(reason='NFS mount failed!')
        elif mount_status == 'mounted':
            self.log.debug("Attached CD: %s" %(image_filename))
            return True
        else:
            self.log.exception("NFS mount failed!. Unknown error!")
            raise exception.InstanceDeployFailure(reason='NFS mount failed!')
