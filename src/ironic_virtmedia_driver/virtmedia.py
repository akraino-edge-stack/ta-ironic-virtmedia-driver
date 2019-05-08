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

import os
import shutil
import tempfile
import tarfile

from ironic_lib import metrics_utils
from ironic_lib import utils as ironic_utils
from oslo_log import log as logging
from oslo_utils import importutils

from ironic.common import boot_devices
from ironic.common import exception
from ironic.common.glance_service import service_utils
from ironic.common.i18n import _, _translators
from ironic.common import images
from ironic.common import states
from ironic.common import utils
from ironic.conductor import utils as manager_utils
from ironic_virtmedia_driver.conf import CONF
from ironic.drivers import base
from ironic.drivers.modules import deploy_utils
from ironic_virtmedia_driver import virtmedia_exception

LOG = logging.getLogger(__name__)

METRICS = metrics_utils.get_metrics_logger(__name__)

REQUIRED_PROPERTIES = {
    'virtmedia_deploy_iso': _("Deployment ISO image file name. "
                         "Required."),
}

COMMON_PROPERTIES = REQUIRED_PROPERTIES


def _parse_config_option():
    """Parse config file options.

    This method checks config file options validity.

    :raises: InvalidParameterValue, if config option has invalid value.
    """
    error_msgs = []
    if not os.path.isdir(CONF.remote_image_share_root):
        error_msgs.append(
            _("Value '%s' for remote_image_share_root isn't a directory "
              "or doesn't exist.") %
            CONF.remote_image_share_root)
    if error_msgs:
        msg = (_("The following errors were encountered while parsing "
                 "config file:%s") % error_msgs)
        raise exception.InvalidParameterValue(msg)


def _parse_driver_info(node):
    """Gets the driver specific Node deployment info.

    This method validates whether the 'driver_info' property of the
    supplied node contains the required or optional information properly
    for this driver to deploy images to the node.

    :param node: a target node of the deployment
    :returns: the driver_info values of the node.
    :raises: MissingParameterValue, if any of the required parameters are
        missing.
    :raises: InvalidParameterValue, if any of the parameters have invalid
        value.
    """
    d_info = node.driver_info
    deploy_info = {}

    deploy_info['virtmedia_deploy_iso'] = d_info.get('virtmedia_deploy_iso')
    error_msg = _("Error validating virtual media deploy. Some parameters"
                  " were missing in node's driver_info")
    deploy_utils.check_for_missing_params(deploy_info, error_msg)

    if service_utils.is_image_href_ordinary_file_name(
            deploy_info['virtmedia_deploy_iso']):
        deploy_iso = os.path.join(CONF.remote_image_share_root,
                                  deploy_info['virtmedia_deploy_iso'])
        if not os.path.isfile(deploy_iso):
            msg = (_("Deploy ISO file, %(deploy_iso)s, "
                     "not found for node: %(node)s.") %
                   {'deploy_iso': deploy_iso, 'node': node.uuid})
            raise exception.InvalidParameterValue(msg)

    return deploy_info

def _parse_deploy_info(node):
    """Gets the instance and driver specific Node deployment info.

    This method validates whether the 'instance_info' and 'driver_info'
    property of the supplied node contains the required information for
    this driver to deploy images to the node.

    :param node: a target node of the deployment
    :returns: a dict with the instance_info and driver_info values.
    :raises: MissingParameterValue, if any of the required parameters are
        missing.
    :raises: InvalidParameterValue, if any of the parameters have invalid
        value.
    """
    deploy_info = {}
    deploy_info.update(deploy_utils.get_image_instance_info(node))
    deploy_info.update(_parse_driver_info(node))

    return deploy_info

def _get_deploy_iso_name(node):
    """Returns the deploy ISO file name for a given node.

    :param node: the node for which ISO file name is to be provided.
    """
    return "deploy-%s.iso" % node.name

def _get_boot_iso_name(node):
    """Returns the boot ISO file name for a given node.

    :param node: the node for which ISO file name is to be provided.
    """
    return "boot-%s.iso" % node.uuid

def _get_floppy_image_name(node):
    """Returns the floppy image name for a given node.

    :param node: the node for which image name is to be provided.
    """
    return "image-%s.img" % node.name


def _prepare_floppy_image(task, params):
    """Prepares the floppy image for passing the parameters.

    This method prepares a temporary vfat filesystem image, which
    contains the parameters to be passed to the ramdisk.
    Then it uploads the file NFS or CIFS server.

    :param task: a TaskManager instance containing the node to act on.
    :param params: a dictionary containing 'parameter name'->'value' mapping
        to be passed to the deploy ramdisk via the floppy image.
    :returns: floppy image filename
    :raises: ImageCreationFailed, if it failed while creating the floppy image.
    :raises: VirtmediaOperationError, if copying floppy image file failed.
    """
    floppy_filename = _get_floppy_image_name(task.node)
    floppy_fullpathname = os.path.join(
        CONF.remote_image_share_root, floppy_filename)

    with tempfile.NamedTemporaryFile() as vfat_image_tmpfile_obj:
        images.create_vfat_image(vfat_image_tmpfile_obj.name,
                                 parameters=params)
        try:
            shutil.copyfile(vfat_image_tmpfile_obj.name,
                            floppy_fullpathname)
        except IOError as e:
            operation = _("Copying floppy image file")
            raise virtmedia_exception.VirtmediaOperationError(
                operation=operation, error=e)

    return floppy_filename

def _append_floppy_to_cd(bootable_iso_filename, floppy_image_filename):
    """ Quanta HW cannot attach 2 Virtual media at the moment.
        Preparing CD which has floppy content at the end of it as
        64K block tar file.
    """
    boot_iso_full_path = CONF.remote_image_share_root + bootable_iso_filename
    floppy_image_full_path = CONF.remote_image_share_root + floppy_image_filename
    tar_file_path = CONF.remote_image_share_root + floppy_image_filename + '.tar.gz' 

    # Prepare a temporary Tar file
    tar = tarfile.open(tar_file_path, "w:gz")
    tar.add(floppy_image_full_path, arcname=os.path.basename(floppy_image_full_path))
    tar.close()

    # Using dd append Tar to iso and remove Tar file
    ironic_utils.dd(tar_file_path, boot_iso_full_path, 'bs=64k', 'conv=notrunc,sync', 'oflag=append')

    os.remove(tar_file_path)

def _remove_share_file(share_filename):
    """Remove given file from the share file system.

    :param share_filename: a file name to be removed.
    """
    share_fullpathname = os.path.join(
        CONF.remote_image_share_root, share_filename)
    LOG.debug(_translators.log_info("_remove_share_file: Unlinking %s"), share_fullpathname)
    ironic_utils.unlink_without_raise(share_fullpathname)

class VirtmediaBoot(base.BootInterface):
    """Implementation of a boot interface using Virtual Media."""

    def __init__(self):
        """Constructor of VirtualMediaBoot.

        :raises: InvalidParameterValue, if config option has invalid value.
        """
        super(VirtmediaBoot, self).__init__()

    def get_properties(self):
        return COMMON_PROPERTIES

    @METRICS.timer('VirtualMediaBoot.validate')
    def validate(self, task):
        """Validate the deployment information for the task's node.

        :param task: a TaskManager instance containing the node to act on.
        :raises: InvalidParameterValue, if config option has invalid value.
        :raises: InvalidParameterValue, if some information is invalid.
        :raises: MissingParameterValue if 'kernel_id' and 'ramdisk_id' are
            missing in the Glance image, or if 'kernel' and 'ramdisk' are
            missing in the Non Glance image.
        """
        d_info = _parse_deploy_info(task.node)
        if task.node.driver_internal_info.get('is_whole_disk_image'):
            props = []
        elif service_utils.is_glance_image(d_info['image_source']):
            props = ['kernel_id', 'ramdisk_id']
        else:
            props = ['kernel', 'ramdisk']
        deploy_utils.validate_image_properties(task.context, d_info,
                                               props)

    @METRICS.timer('VirtualMediaBoot.prepare_ramdisk')
    def prepare_ramdisk(self, task, ramdisk_params):
        """Prepares the deploy ramdisk using virtual media.

        Prepares the options for the deployment ramdisk, sets the node to boot
        from virtual media cdrom.

        :param task: a TaskManager instance containing the node to act on.
        :param ramdisk_params: the options to be passed to the deploy ramdisk.
        :raises: ImageRefValidationFailed if no image service can handle
                 specified href.
        :raises: ImageCreationFailed, if it failed while creating the floppy
                 image.
        :raises: InvalidParameterValue if the validation of the
                 PowerInterface or ManagementInterface fails.
        :raises: VirtmediaOperationError, if some operation fails.
        """

        # NOTE(TheJulia): If this method is being called by something
        # aside from deployment and clean, such as conductor takeover, we
        # should treat this as a no-op and move on otherwise we would modify
        # the state of the node due to virtual media operations.

        if (task.node.provision_state != states.DEPLOYING and
                task.node.provision_state != states.CLEANING):
            return

        deploy_nic_mac = deploy_utils.get_single_nic_with_vif_port_id(task)
        ramdisk_params['BOOTIF'] = deploy_nic_mac
        os_net_config = task.node.driver_info.get('os_net_config')
        if os_net_config:
            ramdisk_params['os_net_config'] = os_net_config

        self._setup_deploy_iso(task, ramdisk_params)

    @METRICS.timer('VirtualMediaBoot.clean_up_ramdisk')
    def clean_up_ramdisk(self, task):
        """Cleans up the boot of ironic ramdisk.

        This method cleans up the environment that was setup for booting the
        deploy ramdisk.

        :param task: a task from TaskManager.
        :returns: None
        :raises: VirtmediaOperationError if operation failed.
        """
        self._cleanup_vmedia_boot(task)

    @METRICS.timer('VirtualMediaBoot.prepare_instance')
    def prepare_instance(self, task):
        """Prepares the boot of instance.

        This method prepares the boot of the instance after reading
        relevant information from the node's database.

        :param task: a task from TaskManager.
        :returns: None
        """
        self._cleanup_vmedia_boot(task)

        node = task.node
        iwdi = node.driver_internal_info.get('is_whole_disk_image')
        if deploy_utils.get_boot_option(node) == "local" or iwdi:
            manager_utils.node_set_boot_device(task, boot_devices.DISK,
                                               persistent=True)
        else:
            driver_internal_info = node.driver_internal_info
            root_uuid_or_disk_id = driver_internal_info['root_uuid_or_disk_id']
            self._configure_vmedia_boot(task, root_uuid_or_disk_id)

    @METRICS.timer('VirtualMediaBoot.clean_up_instance')
    def clean_up_instance(self, task):
        """Cleans up the boot of instance.

        This method cleans up the environment that was setup for booting
        the instance.

        :param task: a task from TaskManager.
        :returns: None
        :raises: VirtmediaOperationError if operation failed.
        """
        _remove_share_file(_get_boot_iso_name(task.node))
        driver_internal_info = task.node.driver_internal_info
        driver_internal_info.pop('root_uuid_or_disk_id', None)
        task.node.driver_internal_info = driver_internal_info
        task.node.save()
        self._cleanup_vmedia_boot(task)

    def _configure_vmedia_boot(self, task, root_uuid_or_disk_id):
        """Configure vmedia boot for the node."""
        return

    def _set_deploy_boot_device(self, task):
        """Set the boot device for deployment"""
        manager_utils.node_set_boot_device(task, boot_devices.CDROM)

    def _setup_deploy_iso(self, task, ramdisk_options):
        """Attaches virtual media and sets it as boot device.

        This method attaches the given deploy ISO as virtual media, prepares the
        arguments for ramdisk in virtual media floppy.

        :param task: a TaskManager instance containing the node to act on.
        :param ramdisk_options: the options to be passed to the ramdisk in virtual
            media floppy.
        :raises: ImageRefValidationFailed if no image service can handle specified
           href.
        :raises: ImageCreationFailed, if it failed while creating the floppy image.
        :raises: VirtmediaOperationError, if some operation on failed.
        :raises: InvalidParameterValue if the validation of the
            PowerInterface or ManagementInterface fails.
        """
        d_info = task.node.driver_info

        deploy_iso_href = d_info['virtmedia_deploy_iso']
        if service_utils.is_image_href_ordinary_file_name(deploy_iso_href):
            deploy_iso_file = deploy_iso_href
        else:
            deploy_iso_file = _get_deploy_iso_name(task.node)
            deploy_iso_fullpathname = os.path.join(
                CONF.remote_image_share_root, deploy_iso_file)
            images.fetch(task.context, deploy_iso_href, deploy_iso_fullpathname)

        self._setup_vmedia_for_boot(task, deploy_iso_file, ramdisk_options)
        self._set_deploy_boot_device(task)

    def _setup_vmedia_for_boot(self, task, bootable_iso_filename, parameters=None):
        """Sets up the node to boot from the boot ISO image.

        This method attaches a boot_iso on the node and passes
        the required parameters to it via a virtual floppy image.

        :param task: a TaskManager instance containing the node to act on.
        :param bootable_iso_filename: a bootable ISO image to attach to.
            The iso file should be present in NFS/CIFS server.
        :param parameters: the parameters to pass in a virtual floppy image
            in a dictionary.  This is optional.
        :raises: ImageCreationFailed, if it failed while creating a floppy image.
        :raises: VirtmediaOperationError, if attaching a virtual media failed.
        """
        LOG.info(_translators.log_info("Setting up node %s to boot from virtual media"),
                 task.node.uuid)

        self._detach_virtual_cd(task)
        self._detach_virtual_fd(task)

        floppy_image_filename = None
        if parameters:
            floppy_image_filename = _prepare_floppy_image(task, parameters)
            self._attach_virtual_fd(task, floppy_image_filename)

        if floppy_image_filename:
            _append_floppy_to_cd(bootable_iso_filename, floppy_image_filename)

        self._attach_virtual_cd(task, bootable_iso_filename)

    def _cleanup_vmedia_boot(self, task):
        """Cleans a node after a virtual media boot.

        This method cleans up a node after a virtual media boot.
        It deletes floppy and cdrom images if they exist in NFS/CIFS server.
        It also ejects both the virtual media cdrom and the virtual media floppy.

        :param task: a TaskManager instance containing the node to act on.
        :raises: VirtmediaOperationError if ejecting virtual media failed.
        """
        LOG.debug("Cleaning up node %s after virtual media boot", task.node.uuid)

        node = task.node
        self._detach_virtual_cd(task)
        self._detach_virtual_fd(task)

        _remove_share_file(_get_floppy_image_name(node))
        _remove_share_file(_get_deploy_iso_name(node))

    def _attach_virtual_cd(self, task, bootable_iso_filename):
        """Attaches the given url as virtual media on the node.

        :param node: an ironic node object.
        :param bootable_iso_filename: a bootable ISO image to attach to.
            The iso file should be present in NFS/CIFS server.
        :raises: VirtmediaOperationError if attaching virtual media failed.
        """
        return

    def _detach_virtual_cd(self, task):
        """Detaches virtual cdrom on the node.

        :param node: an ironic node object.
        :raises: VirtmediaOperationError if eject virtual cdrom failed.
        """
        return

    def _attach_virtual_fd(self, task, floppy_image_filename):
        """Attaches virtual floppy on the node.

        :param node: an ironic node object.
        :raises: VirtmediaOperationError if insert virtual floppy failed.
        """
        return

    def _detach_virtual_fd(self, task):
        """Detaches virtual media floppy on the node.

        :param node: an ironic node object.
        :raises: VirtmediaOperationError if eject virtual media floppy failed.
        """
        return
