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

from ironic.drivers import ipmi
from ironic_virtmedia_driver import virtmedia_ssh_boot

class SSHVirtmediaHardware(ipmi.IPMIHardware):
    @property
    def supported_boot_interfaces(self):
        """List of supported boot interfaces."""
        return [virtmedia_ssh_boot.VirtualMediaAndSSHBoot]
