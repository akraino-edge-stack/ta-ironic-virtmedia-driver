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

Name:       ironic-virtmedia-driver
Version:    %{_version}
Release:    2%{?dist}
Summary:    Contains ironic drivers for virtualmedia based deployment
License:    %{_platform_licence}
Source0:    %{name}-%{version}.tar.gz
Vendor:     %{_platform_vendor}
BuildArch:  noarch

Requires: python-cliff python-pip openstack-ironic-common
BuildRequires: python
BuildRequires: python-setuptools


%description
This RPM contains ironic drivers for virtualmedia based deployment

%prep
%autosetup

%build

%install
cd src && python setup.py install --root %{buildroot} --no-compile --install-purelib %{_python_site_packages_path} && cd -


%files
%{_python_site_packages_path}/ironic_virtmedia_driver*

%pre

%post


%preun

%postun

%clean
rm -rf %{buildroot}

# TIPS:
# File /usr/lib/rpm/macros contains useful variables which can be used for example to define target directory for man page.
