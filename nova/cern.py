# Copyright (c) 2014 CERN
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


import ldap
import time
import random
import socket
import string
import suds.client

from suds.xsd.doctor import ImportDoctor, Import
from suds.client import Client

import logging as pylog
from keystoneclient.v3 import client as keyclient
from nova import exception
from nova.i18n import _
from oslo_log import log as logging
from oslo.config import cfg


cern_network_opts = [
    cfg.StrOpt('landb_username',
        default='', secret=True,
        help='landb username'),
    cfg.StrOpt('landb_password',
        default='', secret=True,
        help='landb password'),
    cfg.StrOpt('landb_keystone',
        default='', secret=False,
        help='landb keystone auth'),
]

CONF = cfg.CONF
CONF.register_opts(cern_network_opts)


LOG = logging.getLogger(__name__)
pylog.getLogger('suds.client').setLevel(pylog.CRITICAL)


class Keystone:
    def __init__(self, context, endpoint=None):
        auth_token = context.auth_token
        if not endpoint:
            endpoint = CONF.landb_keystone
        self.client = keyclient.Client(token=auth_token, endpoint=endpoint)


    def _get_project(self, project_id):
        try:
            project =  self.client.projects.get(project_id)
        except Exception as e:
            LOG.error(_("Cannot get project: %s" % str(e)))
            raise exception.CernLanDB()
        return project

    def get_project_mainuser(self, project_id):
        project = self._get_project(project_id)
        mainuser = None
        if project and 'landb-mainuser' in project.__dict__.keys():
            mainuser = project.__dict__['landb-mainuser']
        return mainuser

    def get_project_responsible(self, project_id):
        project = self._get_project(project_id)
        responsible = None
        if project and 'landb-responsible' in project.__dict__.keys():
            responsible = project.__dict__['landb-responsible']
        return responsible


class LanDB:
    def __init__(self, username=None, password=None, client=None):
        if client != None:
            self.client = client
        else:
            self.client = self.__auth(username=None, password=None)


    def __auth(self, username=None, password=None):
        """Authenticates in landb"""
        url = 'https://network.cern.ch/sc/soap/soap.fcgi?v=5&WSDL'
        imp = Import('http://schemas.xmlsoap.org/soap/encoding/')
        d = ImportDoctor(imp)
        client = Client(url, doctor=d)

        if username == None or password == None:
            username = CONF.landb_username
            password = CONF.landb_password

        try:
            token = client.service.getAuthToken(username,password,'CERN')
            myheader = dict(Auth={'token':token})
            client.set_options(soapheaders=myheader)
        except Exception as e:
            LOG.error(_("Cannot authenticate in landb: %s" % str(e)))
            raise exception.CernLanDBAuthentication()

        return client


    def vm_update(self, device, new_device=None,
            location=None, manufacter=None, model=None, description=None, tag=None,
            operating_system=None, responsible_person=None, user_person=None):
        """Update vm metadata in landb"""
        metadata = None
        try:
            metadata = self.client.service.getDeviceBasicInfo(device.upper())
        except Exception as e:
            pass

        if new_device == None: new_device = device

        if location == None:
            location = {'Floor':'0', 'Room':'0', 'Building':'0'}

        if manufacter == None:
            manufacter = 'KVM'

        if model == None:
            model = 'VIRTUAL MACHINE'

        if description == None and metadata != None:
            description = metadata.Description if metadata.Description != None\
             else ''

        if tag == None:
            tag = 'OPENSTACK VM'

        if operating_system == None and metadata != None:
            operating_system = metadata.OperatingSystem

        if responsible_person == None and metadata != None:
            responsible_person = metadata.ResponsiblePerson

        if user_person == None and metadata != None:
            user_person = metadata.UserPerson

        try:
            self.client.service.vmUpdate(device,
                        {'DeviceName': new_device,
                        'Location': location,
                        'Manufacturer': manufacter,
                        'Model': model,
                        'Description': description,
                        'Tag': tag,
                        'OperatingSystem': operating_system,
                        'ResponsiblePerson': responsible_person,
                        'UserPerson': user_person})
        except Exception as e:
            LOG.error(_("Cannot update landb: %s" % str(e)))
            raise exception.CernLanDBUpdate(str(e))


    def vm_delete(self, device, new_device=None):
        """Update vm as deleted in landb"""
        if new_device == None:
            for i in range(5):
                new_device = 'Z' + ''.join(random.choice(string.ascii_uppercase\
                 + string.digits) for x in range(10))
                LOG.debug(_("Random instance name for Landb: %s" % new_device))
                if not self.device_exists:
                    LOG.debug(_("Hostname already exists: %s" % new_device))
                    continue
                else:
                    break

        os = {'Name': 'UNKNOWN',
              'Version': 'UNKNOWN'}

        responsible = {'FirstName':'E-GROUP',
                       'Name':'AI-OPENSTACK-ADMIN',
                       'Department':'IT'}

        user_person = {'FirstName':'E-GROUP',
                       'Name':'AI-OPENSTACK-ADMIN',
                       'Department':'IT'}

        try:
            self.client.service.vmNetReset(device)
            self.vm_update(device, new_device=new_device,
                description='Not in use', operating_system=os,
                responsible_person=responsible, user_person=user_person)
        except Exception as e:
            LOG.error(_("Cannot delete vm from landb: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def device_exists(self, device):
        """Check if a device is registered in landb"""
        try:
            self.client.service.getDeviceInfo(device)
        except:
            return False
        return device


    def device_hostname(self, address):
        """Get the hostname given an IP"""
        try:
            device = (self.client.service.searchDevice({'IPAddress':address}))[0]
        except Exception as e:
            LOG.error(_("Cannot find device with IP: %s" % str(e)))
            raise exception.CernDeviceNotFound('')
        return device


    def device_migrate(self, hostname, node):
        """Migrate VM to node"""
        try:
            self.client.service.vmMigrate(hostname, node)
        except Exception as e:
            LOG.error(_("Cannot migrate device in lanDB: %s" % str(e)))
            return False
        return True


    def alias_update(self, device, new_alias):
        """Update alias"""
        try:
            old_alias = self.getDeviceInfo(device).Interfaces[0].IPAliases
        except: 
            LOG.error(_("Cannot connect to LanDB"))
            raise exception.CernLanDB()

        if old_alias == None: old_alias = []

        old_alias_set = set([x.upper() for x in old_alias])
        new_alias_set = set([x.upper() for x in new_alias])

        add_alias = new_alias_set - old_alias_set
        remove_alias = old_alias_set - new_alias_set

        LOG.debug(_("Alias to add: %s" % add_alias))
        LOG.debug(_("Alias to remove: %s" % remove_alias))

        for alias in new_alias_set:
            if self.device_exists(alias):
                LOG.error(_("Alias already exists: %s" % str(alias)))
                msg = _("%s - The device already exists or is not "
                     "a valid hostname" % str(alias))
                raise exception.CernInvalidHostname(msg)

        try:
            for alias in remove_alias:
                self.__unset_alias(device, alias)

            for alias in add_alias:
                self.__set_alias(device, alias)
        except Exception as e:
            self.__recover_alias(device, old_alias)
            msg = _("%s - The device already exists or is not "
                     "a valid hostname" % str(alias))
            raise exception.CernInvalidHostname(msg)


    def ipv6ready_update(self, device, boolean):
        """Update ipv6 ready flag"""
        try:
            self.client.service.deviceUpdateIPv6Ready(device, boolean)
        except Exception as e:
            LOG.error(_("Cannot change IPv6-ready: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def internet_update(self, device, boolean):
        """Update VM internet connectivity"""
        clusters = self.vmGetClusterMembership(device)
        try:
            netcluster = [x for x in clusters if x.startswith('VMPOOL')][0]
        except Exception as e:
            LOG.error(_("Can't find netcluster: %s" % str(e)))
            raise exception.CernLanDBUpdate()

        parent = self.vmGetInfo(device).VMParent

        if boolean:
            internet = 1
        else:
            internet = 0

        try:
            self.client.service.vmMove(device, netcluster, parent, {'InternetConnectivity':internet})
        except Exception as e:
            LOG.error(_("Cannot update internet connectivity: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def __recover_alias(self, device, old_alias):
        """Try to recover alias after an error"""
        LOG.info(_("Trying to recover old alias"))
        try:
            current_alias = self.getDeviceInfo(device).Interfaces[0].IPAliases
        except:
            LOG.error(_("Cannot connect to LanDB"))
            raise exception.CernLanDB()

        for alias in current_alias:
            self.client.service.interfaceRemoveAlias(device, alias)

        try:
            for alias in old_alias:
                self.client.service.interfaceAddAlias(device, alias)
        except Exception as e:
            LOG.error(_("Cannot recover all alias: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def __set_alias(self, device, alias):
        """Set alias to a device"""
        try:
            self.client.service.interfaceAddAlias(device, alias)
        except Exception as e:
            LOG.error(_("Cannot set alias in landb: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def __unset_alias(self, device, alias):
        """Unset all alias in a device"""
        try:
            self.client.service.interfaceRemoveAlias(device, alias)
        except Exception as e:
            LOG.error(_("Cannot unset alias in landb: %s" % str(e)))
            raise exception.CernLanDBUpdate()


    def vmClusterGetDevices(self, cluster):
        """Get all cluster devices"""
        try:
            return self.client.service.vmClusterGetDevices(cluster)
        except Exception as e:
            LOG.error(_("Cannot get VMs from network cluster - %s - %s"),
                        cluster, str(e))
            raise exception.CernLanDB()


    def vmClusterGetInfo(self, cluster):
        """Get cluster info"""
        try:
            return self.client.service.vmClusterGetInfo(cluster)
        except Exception as e:
            LOG.error(_("Cannot get network services for network cluster - "
                        "%s - %s"), cluster, str(e))
            raise exception.CernLanDB()


    def vmGetClusterMembership(self, device):
        """Get device cluster"""
        try:
            return self.client.service.vmGetClusterMembership(device)
        except Exception as e:
            LOG.error(_("Cannot get VM cluster: %s" % str(e)))
            raise exception.CernLanDB()


    def vmGetInfo(self, device):
        """Get vm device cluster"""
        try:
            return self.client.service.vmGetInfo(device)
        except Exception as e:
            LOG.error(_("Cannot get VM info: %s" % str(e)))
            raise exception.CernLanDB()


    def getServiceInfo(self, service):
        """Get service information"""
        try:
            return self.client.service.getServiceInfo(service)
        except Exception as e:
            LOG.error(_("Cannot get service information"))
            raise exception.CernLanDB()


    def getDevicesFromService(self, service):
        """Get devices from service"""
        try:
            return self.client.service.getDevicesFromService(service)
        except Exception as e:
            LOG.error(_("Cannot devices from service: %s - %s"), service, str(e))
            raise exception.CernLanDB()


    def getDeviceInfo(self, device):
        """Get device information"""
        try:
            return self.client.service.getDeviceInfo(device)
        except Exception as e:
            LOG.error(_("Cannot get VM network info - %s" % str(e)))
            raise exception.CernLanDB()


class Xldap:
    def __init__(self, url='xldap.cern.ch', protocol_version=ldap.VERSION3,
                 searchScope=ldap.SCOPE_SUBTREE, retrieveAttributes=None):
        self.client = ldap.open(url)
        self.client.protocol_version = protocol_version
        self.searchScope = searchScope
        self.retrieveAttributes = retrieveAttributes

    def user_exists(self, user, baseDN='OU=Users,OU=Organic Units,DC=cern,DC=ch'):
        """Check if an user exists at CERN"""
        try:
            searchFilter = "cn="+user
            ldap_result_id = self.client.search(baseDN, self.searchScope,
                        searchFilter, self.retrieveAttributes)
            result_type, result_data = self.client.result(ldap_result_id, 0)
            if (result_data == []):
                return False
            if result_type == ldap.RES_SEARCH_ENTRY:
                return int(result_data[0][1]['employeeID'][0])
        except Exception as e:
            LOG.error(_("Cannot verify if USER exists. %s" % str(e)))
            raise exception.CernInvalidUser()

    def egroup_exists(self, egroup, baseDN='OU=Workgroups,DC=cern,DC=ch'):
        """Check if an egroup exists at CERN"""
        try:
            searchFilter = "cn="+egroup
            ldap_result_id = self.client.search(baseDN, self.searchScope,
                        searchFilter, self.retrieveAttributes)
            result_type, result_data = self.client.result(ldap_result_id, 0)
            if (result_data == []):
                return False
            if result_type == ldap.RES_SEARCH_ENTRY:
                return str(egroup)
        except Exception as e:
            LOG.error(_("Cannot verify if EGROUP exists. %s" % str(e)))
            raise exception.CernInvalidEgroup()

    def device_exists(self, device):
        """Check if device exists in Xldap"""
        try:
            searchFilter = "(&(name="+device+"))"
            ldap_result_id = self.client.search(baseDN, self.searchScope,
                        searchFilter, self.retrieveAttributes)
            result_type, result_data = self.client.result(ldap_result_id, 0)
            if (result_data == []):
                return False
            if result_type == ldap.RES_SEARCH_ENTRY:
                return device
        except Exception as e:
            LOG.error(_("Cannot verify if device exists. %s" % str(e)))
            raise exception.CernInvalidDevice()

class ActiveDirectory():
    def __init__(self):
        url='http://compmgtsvc.web.cern.ch/compmgtsvc/compmgtsvc.asmx?wsdl'
        self.client = Client(url, cache=None)

    def register(self, hostname):
        try:
            result = self.client.service.CheckComputer(hostname)
            if result != None:
                LOG.warn(_("AD update failed - %s - %s" % (hostname, str(result))))
        except Exception as e:
            raise exception.CernActiveDirectory()

    def delete(self, hostname):
        try:
            result = self.client.service.DeleteComputer(hostname)
            if result != None:
                LOG.warn(_("AD delete failed - %s - %s" % (hostname, str(result))))
        except Exception as e:
            LOG.warn(_("Cannot delete VM from AD. %s" % str(e)))

class Dns():
    def gethostbyname(self, hostname):
        try:
            socket.gethostbyname(hostname)
            return hostname
        except:
            return False
# CERN
