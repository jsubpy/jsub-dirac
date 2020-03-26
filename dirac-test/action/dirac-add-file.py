#!/usr/bin/env python

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


dirac_output_dir = 'a/b/c'
upload_file = 'xxx.slcio'
se = 'IHEP-STORM'


def getUserHome():
    res = getProxyInfo(False, False)
    if not res['OK']:
        return ''
    owner = res['Value']['username']
    ownerGroup = res['Value']['group']
    vo = Registry.getVOMSVOForGroup(ownerGroup)
    voHome = '/{0}'.format(vo)
    userHome = '/{0}/user/{1:.1}/{1}'.format(vo, owner)
    return userHome


def main():
    userHome = getUserHome()
    if not userHome:
        gLogger.error('Failed to get user home')
        return 1

    lfn = '{0}/{1}/{2}'.format(getUserHome(), dirac_output_dir, upload_file)

    fcc = FileCatalogClient()
    dm = DataManager(['FileCatalog'])

    res = dm.putAndRegister(lfn, upload_file, se)
    if not res['OK']:
        gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (
            lfn, se, res['Message']))
        return 1
    elif res['Value']['Failed'].has_key(lfn):
        gLogger.error('Failed to putAndRegister %s to %s' % (lfn, se))
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
