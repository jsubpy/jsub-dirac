#!/usr/bin/env python

import itertools
import sys
import os

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

from DIRAC.Interfaces.API.Dirac import Dirac


def input_path_to_lfn(input_path):

    res = getProxyInfo( False, False )
    if not res['OK']:
        gLogger.error( "Failed to get client proxy information.", res['Message'] )
        DIRAC.exit( 2 ) 
    proxyInfo = res['Value']
    if proxyInfo['secondsLeft'] == 0:
        gLogger.error( "Proxy expired" )
        DIRAC.exit( 2 )
    username = proxyInfo['username']
    vo = ''
    if 'group' in proxyInfo:
        vo = getVOForGroup( proxyInfo['group'] )

    folder_name=os.path.basename(os.path.dirname(input_path))
    lfn = '/%s/user/%s/%s/jsub/'%(vo,username[0],username) + folder_name + '/' + os.path.basename(input_path)
    gLogger.debug('input path = %s'% input_path)
    gLogger.debug('lfn translation = %s'% lfn)

    return(lfn)


def main():
    input_file_jobvar_name='JSUB_'+os.environ.get('JSUB_input_file_jobvar_name','input_file') # if input file is local, and put to DFC through jsub register.
    input_lfn_jobvar_name='JSUB_'+os.environ.get('JSUB_input_lfn_jobvar_name','input_lfn') # if input file is directly from DFC
    input_path=os.environ.get(input_file_jobvar_name)
    input_lfn=os.environ.get(input_lfn_jobvar_name)
    destination=os.environ.get('JSUB_destination','../')

	if input_lfn is None:
    	lfn=input_path_to_lfn(input_path)
	else:
		lfn=input_lfn
    fname=os.path.basename(lfn)


#    gfal_prefix = 'srm://storm.ihep.ac.cn:8444'
#    gfal_path = gfal_prefix + input_path
#    dest_dir = os.environ.get('destination_dir','../')
#    dest_filename = dest_dir + os.path.basename(input_path)
#    os.system('gfal-copy {} {}'.format(gfal_path,dest_filename))  #replace the input file

    # download the file to the action folder
    dirac = Dirac()
    result=dirac.getFile(lfn)
    
    if not result['OK']:
        gLogger.error('Download file error: %s' % result['Message'])
        return 2

    # cp to destination
    os.system('cp %s %s'%(fname, destination))

    return 0


if __name__ == '__main__':
    exit(main())



