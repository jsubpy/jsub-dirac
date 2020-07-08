#!/usr/bin/env python

'''
This action module uploads files on WN to DFC.
'''

import os
import glob

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


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
	source_location=os.environ.get('JSUB_source_location','./')
	destination_dir=os.environ.get('JSUB_destination_dir','')
	destination_in_user_home=os.environ.get('JSUB_user_home','True')	#if true: relative path to cefs user home for upload destination; if false: absolute path
	files_to_upload=os.environ.get('JSUB_files_to_upload','*')
	overwrite=(os.environ.get('JSUB_overwrite','False').upper() == 'TRUE')
	upload_file_jobvar=os.environ.get('JSUB_upload_file_jobvar')
	relocate_to_cwd=os.environ.get('JSUB_relocate_to_cwd')		#if true, value of jobvar doesn't reflect dir name; file is under cwd instead
	se = os.environ.get('JSUB_SE','IHEP-STORM')
	upload_status=0


	# if upload_file_jobvar exists, need to reshape output setting to a standard one
	if upload_file_jobvar!=None:
		if relocate_to_cwd:		#value of jobvar doesn't reflect dir name; file is under cwd instead
			upfile=os.environ.get('JSUB_'+upload_file_jobvar)
			source_location='./'
			files_to_upload=os.path.basename(upfile)
			destination_dir=os.path.dirname(upfile)
		else:
			files_to_upload=os.environ.get('JSUB_'+upload_file_jobvar)
#			destination_dir=os.path.dirname(upfile)
			

	# files_to_upload should be wildcards splitted by ','
	flist_raw=files_to_upload.split(',')
	flist=[]
	for f in flist_raw:
		l=glob.glob(os.path.join(source_location,f))
		l=[os.path.relpath(x,source_location) for x in l]
		flist+=l

	userHome = getUserHome()
	


	if not userHome:
		gLogger.error('Failed to get user home')
		return 1
	for f in flist:
		if destination_in_user_home:
			lfn = os.path.join(getUserHome(), destination_dir, f)
		else:
			lfn = os.path.join(destination_dir,f)
	
		fcc = FileCatalogClient()
		dm = DataManager(['FileCatalog'])
	
		print('registering file: LFN={0}, source={1}, SE={2}'.format(lfn,os.path.join(source_location,f),se))
		res = dm.putAndRegister(lfn, os.path.join(source_location,f), se, overwrite=overwrite)
		if not res['OK']:
			gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (
				lfn, se, res['Message']))
			upload_status=1
		elif res['Value']['Failed'].has_key(lfn):
			gLogger.error('Failed to putAndRegister %s to %s' % (lfn, se))
			upload_status=1

	return upload_status


if __name__ == '__main__':
	exit(main())
